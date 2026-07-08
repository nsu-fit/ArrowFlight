# Поток выполнения SQL-запроса

Этот документ описывает путь SQL-запроса через Arrow Flight сервер: от `GetFlightInfo` до чтения Parquet-файлов, выбора движка выполнения и отправки Arrow batch-ей клиенту.

## Основные компоненты

`HadoopArrowFlightServer` запускает сервер и настраивает Hadoop FileSystem, Hazelcast и Arrow Flight.

`HadoopFlightSqlService` реализует слой Flight SQL. Он принимает запросы, строит `FlightInfo`, создает `Ticket`, распределяет файлы по Flight-нодам и восстанавливает состояние запроса во время `DoGet`.

`ParquetManager` отвечает за Parquet-часть: поиск файлов, локальность блоков, Java fast path по footer metadata, сканирование через Arrow Dataset / Acero и выполнение SQL через DuckDB.

`ParquetQueryParser` разбирает SQL и извлекает схему, таблицу, список колонок, фильтр, агрегаты и `GROUP BY`.

`RuntimeSettings` читает настройки из `arrowflight.properties`. JVM system properties и часть environment variables могут переопределять значения из файла.

## Общий поток

Запрос проходит две фазы.

1. Планирование: клиент вызывает `GetFlightInfo`; сервер определяет схему результата, распределяет файлы между Flight-нодами и возвращает endpoints с tickets.
2. Чтение: клиент вызывает `DoGet` по tickets; соответствующие Flight-ноды читают назначенные файлы и стримят результат в Arrow-формате.

Данные не читаются во время `GetFlightInfo`. Реальное чтение начинается только во время `DoGet`.

## Получение SQL-запроса

SQL приходит в `HadoopFlightSqlService.getFlightInfoStatement`.

На этом этапе сервер:

1. Создает уникальный handle для запроса.
2. Строит Arrow-схему результата.
3. Сохраняет SQL в Hazelcast `statementCache`.
4. Начинает построение endpoints.

Handle связывает будущий ticket с состоянием запроса.

## Hazelcast cache и ticket

`statementCache` - распределенная Hazelcast `IMap`, доступная всем Flight-нодам в кластере.

Ticket не содержит сами файлы и не является полным execution plan. Ticket содержит handle. По этому handle сервер находит SQL-запрос и список файлов, назначенных конкретному endpoint.

Это нужно потому, что `GetFlightInfo` и `DoGet` - разные сетевые вызовы. Состояние запроса должно жить между ними.

В cluster mode ticket может быть создан одной нодой, а чтение может выполняться другой. Общий Hazelcast cache позволяет любой ноде восстановить состояние по handle.

## Построение схемы результата

Перед возвратом `FlightInfo` сервер должен знать схему результата. Для этого `HadoopFlightSqlService` вызывает `ParquetManager.getQuerySchema`.

Для обычных `SELECT` схема строится из Parquet schema таблицы и выбранных колонок. Для агрегатов схема строится из aggregate expressions и `GROUP BY`.

## Распределение файлов

Распределение выполняется в `HadoopFlightSqlService.determineEndpoints`.

Сервер получает SQL из `statementCache`, затем `ParquetManager.locationsForQuery` определяет Parquet-файлы таблицы и Hadoop hosts, на которых лежат блоки этих файлов.

Дальше сервер читает список зарегистрированных Flight-нод из `serverRegistry`. Для каждого файла вызывается `pickServer`, который выбирает ноду для чтения.

Если Flight-нода находится на host, где есть блоки файла, такая нода предпочитается. Среди подходящих нод выбирается нода с меньшей накопленной нагрузкой по размеру назначенных файлов.

Для каждой группы файлов создается отдельный endpoint.

## Чтение по ticket

Когда клиент вызывает `DoGet`, сервер попадает в `HadoopFlightSqlService.getStreamStatement`.

Сервер извлекает handle из ticket, загружает SQL и список назначенных файлов из `statementCache`, затем вызывает `ParquetManager.readParquet`.

Именно здесь начинается выполнение запроса.

## Выбор query engine

В проекте есть три пути чтения и выполнения Parquet-запросов. Выбор делается в `ParquetManager.readParquet` после разбора SQL.

Правила маршрутизации:

1. Metadata-only aggregates -> Java footer path.
2. Full scan и projection-only scan без `WHERE` -> Arrow Dataset / Acero.
3. `WHERE`, `GROUP BY`, `SUM`, fallback aggregates и `JOIN` -> DuckDB.

Это соответствует ADR 0001: Acero используется там, где он быстрее всего читает Parquet в Arrow; DuckDB используется там, где нужны SQL semantics и predicate pushdown; Java используется там, где результат можно получить из Parquet footer без чтения data pages.

## Java footer path

Java footer path используется для простых агрегатов без `WHERE` и без `GROUP BY`.

Поддерживаемые fast path выражения:

- `COUNT(*)`
- `COUNT(col)`, если есть полная статистика null-count
- `MIN(col)`, если есть min statistics
- `MAX(col)`, если есть max statistics

Для `COUNT(*)` Java суммирует row counts из metadata row group-ов. Для `COUNT(col)` Java вычитает null counts. Для `MIN` и `MAX` Java объединяет статистики по row group-ам.

Этот путь не читает data pages и не запускает Acero или DuckDB для самого запроса. Если нужной статистики нет, запрос безопасно fallback-ится в DuckDB.

## Acero path

Acero используется для чтения без фильтра:

- `SELECT * FROM schema.table`
- `SELECT col1, col2 FROM schema.table`

`ParquetManager` превращает назначенные относительные пути в абсолютные URI и создает Arrow Dataset scanner на файлы конкретного ticket. Projection передается в scanner как список колонок. Если projection нет, читаются все колонки таблицы.

Arrow Dataset / Acero открывает Parquet-файлы, читает нужные колонки и возвращает Arrow batches. Java не конвертирует значения вручную: результат приходит как `ArrowReader` и `VectorSchemaRoot`.

Каждый batch отправляется клиенту через Flight listener. Перед отправкой сервер проверяет backpressure, чтобы не буферизовать лишние данные, когда клиент не готов.

## DuckDB path

DuckDB используется для запросов, где нужен SQL engine:

- `WHERE`
- фильтрованные projection
- `GROUP BY`
- `SUM`
- агрегаты, которые не удалось выполнить из footer statistics
- `JOIN`

Для single-table запросов `ParquetManager` строит SQL поверх DuckDB table function `read_parquet([...])`.

Для join-запросов `ParquetManager` создает временные DuckDB views для alias-ов таблиц. Каждая view читает свои файлы через `read_parquet([...])`. После этого выполняется переписанный join SQL.

DuckDB возвращает результат через `DuckDBResultSet.arrowExportStream`. Сервер копирует строки из DuckDB Arrow stream в Flight batches и отправляет клиенту.

Для локальных файлов DuckDB extension не нужен. Для HDFS URI DuckDB должен загрузить HDFS extension.

Настройки HDFS extension:

- `duckDbHdfsExtension` или `DUCKDB_HDFS_EXTENSION`
- `duckDbAllowUnsignedExtensions` или `DUCKDB_ALLOW_UNSIGNED_EXTENSIONS`
- `duckDbHdfsDefaultNamenode` или `HDFS_DEFAULT_NAMENODE`
- `duckDbHdfsHaNamenodes` или `HDFS_HA_NAMENODES`
- `duckDbHdfsShortcircuit` или `HDFS_SHORTCIRCUIT`
- `duckDbHdfsDomainSocketPath` или `HDFS_DOMAIN_SOCKET_PATH`

## Runtime tuning

Основной файл настроек: `src/main/resources/arrowflight.properties`.

Главная общая настройка - `batchSize`. Она используется и для Acero scan batch size, и для DuckDB Arrow export batch size. Поэтому изменение `batchSize` влияет на размер batch-ей, которые уходят через Flight.

Параллелизм I/O считается так:

`max(ioParallelismMinThreads, min(availableProcessors, ioParallelismMaxCores) * ioParallelismMultiplier)`

Если `ioParallelism` задан явно, используется он. `ioParallelismMaxCores=0` означает "не ограничивать число ядер".

Пример override через JVM system property:

```bash
mvn test -Darrowflight.io.parallelism=64
```

## Отправка результата клиенту

Обычный `SELECT` обычно отправляет несколько Arrow batch-ей. Агрегации часто отправляют один итоговый batch, но это зависит от запроса и количества групп.

Отправка выполняется через `ServerStreamListener`. `VectorSchemaRoot` содержит текущий Arrow batch, а `putNext` отправляет его клиенту.

После завершения чтения сервер завершает stream.

## Текущие ограничения

Работа распределяется на уровне целых файлов. Один очень большой файл не делится между несколькими Flight-нодами.

Ticket является ссылкой на состояние запроса в Hazelcast, а не самостоятельным execution plan. Если cache entry истек или недоступен, чтение по ticket невозможно.

Фактическое выполнение запроса начинается только во время `DoGet`. `GetFlightInfo` отвечает за планирование, схему результата, endpoints и tickets.
