# BenchBase TPC-H/TPC-DS на Ubuntu server

Минимальный baseline для BenchBase через Docker Compose на Ubuntu server.

Важно: BenchBase работает через JDBC. Текущий Arrow Flight SQL server напрямую не JDBC-цель. Для бенча именно Arrow Flight нужен отдельный Flight benchmark client или JDBC bridge.

## Нужна ли Java?

Если запускаешь через Docker, Java на Ubuntu host не нужна. Java уже внутри BenchBase image.

Если собираешь BenchBase из исходников без Docker, нужна Java 21 и Maven/Maven Wrapper.

## Установка на Ubuntu

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker "$USER"
```

После `usermod` перелогинься или выполни `newgrp docker`.

## Быстрый старт TPC-H

Из корня репозитория:

```bash
bash benchbase/run-benchmark.sh tpch all
```

Что делает команда:

1. Поднимает PostgreSQL на `localhost:5432`.
2. Создает TPC-H schema.
3. Загружает данные.
4. Выполняет Q1-Q22.
5. Кладет результаты в `benchbase/results/`.

## Частые команды

Повторить только запросы, без пересоздания и загрузки данных:

```bash
bash benchbase/run-benchmark.sh tpch execute
```

Запустить только часть TPC-H queries:

```bash
bash benchbase/run-benchmark.sh tpch execute q6
bash benchbase/run-benchmark.sh tpch execute q1,q6,q14
```

Для быстрой проверки лучше `q6`: простой запрос, обычно проходит быстрее тяжелых join queries.

Раздельные шаги:

```bash
bash benchbase/run-benchmark.sh tpch create
bash benchbase/run-benchmark.sh tpch load
bash benchbase/run-benchmark.sh tpch execute
```

Очистить таблицы BenchBase:

```bash
bash benchbase/run-benchmark.sh tpch clear
```

Полностью снести PostgreSQL volume:

```bash
bash benchbase/run-benchmark.sh tpch down
```

Если порт `5432` занят:

```bash
BENCH_POSTGRES_PORT=15432 bash benchbase/run-benchmark.sh tpch all
```

Внутри Docker BenchBase все равно ходит на `postgres:5432`; переменная меняет только порт на хосте.

## Настройка нагрузки

Файл: `benchbase/config/postgres/tpch.xml`.

Главные ручки:

- `<scalefactor>0.1</scalefactor>` - размер данных. `0.1` быстро, `1` около 1 GB.
- `<terminals>1</terminals>` - число worker threads.
- `<serial>true</serial>` - запросы идут последовательно. Для простой проверки оставь `true`.
- `<weights>...</weights>` - веса Q1-Q22.

Третий аргумент runner временно меняет `<weights>` без правки основного XML. Например `q1,q6` превращается в веса `1,0,0,0,0,1,0...`.

После изменения `scalefactor` лучше пересоздать данные:

```bash
bash benchbase/run-benchmark.sh tpch down
bash benchbase/run-benchmark.sh tpch all
```

## TPC-DS

TPC-DS сейчас не включен. Причина: в upstream BenchBase есть `sample_tpcds_config.xml`, но он пустой и содержит только todo. Поэтому я не добавляю фейковый config.

Скрипт явно остановится:

```bash
bash benchbase/run-benchmark.sh tpcds all
```

Когда появится валидный `benchbase/config/postgres/tpcds.xml`, runner можно будет расширить на `tpcds`.

## Где смотреть результат

Смотри `benchbase/results/`. BenchBase пишет summary, latency/throughput files и transaction stats. Для первичной оценки бери latency per query и total runtime из summary.
