# BenchBase Spark: сравнительные тесты на Linux

Guide запускает одинаковый набор TPC-H queries двумя путями:

- `Flight` - Spark читает Parquet через Arrow Flight SQL;
- `Direct` - Spark читает те же данные напрямую через Parquet DataSource.

## Требования

- Linux x86_64;
- Docker Engine;
- Docker Compose v2 (`docker compose`);
- Bash, Python 3, `awk`, `sed` и `tee`;
- минимум 8 GB RAM, рекомендовано 12-16 GB.

Проверка окружения:

```bash
docker --version
docker compose version
docker info
python3 --version
```

Если `docker info` требует root, запускай команды через `sudo` или добавь пользователя в группу `docker` и заново войди в shell.

## Полное сравнение

Из корня репозитория:

```bash
chmod +x benchmarks/benchbase-spark/run-benchbase-spark.sh

BENCHMARK_SCALE_FACTOR=0.01 \
BENCHBASE_TIME_SECONDS=60 \
BENCHBASE_TERMINALS=1 \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh \
  tpch compare q1,q6,q14
```

`compare` выполняет полный цикл:

1. удаляет старые benchmark containers и volumes;
2. генерирует один TPC-H dataset;
3. публикует `tpch_flight` и `tpch_direct`;
4. запускает выбранные queries для обоих путей;
5. строит общий HTML report и обновляет локальную папку `pages/`.

В generated report и reference results попадут только `q1`, `q6`, `q14`. BenchBase может вывести в консоль остальные transaction types с нулевыми counters; они не выполняются и не добавляются в per-query report.

Результат:

```text
benchmarks/benchbase-spark/results/tpch-compare-q1,q6,q14-<timestamp>/compare.report.html
```

Внутри также будут отдельные reports:

```text
flight/*.report.html
direct/*.report.html
```

## Основные параметры

```bash
BENCHMARK_SCALE_FACTOR=0.1       # TPC-H scale factor
BENCHBASE_TIME_SECONDS=120      # длительность каждого пути
BENCHBASE_TERMINALS=2           # параллельные BenchBase workers
BENCHBASE_RATE=unlimited        # лимит requests/sec
BENCHBASE_UPDATE_PAGES=false    # не обновлять локальную pages/
```

Пример более тяжёлого запуска:

```bash
BENCHMARK_SCALE_FACTOR=1 \
BENCHBASE_TIME_SECONDS=300 \
BENCHBASE_TERMINALS=4 \
BENCHBASE_UPDATE_PAGES=false \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh \
  tpch compare q1,q6,q14
```

## Одиночный тест

Запуск Flight с нуля, один проход Q6:

```bash
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch fresh q6
```

Несколько выбранных queries, по одному проходу каждого:

```bash
bash benchmarks/benchbase-spark/run-benchbase-spark.sh \
  tpch fresh q1,q6,q14
```

## Диагностика и остановка

Логи всех benchmark services:

```bash
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch logs
```

Проверка состояния:

```bash
docker compose -f docker-compose.yml --profile benchbase ps
```

Остановка с удалением benchmark volumes:

```bash
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch down
```

`compare`, `fresh`, `prepare` и `down` удаляют benchmark volumes. Не используй их, если текущий generated dataset нужно сохранить.
