import os
import random
import shutil
from pathlib import Path

from pyspark.sql import SparkSession


def env_int(name, default):
    return int(os.environ.get(name, str(default)))


def clean_dir(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def list_loaded_tables(spark, schema):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    rows = spark.sql(f"SHOW TABLES IN {schema}").collect()
    return sorted(row.tableName for row in rows if not row.isTemporary)


def list_exported_tables(server_roots, schema):
    tables = set()
    for root in server_roots:
        schema_dir = root / schema
        if schema_dir.exists():
            tables.update(path.name for path in schema_dir.iterdir() if path.is_dir())
    return sorted(tables)


def export_tables(spark, load_schema, publish_schema, server_roots, export_root, partitions, seed):
    tables = list_loaded_tables(spark, load_schema)
    if not tables:
        raise RuntimeError(f"No BenchBase tables found in Spark schema '{load_schema}'")

    rng = random.Random(seed)
    for table in tables:
        source_name = f"{load_schema}.{table}"
        export_path = export_root / publish_schema / table
        clean_dir(export_path)

        dataframe = spark.table(source_name)
        dataframe.repartition(max(1, partitions)).write.mode("overwrite").parquet(str(export_path))

        for root in server_roots:
            clean_dir(root / publish_schema / table)

        files = sorted(export_path.glob("*.parquet"))
        if not files:
            raise RuntimeError(f"No parquet files exported for {source_name}")

        rng.shuffle(files)
        for index, file_path in enumerate(files):
            target_root = server_roots[index % len(server_roots)]
            target_path = target_root / publish_schema / table / file_path.name
            shutil.move(str(file_path), str(target_path))

        print(f"Published {source_name} -> {publish_schema}.{table}: {len(files)} parquet file(s)")


def register_flight_tables(spark, publish_schema, tables, host, port):
    if not tables:
        raise RuntimeError(f"No exported tables found for schema '{publish_schema}'")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {publish_schema}")
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {publish_schema}.{table}")
        spark.sql(
            f"""
            CREATE TABLE {publish_schema}.{table}
            USING flight
            OPTIONS (
              host '{host}',
              port '{port}',
              user 'user',
              password 'password',
              `tls.enabled` 'false',
              table '{publish_schema}.{table}'
            )
            """
        )
        print(f"Registered Spark table {publish_schema}.{table} via flight://{host}:{port}/{publish_schema}.{table}")


dataset = os.environ.get("BENCHMARK_DATASET", "tpch").lower()
load_schema = os.environ.get("BENCHMARK_LOAD_SCHEMA", "default")
publish_schema = os.environ.get("BENCHMARK_SCHEMA", dataset)
action = os.environ.get("PUBLISH_ACTION", "all").lower()
partitions = env_int("TEST_PARTITIONS", 24)
seed = env_int("TEST_SEED", 47)
export_root = Path(os.environ.get("GENERATED_ROOT", "/generated/benchbase-export"))
server_roots = [
    Path(item)
    for item in os.environ.get(
        "FLIGHT_SERVER_DATA_DIRS",
        "/server-data/flight-server-1,/server-data/flight-server-2,/server-data/flight-server-3",
    ).split(",")
    if item
]
flight_host = os.environ.get("FLIGHT_SOURCE_HOST", "flight-server-1")
flight_port = os.environ.get("FLIGHT_SOURCE_PORT", "32010")

if action not in {"all", "export", "register"}:
    raise RuntimeError("PUBLISH_ACTION must be one of: all, export, register")

if not server_roots:
    raise RuntimeError("FLIGHT_SERVER_DATA_DIRS is empty")

spark = (
    SparkSession.builder.appName("BenchBaseSparkFlightPublisher")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

if action in {"all", "export"}:
    export_tables(spark, load_schema, publish_schema, server_roots, export_root, partitions, seed)

if action in {"all", "register"}:
    register_flight_tables(
        spark,
        publish_schema,
        list_exported_tables(server_roots, publish_schema),
        flight_host,
        flight_port,
    )

spark.stop()
