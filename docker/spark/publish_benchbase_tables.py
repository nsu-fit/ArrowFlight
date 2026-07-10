import os
from pathlib import Path

from pyspark.sql import SparkSession

FLIGHT_SOURCE_PROVIDER = "net.surpin.data.arrowflight.client.spark.FlightSource"


def list_exported_tables(server_roots, schema):
    tables = set()
    for root in server_roots:
        schema_dir = root / schema
        if schema_dir.exists():
            tables.update(path.name for path in schema_dir.iterdir() if path.is_dir())
    return sorted(tables)


def register_flight_tables(spark, publish_schema, tables, host, port):
    if not tables:
        raise RuntimeError(f"No DuckDB Parquet tables found for schema '{publish_schema}'")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {publish_schema}")
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {publish_schema}.{table}")
        spark.sql(
            f"""
            CREATE TABLE {publish_schema}.{table}
            USING {FLIGHT_SOURCE_PROVIDER}
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
publish_schema = os.environ.get("BENCHMARK_SCHEMA", dataset)
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

if not server_roots:
    raise RuntimeError("FLIGHT_SERVER_DATA_DIRS is empty")

spark = (
    SparkSession.builder.appName("BenchBaseSparkFlightPublisher")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

register_flight_tables(
    spark,
    publish_schema,
    list_exported_tables(server_roots, publish_schema),
    flight_host,
    flight_port,
)

spark.stop()
