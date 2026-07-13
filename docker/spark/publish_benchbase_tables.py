import os
from pathlib import Path

from pyspark.sql import SparkSession

FLIGHT_SOURCE_PROVIDER = "net.surpin.data.arrowflight.client.spark.FlightSource"


def quote_identifier(value):
    return "`" + value.replace("`", "``") + "`"


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def list_exported_tables(roots, schema):
    tables_by_root = {}
    for root in roots:
        schema_dir = root / schema
        tables = set()
        if schema_dir.exists():
            tables.update(
                path.name
                for path in schema_dir.iterdir()
                if path.is_dir() and any(path.rglob("*.parquet"))
            )
        tables_by_root[root] = tables

    all_tables = set().union(*tables_by_root.values()) if tables_by_root else set()
    missing = {
        str(root): sorted(all_tables - tables)
        for root, tables in tables_by_root.items()
        if all_tables - tables
    }
    if missing:
        raise RuntimeError(
            f"Incomplete node-local Parquet layout for schema '{schema}': {missing}"
        )
    return sorted(all_tables)


def create_database(spark, schema):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(schema)}")


def register_flight_tables(spark, publish_schema, source_schema, tables, host, port):
    if not tables:
        raise RuntimeError(f"No Flight Parquet tables found for schema '{source_schema}'")

    create_database(spark, publish_schema)
    for table in tables:
        spark_table = f"{quote_identifier(publish_schema)}.{quote_identifier(table)}"
        source_table = f"{source_schema}.{table}"
        spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
        spark.sql(
            f"""
            CREATE TABLE {spark_table}
            USING {FLIGHT_SOURCE_PROVIDER}
            OPTIONS (
              host {sql_string(host)},
              port {sql_string(port)},
              user 'user',
              password 'password',
              `tls.enabled` 'false',
              table {sql_string(source_table)}
            )
            """
        )
        if not spark.table(spark_table).schema.fields:
            raise RuntimeError(
                f"Flight returned an empty schema for {source_table}; "
                f"verify the Parquet shard mounted on {host}"
            )
        print(f"Registered Spark Flight table {publish_schema}.{table} via flight://{host}:{port}/{source_table}")


def register_direct_parquet_tables(spark, publish_schema, source_schema, tables, direct_root):
    if not tables:
        raise RuntimeError(f"No direct Parquet tables found for schema '{source_schema}' under {direct_root}")

    create_database(spark, publish_schema)
    for table in tables:
        spark_table = f"{quote_identifier(publish_schema)}.{quote_identifier(table)}"
        location = direct_root / source_schema / table
        spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
        spark.sql(
            f"""
            CREATE TABLE {spark_table}
            USING parquet
            LOCATION {sql_string(location)}
            """
        )
        print(f"Registered Spark direct Parquet table {publish_schema}.{table} at {location}")


dataset = os.environ.get("BENCHMARK_DATASET", "tpch").lower()
source_schema = os.environ.get("BENCHMARK_SOURCE_SCHEMA", os.environ.get("BENCHMARK_SCHEMA", dataset)).lower()
publish_schema = os.environ.get("SPARK_PUBLISH_SCHEMA") or os.environ.get("BENCHMARK_SCHEMA", source_schema)
publish_mode = os.environ.get("BENCHMARK_PUBLISH_MODE", "flight").lower()
flight_publish_schema = os.environ.get("FLIGHT_PUBLISH_SCHEMA", f"{source_schema}_flight")
direct_publish_schema = os.environ.get("DIRECT_PUBLISH_SCHEMA", f"{source_schema}_direct")
direct_root = Path(os.environ.get("DIRECT_PARQUET_DIR", "/direct-data"))
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
    SparkSession.builder.appName("BenchBaseSparkTablePublisher")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

flight_tables = list_exported_tables(server_roots, source_schema)
direct_tables = list_exported_tables([direct_root], source_schema)

if publish_mode == "flight":
    register_flight_tables(spark, publish_schema, source_schema, flight_tables, flight_host, flight_port)
elif publish_mode == "direct":
    register_direct_parquet_tables(spark, publish_schema, source_schema, direct_tables, direct_root)
elif publish_mode == "both":
    register_flight_tables(spark, flight_publish_schema, source_schema, flight_tables, flight_host, flight_port)
    register_direct_parquet_tables(spark, direct_publish_schema, source_schema, direct_tables, direct_root)
else:
    raise RuntimeError("BENCHMARK_PUBLISH_MODE must be flight, direct, or both")

spark.stop()
