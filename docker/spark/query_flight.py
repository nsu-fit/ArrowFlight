import os
from functools import reduce

from pyspark.sql import SparkSession, functions as F


def parse_servers(value):
    servers = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            host, port = item.rsplit(":", 1)
        else:
            host, port = item, os.environ.get("FLIGHT_PORT", "32010")
        servers.append((host, int(port)))
    return servers


servers = parse_servers(os.environ.get("FLIGHT_SERVERS", "flight-server-1:32010"))
table = os.environ.get("FLIGHT_TABLE", "test_schema.test_table")
query = os.environ.get("FLIGHT_QUERY", "SELECT count(*) AS rows FROM flight_table")

if not servers:
    raise RuntimeError("FLIGHT_SERVERS is empty")

spark = SparkSession.builder.appName("ArrowFlightSparkClient").getOrCreate()


def load_flight_dataframe(host, port):
    endpoint = f"{host}:{port}"
    return (
        spark.read.format("flight")
        .option("host", host)
        .option("port", str(port))
        .option("user", os.environ.get("FLIGHT_USER", "user"))
        .option("password", os.environ.get("FLIGHT_PASSWORD", "password"))
        .option("tls.enabled", os.environ.get("FLIGHT_TLS_ENABLED", "false"))
        .option("table", table)
        .load()
        .withColumn("_flight_server", F.lit(endpoint))
    )


dataframes = []
for host, port in servers:
    endpoint = f"{host}:{port}"
    count = load_flight_dataframe(host, port).count()
    print(f"{endpoint}: {count} row(s)")
    dataframes.append(load_flight_dataframe(host, port))

flight_table = reduce(lambda left, right: left.unionByName(right), dataframes)
flight_table.createOrReplaceTempView("flight_table")

print(f"Running query: {query}")
spark.sql(query).show(100, truncate=False)
spark.stop()
