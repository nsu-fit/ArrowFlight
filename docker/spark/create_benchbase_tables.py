import os

from pyspark.sql import SparkSession


dataset = os.environ.get("BENCHMARK_DATASET", "tpch").lower()
schema = os.environ.get("BENCHMARK_LOAD_SCHEMA", "default")

spark = (
    SparkSession.builder.appName("BenchBaseSparkTableCreator")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))


def create_tpch_tables():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

    tables = {
        "region": """
            r_regionkey BIGINT,
            r_name STRING,
            r_comment STRING
        """,
        "nation": """
            n_nationkey BIGINT,
            n_name STRING,
            n_regionkey BIGINT,
            n_comment STRING
        """,
        "supplier": """
            s_suppkey BIGINT,
            s_name STRING,
            s_address STRING,
            s_nationkey BIGINT,
            s_phone STRING,
            s_acctbal DECIMAL(15,2),
            s_comment STRING
        """,
        "customer": """
            c_custkey BIGINT,
            c_name STRING,
            c_address STRING,
            c_nationkey BIGINT,
            c_phone STRING,
            c_acctbal DECIMAL(15,2),
            c_mktsegment STRING,
            c_comment STRING
        """,
        "part": """
            p_partkey BIGINT,
            p_name STRING,
            p_mfgr STRING,
            p_brand STRING,
            p_type STRING,
            p_size INT,
            p_container STRING,
            p_retailprice DECIMAL(15,2),
            p_comment STRING
        """,
        "partsupp": """
            ps_partkey BIGINT,
            ps_suppkey BIGINT,
            ps_availqty INT,
            ps_supplycost DECIMAL(15,2),
            ps_comment STRING
        """,
        "orders": """
            o_orderkey BIGINT,
            o_custkey BIGINT,
            o_orderstatus STRING,
            o_totalprice DECIMAL(15,2),
            o_orderdate DATE,
            o_orderpriority STRING,
            o_clerk STRING,
            o_shippriority INT,
            o_comment STRING
        """,
        "lineitem": """
            l_orderkey BIGINT,
            l_partkey BIGINT,
            l_suppkey BIGINT,
            l_linenumber INT,
            l_quantity DECIMAL(15,2),
            l_extendedprice DECIMAL(15,2),
            l_discount DECIMAL(15,2),
            l_tax DECIMAL(15,2),
            l_returnflag STRING,
            l_linestatus STRING,
            l_shipdate DATE,
            l_commitdate DATE,
            l_receiptdate DATE,
            l_shipinstruct STRING,
            l_shipmode STRING,
            l_comment STRING
        """,
    }

    for table, columns in tables.items():
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
        spark.sql(f"CREATE TABLE {schema}.{table} ({columns}) USING parquet")
        print(f"Created Spark table {schema}.{table}")


if dataset == "tpch":
    create_tpch_tables()
else:
    raise RuntimeError(
        f"Dataset '{dataset}' is not supported by create_benchbase_tables.py yet"
    )

spark.stop()
