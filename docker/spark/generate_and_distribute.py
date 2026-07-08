import os
import random
import shutil
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def env_int(name, default):
    return int(os.environ.get(name, str(default)))


def clean_dir(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


rows = env_int("TEST_ROWS", 1_000_000)
partitions = env_int("TEST_PARTITIONS", 24)
schema = os.environ.get("TEST_SCHEMA", "test_schema")
table = os.environ.get("TEST_TABLE", "test_table")
seed = env_int("TEST_SEED", 47)
generated_root = Path(os.environ.get("GENERATED_ROOT", "/generated/raw"))
server_roots = [
    Path(item)
    for item in os.environ.get(
        "FLIGHT_SERVER_DATA_DIRS",
        "/server-data/flight-server-1,/server-data/flight-server-2,/server-data/flight-server-3",
    ).split(",")
    if item
]

if not server_roots:
    raise RuntimeError("FLIGHT_SERVER_DATA_DIRS is empty")

source_table_dir = generated_root / schema / table
clean_dir(source_table_dir)

spark = SparkSession.builder.appName("ArrowFlightDataGenerator").getOrCreate()

id_col = F.col("id")
df = spark.range(0, rows, 1, partitions).select(
    id_col.cast("integer").alias("id"),
    (F.pmod(id_col, F.lit(2)) == F.lit(0)).alias("bool_col"),
    F.pmod(id_col, F.lit(10)).cast("byte").alias("tinyint_col"),
    F.pmod(id_col, F.lit(10)).cast("short").alias("smallint_col"),
    F.pmod(id_col, F.lit(10)).cast("integer").alias("int_col"),
    (F.pmod(id_col, F.lit(10)) * F.lit(10)).cast("long").alias("bigint_col"),
    F.rand(seed).cast("float").alias("float_col"),
    F.rand(seed + 1).cast("double").alias("double_col"),
    F.concat(
        F.lit("20"),
        F.format_string("%02d", (F.floor(F.rand(seed + 2) * F.lit(24)) + F.lit(1)).cast("integer")),
        F.lit("-"),
        F.format_string("%02d", (F.floor(F.rand(seed + 3) * F.lit(12)) + F.lit(1)).cast("integer")),
        F.lit("-"),
        F.format_string("%02d", (F.floor(F.rand(seed + 4) * F.lit(28)) + F.lit(1)).cast("integer")),
    ).alias("date_string_col"),
    F.concat(
        F.lit("str_"),
        F.lpad((F.rand(seed + 5) * F.lit(1_000_000)).cast("integer").cast("string"), 6, "0"),
    ).alias("string_col"),
    F.floor(F.rand(seed + 6) * F.lit(1_000_000_000_000)).cast("long").alias("timestamp_col"),
    (F.floor(F.rand(seed + 7) * F.lit(25)) + F.lit(2000)).cast("integer").alias("year"),
    (F.floor(F.rand(seed + 8) * F.lit(12)) + F.lit(1)).cast("integer").alias("month"),
)

df.write.mode("overwrite").format("parquet").save(str(source_table_dir))
spark.stop()

for root in server_roots:
    clean_dir(root / schema / table)

files = sorted(source_table_dir.glob("*.parquet"))
if not files:
    raise RuntimeError(f"No parquet files generated in {source_table_dir}")

rng = random.Random(seed)
rng.shuffle(files)

counts = {str(root): 0 for root in server_roots}
bytes_by_root = {str(root): 0 for root in server_roots}

for index, file_path in enumerate(files):
    target_root = server_roots[index % len(server_roots)]
    target_path = target_root / schema / table / file_path.name
    size = file_path.stat().st_size
    shutil.move(str(file_path), str(target_path))
    counts[str(target_root)] += 1
    bytes_by_root[str(target_root)] += size

print("Generated and distributed parquet files")
print(f"Rows: {rows}")
print(f"Partitions/files: {len(files)}")
print(f"Table: {schema}.{table}")
for root in server_roots:
    key = str(root)
    print(f"{key}: {counts[key]} file(s), {bytes_by_root[key]} byte(s)")
