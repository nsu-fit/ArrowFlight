import os
import shutil
from pathlib import Path

import duckdb


TPCH_TABLES = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def clean_dir(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def load_extension(connection, name):
    try:
        connection.execute(f"LOAD {name}")
    except duckdb.Error:
        connection.execute(f"INSTALL {name}")
        connection.execute(f"LOAD {name}")


def table_names(connection, dataset):
    if dataset == "tpch":
        return TPCH_TABLES
    rows = connection.execute("SHOW TABLES").fetchall()
    return sorted(row[0] for row in rows)


def column_list(connection, table):
    rows = connection.execute(f"PRAGMA table_info({quote_identifier(table)})").fetchall()
    return ", ".join(quote_identifier(row[1]) for row in rows)


def export_table(connection, table, schema, server_roots):
    columns = column_list(connection, table)
    table_name = quote_identifier(table)

    for shard, root in enumerate(server_roots):
        table_dir = root / schema / table
        clean_dir(table_dir)
        target = table_dir / f"part-{shard:05d}.parquet"
        query = f"""
            COPY (
              WITH numbered AS (
                SELECT {columns}, row_number() OVER () AS __duckdb_row_number
                FROM {table_name}
              )
              SELECT {columns}
              FROM numbered
              WHERE ((__duckdb_row_number - 1) % {len(server_roots)}) = {shard}
            )
            TO {sql_string(target)}
            (FORMAT PARQUET)
        """
        connection.execute(query)

    print(f"Generated {schema}.{table} across {len(server_roots)} flight server volume(s)")


def main():
    dataset = os.environ.get("BENCHMARK_DATASET", "tpch").lower()
    schema = os.environ.get("BENCHMARK_SCHEMA", dataset).lower()
    scale_factor = float(os.environ.get("BENCHMARK_SCALE_FACTOR", "0.01"))
    server_roots = [
        Path(item)
        for item in os.environ.get(
            "FLIGHT_SERVER_DATA_DIRS",
            "/server-data/flight-server-1,/server-data/flight-server-2,/server-data/flight-server-3",
        ).split(",")
        if item
    ]

    if dataset not in {"tpch", "tpcds"}:
        raise RuntimeError("BENCHMARK_DATASET must be tpch or tpcds")
    if not server_roots:
        raise RuntimeError("FLIGHT_SERVER_DATA_DIRS is empty")

    for root in server_roots:
        clean_dir(root / schema)

    connection = duckdb.connect()
    if dataset == "tpch":
        load_extension(connection, "tpch")
        connection.execute(f"CALL dbgen(sf={scale_factor})")
    else:
        load_extension(connection, "tpcds")
        connection.execute(f"CALL dsdgen(sf={scale_factor})")

    for table in table_names(connection, dataset):
        export_table(connection, table, schema, server_roots)

    connection.close()
    print(f"Generated {dataset.upper()} sf={scale_factor} as Parquet for schema {schema}")


if __name__ == "__main__":
    main()
