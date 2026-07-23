import os
import json
import re
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

TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]

MAX_QUERY_ID = {"tpch": 22, "tpcds": 99}


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def clean_dir(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def enabled(value):
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_query_ids(value, max_id=22):
    if not value:
        return []

    if str(value).strip().lower() == "all":
        return list(range(1, 23))

    query_ids = []
    for token in str(value).lower().replace(" ", "").split(","):
        if not token:
            continue
        token = token[1:] if token.startswith("q") else token
        if token.isdigit():
            query_id = int(token)
            if 1 <= query_id <= max_id and query_id not in query_ids:
                query_ids.append(query_id)
    return query_ids


def spark_compatible_reference_sql(query_id, sql):
    if query_id != 17:
        return sql
    return re.sub(
        r"0\.2\s*\*\s*avg\s*\(\s*l_quantity\s*\)",
        "0.2 * AVG(CAST(l_quantity AS DOUBLE))",
        sql,
        flags=re.IGNORECASE,
    )


def json_safe(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def directory_stats(path):
    files = []
    total_bytes = 0
    if path.exists():
        for file in sorted(path.rglob("*.parquet")):
            size = file.stat().st_size
            total_bytes += size
            files.append(
                {
                    "path": str(file),
                    "bytes": size,
                    "relative_path": str(file.relative_to(path)),
                }
            )
    return {"path": str(path), "bytes": total_bytes, "files": len(files), "file_details": files}


def owner_directory_stats(path, owner):
    files = []
    total_bytes = 0
    if path.exists():
        for file in sorted(path.rglob("*.parquet")):
            if owner not in file.name:
                continue
            size = file.stat().st_size
            total_bytes += size
            files.append(
                {
                    "path": str(file),
                    "bytes": size,
                    "relative_path": str(file.relative_to(path)),
                }
            )
    return {"path": str(path), "bytes": total_bytes, "files": len(files), "file_details": files}


def table_stats(root, schema):
    schema_dir = root / schema
    tables = []
    if schema_dir.exists():
        for table_dir in sorted(path for path in schema_dir.iterdir() if path.is_dir()):
            stats = directory_stats(table_dir)
            stats["table"] = table_dir.name
            tables.append(stats)
    return tables


def owner_table_stats(root, schema, owner):
    schema_dir = root / schema
    tables = []
    if schema_dir.exists():
        for table_dir in sorted(path for path in schema_dir.iterdir() if path.is_dir()):
            stats = owner_directory_stats(table_dir, owner)
            stats["table"] = table_dir.name
            tables.append(stats)
    return tables


def tpch_reference_queries(connection, query_ids):
    if not query_ids:
        return []

    queries = {
        int(row[0]): row[1]
        for row in connection.execute("FROM tpch_queries()").fetchall()
    }
    references = []
    for query_id in query_ids:
        cursor = connection.execute(f"PRAGMA tpch({query_id})")
        columns = [column[0] for column in cursor.description]
        rows = [
            {columns[index]: json_safe(value) for index, value in enumerate(row)}
            for row in cursor.fetchall()
        ]
        references.append(
            {
                "query_id": query_id,
                "name": f"Q{query_id}",
                "sql": spark_compatible_reference_sql(
                    query_id, queries.get(query_id, "")
                ),
                "columns": columns,
                "expected_rows": rows,
                "expected_row_count": len(rows),
            }
        )
    return references


def tpcds_reference_queries(connection, query_ids):
    if not query_ids:
        return []

    queries = {
        int(row[0]): row[1]
        for row in connection.execute("FROM tpcds_queries()").fetchall()
    }
    references = []
    for query_id in query_ids:
        cursor = connection.execute(f"PRAGMA tpcds({query_id})")
        columns = [column[0] for column in cursor.description]
        rows = [
            {columns[index]: json_safe(value) for index, value in enumerate(row)}
            for row in cursor.fetchall()
        ]
        references.append(
            {
                "query_id": query_id,
                "name": f"Q{query_id}",
                "sql": queries.get(query_id, ""),
                "columns": columns,
                "expected_rows": rows,
                "expected_row_count": len(rows),
            }
        )
    return references


def write_metadata(
    output,
    dataset,
    schema,
    scale_factor,
    flight_roots,
    flight_hosts,
    hdfs_root,
    hdfs_block_size,
    reference_queries,
):
    if not output:
        return

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    node_stats = [directory_stats(root / schema) for root in flight_roots]
    metadata = {
        "dataset": dataset,
        "schema": schema,
        "scale_factor": scale_factor,
        "storage": "hdfs",
        "hdfs_data_dir": hdfs_root,
        "hdfs_replication": 1,
        "hdfs_block_size_bytes": hdfs_block_size,
        "shared_parquet_dataset": True,
        "cluster_nodes": len(flight_hosts),
        "flight_hosts": os.environ.get("FLIGHT_HOSTS", ""),
        "flight_servers": os.environ.get("FLIGHT_SERVERS", ""),
        "flight_server_data_dirs": [str(root) for root in flight_roots],
        "flight_source_host": os.environ.get("FLIGHT_SOURCE_HOST", "flight-server-1"),
        "flight_source_port": os.environ.get("FLIGHT_SOURCE_PORT", "32010"),
        "direct_parquet_dir": hdfs_root,
        "direct_parquet_partitions": len(flight_roots),
        "direct_generated": True,
        "flight_data": [
            {
                "server_index": index + 1,
                "host": host,
                "root": str(flight_roots[index]),
                "schema_path": str(flight_roots[index] / schema),
                "tables": table_stats(flight_roots[index], schema),
                **directory_stats(flight_roots[index] / schema),
            }
            for index, host in enumerate(flight_hosts)
        ],
        "direct_data": {
            "root": hdfs_root,
            "schema_path": f"{hdfs_root.rstrip('/')}/{schema}",
            "tables": [],
            "bytes": sum(stats["bytes"] for stats in node_stats),
            "files": sum(stats["files"] for stats in node_stats),
            "file_details": [],
        },
        "reference_queries": reference_queries,
    }

    output_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote benchmark metadata to {output_path}")


def load_extension(connection, name):
    try:
        connection.execute(f"LOAD {name}")
    except duckdb.Error:
        connection.execute(f"INSTALL {name}")
        connection.execute(f"LOAD {name}")


def table_names(connection, dataset):
    if dataset == "tpch":
        return TPCH_TABLES
    if dataset == "tpcds":
        return TPCDS_TABLES
    rows = connection.execute("SHOW TABLES").fetchall()
    return sorted(row[0] for row in rows)


def column_list(connection, table):
    rows = connection.execute(f"PRAGMA table_info({quote_identifier(table)})").fetchall()
    return ", ".join(quote_identifier(row[1]) for row in rows)


def export_table_shard(connection, table, target, shard_count, shard):
    columns = column_list(connection, table)
    table_name = quote_identifier(table)
    query = f"""
        COPY (
          SELECT {columns}
          FROM {table_name}
          WHERE (rowid % {shard_count}) = {shard}
        )
        TO {sql_string(target)}
        (FORMAT PARQUET)
    """
    connection.execute(query)
    if not target.is_file() or target.stat().st_size == 0:
        raise RuntimeError(f"DuckDB did not create Parquet shard {target}")


def export_table_to_flight_roots(connection, table, schema, flight_roots, flight_hosts):
    shard_count = len(flight_hosts)
    for flight_root in flight_roots:
        clean_dir(flight_root / schema / table)

    for shard, (host, flight_root) in enumerate(zip(flight_hosts, flight_roots)):
        table_dir = flight_root / schema / table
        target = table_dir / f"part-{host}-{shard:05d}.parquet"
        export_table_shard(connection, table, target, shard_count, shard)

    print(f"Generated {schema}.{table} across {len(flight_hosts)} node-local Flight root(s)")


def main():
    dataset = os.environ.get("BENCHMARK_DATASET", "tpch").lower()
    schema = os.environ.get("BENCHMARK_SCHEMA", dataset).lower()
    scale_factor = float(os.environ.get("BENCHMARK_SCALE_FACTOR", "0.01"))
    hdfs_root = os.environ.get("HDFS_DATA_DIR", "hdfs://hdfs-namenode:8020/bench")
    hdfs_block_size = int(os.environ.get("HDFS_BLOCK_SIZE_BYTES", "1073741824"))
    query_ids = parse_query_ids(os.environ.get("BENCHMARK_QUERY_SET", ""), MAX_QUERY_ID.get(dataset, 22))
    metadata_out = os.environ.get("BENCHMARK_METADATA_OUT", "")
    server_roots = [
        Path(item)
        for item in os.environ.get(
            "FLIGHT_SERVER_DATA_DIRS",
            "/server-data/flight-server-1,/server-data/flight-server-2,/server-data/flight-server-3",
        ).split(",")
        if item
    ]
    flight_hosts = [
        item.strip()
        for item in os.environ.get(
            "FLIGHT_HOSTS", "flight-server-1,flight-server-2,flight-server-3",
        ).split(",")
        if item.strip()
    ]

    if dataset not in {"tpch", "tpcds"}:
        raise RuntimeError("BENCHMARK_DATASET must be tpch or tpcds")
    if not server_roots:
        raise RuntimeError("FLIGHT_SERVER_DATA_DIRS is empty")
    if not flight_hosts:
        raise RuntimeError("FLIGHT_HOSTS is empty")
    if len(server_roots) != len(flight_hosts):
        raise RuntimeError(
            "FLIGHT_SERVER_DATA_DIRS must contain exactly one node-local directory "
            "for every FLIGHT_HOSTS entry"
        )

    for root in server_roots:
        clean_dir(root / schema)
    if hdfs_block_size < 1048576:
        raise RuntimeError("HDFS_BLOCK_SIZE_BYTES must be at least 1048576")

    connection = duckdb.connect()
    if dataset == "tpch":
        load_extension(connection, "tpch")
        connection.execute(f"CALL dbgen(sf={scale_factor})")
    else:
        load_extension(connection, "tpcds")
        connection.execute(f"CALL dsdgen(sf={scale_factor})")

    for table in table_names(connection, dataset):
        export_table_to_flight_roots(connection, table, schema, server_roots, flight_hosts)

    oversized = [
        file
        for root in server_roots
        for file in root.joinpath(schema).rglob("*.parquet")
        if file.stat().st_size > hdfs_block_size
    ]
    if oversized:
        largest = max(oversized, key=lambda file: file.stat().st_size)
        raise RuntimeError(
            f"Parquet shard {largest} is {largest.stat().st_size} bytes, larger than "
            f"HDFS_BLOCK_SIZE_BYTES={hdfs_block_size}. Increase HDFS_BLOCK_SIZE_BYTES so "
            "every benchmark shard occupies one HDFS block."
        )

    reference_queries = []
    if dataset == "tpch":
        reference_queries = tpch_reference_queries(connection, query_ids)
    elif dataset == "tpcds":
        reference_queries = tpcds_reference_queries(connection, query_ids)

    write_metadata(
        metadata_out,
        dataset,
        schema,
        scale_factor,
        server_roots,
        flight_hosts,
        hdfs_root,
        hdfs_block_size,
        reference_queries,
    )

    connection.close()
    print(f"Generated {dataset.upper()} sf={scale_factor} as Parquet for schema {schema}")
    print(f"Staged one shared HDFS dataset for upload to {hdfs_root}/{schema}")


if __name__ == "__main__":
    main()
