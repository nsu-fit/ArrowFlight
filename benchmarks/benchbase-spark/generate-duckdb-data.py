import os
import json
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


def enabled(value):
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_query_ids(value):
    if not value:
        return []

    query_ids = []
    for token in str(value).lower().replace(" ", "").split(","):
        if not token:
            continue
        token = token[1:] if token.startswith("q") else token
        if token.isdigit():
            query_id = int(token)
            if 1 <= query_id <= 22 and query_id not in query_ids:
                query_ids.append(query_id)
    return query_ids


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
    flight_root,
    flight_hosts,
    direct_root,
    direct_partitions,
    generate_direct,
    reference_queries,
):
    if not output:
        return

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    metadata = {
        "dataset": dataset,
        "schema": schema,
        "scale_factor": scale_factor,
        "cluster_nodes": len(flight_hosts),
        "flight_hosts": os.environ.get("FLIGHT_HOSTS", ""),
        "flight_servers": os.environ.get("FLIGHT_SERVERS", ""),
        "flight_server_data_dirs": [str(flight_root)],
        "flight_source_host": os.environ.get("FLIGHT_SOURCE_HOST", "flight-server-1"),
        "flight_source_port": os.environ.get("FLIGHT_SOURCE_PORT", "32010"),
        "direct_parquet_dir": str(direct_root),
        "direct_parquet_partitions": direct_partitions,
        "direct_generated": generate_direct,
        "flight_data": [
            {
                "server_index": index + 1,
                "host": host,
                "root": str(flight_root),
                "schema_path": str(flight_root / schema),
                "tables": owner_table_stats(flight_root, schema, host),
                **owner_directory_stats(flight_root / schema, host),
            }
            for index, host in enumerate(flight_hosts)
        ],
        "direct_data": {
            "root": str(direct_root),
            "schema_path": str(direct_root / schema),
            "tables": table_stats(direct_root, schema),
            **directory_stats(direct_root / schema),
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
          WITH numbered AS (
            SELECT {columns}, row_number() OVER () AS __duckdb_row_number
            FROM {table_name}
          )
          SELECT {columns}
          FROM numbered
          WHERE ((__duckdb_row_number - 1) % {shard_count}) = {shard}
        )
        TO {sql_string(target)}
        (FORMAT PARQUET)
    """
    connection.execute(query)


def export_table_to_flight_root(connection, table, schema, flight_root, flight_hosts):
    shard_count = len(flight_hosts)
    table_dir = flight_root / schema / table
    clean_dir(table_dir)

    for shard, host in enumerate(flight_hosts):
        target = table_dir / f"part-{host}-{shard:05d}.parquet"
        export_table_shard(connection, table, target, shard_count, shard)

    print(f"Generated {schema}.{table} into {flight_root} across {len(flight_hosts)} flight shard owner(s)")


def export_table_to_direct_root(connection, table, schema, direct_root, shard_count):
    table_dir = direct_root / schema / table
    clean_dir(table_dir)

    for shard in range(shard_count):
        target = table_dir / f"part-{shard:05d}.parquet"
        export_table_shard(connection, table, target, shard_count, shard)

    print(f"Generated direct Spark parquet for {schema}.{table} with {shard_count} shard(s)")


def main():
    dataset = os.environ.get("BENCHMARK_DATASET", "tpch").lower()
    schema = os.environ.get("BENCHMARK_SCHEMA", dataset).lower()
    scale_factor = float(os.environ.get("BENCHMARK_SCALE_FACTOR", "0.01"))
    generate_direct = enabled(os.environ.get("GENERATE_DIRECT_PARQUET", "false"))
    direct_root = Path(os.environ.get("DIRECT_PARQUET_DIR", "/direct-data"))
    query_ids = parse_query_ids(os.environ.get("BENCHMARK_QUERY_SET", ""))
    metadata_out = os.environ.get("BENCHMARK_METADATA_OUT", "")
    server_roots = [
        Path(item)
        for item in os.environ.get(
            "FLIGHT_SERVER_DATA_DIRS", "/server-data/all",
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

    for root in server_roots:
        clean_dir(root / schema)
    if generate_direct:
        clean_dir(direct_root / schema)

    direct_partitions = int(os.environ.get("DIRECT_PARQUET_PARTITIONS") or len(server_roots))
    if direct_partitions < 1:
        raise RuntimeError("DIRECT_PARQUET_PARTITIONS must be >= 1")

    connection = duckdb.connect()
    if dataset == "tpch":
        load_extension(connection, "tpch")
        connection.execute(f"CALL dbgen(sf={scale_factor})")
    else:
        load_extension(connection, "tpcds")
        connection.execute(f"CALL dsdgen(sf={scale_factor})")

    for table in table_names(connection, dataset):
        export_table_to_flight_root(connection, table, schema, server_roots[0], flight_hosts)
        if generate_direct:
            export_table_to_direct_root(connection, table, schema, direct_root, direct_partitions)

    reference_queries = []
    if dataset == "tpch":
        reference_queries = tpch_reference_queries(connection, query_ids)

    write_metadata(
        metadata_out,
        dataset,
        schema,
        scale_factor,
        server_roots[0],
        flight_hosts,
        direct_root,
        direct_partitions,
        generate_direct,
        reference_queries,
    )

    connection.close()
    print(f"Generated {dataset.upper()} sf={scale_factor} as Parquet for schema {schema}")
    if generate_direct:
        print(f"Generated direct Spark parquet under {direct_root / schema}")


if __name__ == "__main__":
    main()
