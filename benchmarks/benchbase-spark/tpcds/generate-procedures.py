#!/usr/bin/env python3
"""
Generate BenchBase procedure classes from DuckDB TPC-DS SQL queries.

Reads 01.sql..99.sql from the queries/ directory and produces Q1.java..Q99.java.
Each class extends com.oltpbenchmark.api.Procedure and executes the query
as a simple Statement (no prepared-statement parameters needed — DuckDB queries
use hardcoded substitution variables).

Usage:
    python generate-procedures.py              # generate Java from existing .sql
    python generate-procedures.py --download   # download + generate
"""

import os
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlretrieve

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
QUERIES_DIR = SCRIPT_DIR / "queries"
OUTPUT_DIR = SCRIPT_DIR / "src" / "com" / "oltpbenchmark" / "benchmarks" / "tpcds" / "procedures"

BASE_URL = "https://raw.githubusercontent.com/duckdb/duckdb/main/extension/tpcds/dsdgen/queries"


def download_one(n: int) -> tuple[int, bool]:
    num = f"{n:02d}"
    url = f"{BASE_URL}/{num}.sql"
    dest = QUERIES_DIR / f"{num}.sql"
    try:
        urlretrieve(url, dest)
        return n, True
    except Exception as e:
        print(f"  Q{n}: {e}", file=sys.stderr)
        return n, False


def download_queries() -> int:
    QUERIES_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading 99 TPC-DS queries from DuckDB (parallel x10)...")
    ok = 0
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(download_one, n): n for n in range(1, 100)}
        for future in as_completed(futures):
            n, success = future.result()
            if success:
                ok += 1
    print(f"  Downloaded {ok}/99 queries")
    return ok


def sql_to_java_lines(sql: str) -> list[str]:
    lines = []
    sql_lines = sql.strip().splitlines()
    for i, line in enumerate(sql_lines):
        escaped = (line
            .replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\t", "    "))
        if i == 0:
            lines.append(f'        "{escaped}\\n"')
        else:
            lines.append(f'        + "{escaped}\\n"')
    return lines


def generate_class(num: int, sql: str) -> str:
    java_lines = sql_to_java_lines(sql)
    class_lines = [
        "package com.oltpbenchmark.benchmarks.tpcds.procedures;",
        "",
        "import java.sql.Connection;",
        "import java.sql.ResultSet;",
        "import java.sql.SQLException;",
        "import java.sql.Statement;",
        "",
        "/**",
        " * TPC-DS Query %d." % num,
        ' * SQL source: DuckDB TPC-DS extension (standard SQL, Spark-compatible).',
        " */",
        "public final class Q%d extends TPCDSProcedure {" % num,
        "",
        "    private static final String SQL =",
    ]
    class_lines.extend(java_lines)
    class_lines[-1] += ";"
    class_lines.extend([
        "",
        "    @Override",
        "    public void run(final Connection conn) throws SQLException {",
        "        try (Statement stmt = conn.createStatement();",
        "             ResultSet rs = stmt.executeQuery(SQL)) {",
        "            while (rs.next()) {",
        "                // drain result set",
        "            }",
        "        }",
        "    }",
        "}",
    ])
    return "\n".join(class_lines) + "\n"


def generate_procedures() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    generated = 0
    for n in range(1, 100):
        num = f"{n:02d}"
        sql_file = QUERIES_DIR / f"{num}.sql"
        if not sql_file.exists():
            print(f"  SKIP  {sql_file} — not found")
            continue

        sql = sql_file.read_text(encoding="utf-8")
        java_source = generate_class(n, sql)

        out_file = OUTPUT_DIR / f"Q{n}.java"
        out_file.write_text(java_source, encoding="utf-8")
        generated += 1

    return generated


def main() -> None:
    if "--download" in sys.argv:
        ok = download_queries()
        if ok < 99:
            print(f"ERROR: only {ok}/99 queries downloaded", file=sys.stderr)
            sys.exit(1)

    generated = generate_procedures()
    print(f"Generated {generated} procedure classes in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
