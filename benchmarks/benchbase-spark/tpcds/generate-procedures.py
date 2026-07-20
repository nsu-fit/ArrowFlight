#!/usr/bin/env python3
"""
Generate BenchBase procedure classes from DuckDB TPC-DS SQL queries.

Reads 01.sql..99.sql from the queries/ directory and produces Q1.java..Q99.java.
Each class extends com.oltpbenchmark.api.Procedure and executes the query
as a simple Statement (no prepared-statement parameters needed — DuckDB queries
use hardcoded substitution variables).
"""

import os
import pathlib

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
QUERIES_DIR = SCRIPT_DIR / "queries"
OUTPUT_DIR = SCRIPT_DIR / "src" / "com" / "oltpbenchmark" / "benchmarks" / "tpcds" / "procedures"


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
    # Use %%d as placeholder for num, replace after building
    class_lines = [
        "package com.oltpbenchmark.benchmarks.tpcds.procedures;",
        "",
        "import com.oltpbenchmark.api.Procedure;",
        "import java.sql.Connection;",
        "import java.sql.ResultSet;",
        "import java.sql.SQLException;",
        "import java.sql.Statement;",
        "",
        "/**",
        " * TPC-DS Query %d." % num,
        ' * SQL source: DuckDB TPC-DS extension (standard SQL, Spark-compatible).',
        " */",
        "public final class Q%d extends Procedure {" % num,
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


def main() -> None:
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

    print(f"Generated {generated} procedure classes in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
