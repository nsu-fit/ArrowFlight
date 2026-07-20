#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Write SQL files for benchmark reference queries.")
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--queries", default="", help="Comma-separated queries, for example q1,q6,q14")
    return parser.parse_args()


def parse_query_ids(value):
    query_ids = set()
    for token in value.lower().replace(" ", "").split(","):
        token = token.removeprefix("q")
        if token.isdigit():
            query_ids.add(int(token))
    return query_ids


def remove_inactive_query_files(results_dir, active_query_ids):
    if not active_query_ids:
        return
    pattern = re.compile(r"query-q(\d+)\.(?:sql|actual\.csv)$")
    for path in results_dir.glob("query-q*"):
        match = pattern.fullmatch(path.name)
        if match and int(match.group(1)) not in active_query_ids:
            path.unlink()


def main():
    args = parse_args()
    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    args.results.mkdir(parents=True, exist_ok=True)
    active_query_ids = parse_query_ids(args.queries)
    remove_inactive_query_files(args.results, active_query_ids)

    for query in metadata.get("reference_queries", []):
        query_id = int(query["query_id"])
        if active_query_ids and query_id not in active_query_ids:
            continue
        sql = query.get("sql", "").strip().rstrip(";")
        if not sql:
            continue
        path = args.results / f"query-q{query_id}.sql"
        path.write_text(sql + ";\n", encoding="utf-8")
        print(path)


if __name__ == "__main__":
    main()
