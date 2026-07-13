#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Write SQL files for benchmark reference queries.")
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    args.results.mkdir(parents=True, exist_ok=True)

    for query in metadata.get("reference_queries", []):
        query_id = int(query["query_id"])
        sql = query.get("sql", "").strip().rstrip(";")
        if not sql:
            continue
        path = args.results / f"query-q{query_id}.sql"
        path.write_text(sql + ";\n", encoding="utf-8")
        print(path)


if __name__ == "__main__":
    main()
