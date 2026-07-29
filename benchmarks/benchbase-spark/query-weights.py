#!/usr/bin/env python3
"""Build BenchBase query weights from TPC-H and TPC-DS selectors."""

import argparse


MAX_QUERY_IDS = {
    "tpch": 22,
    "tpcds": 99,
}


def parse_query_ids(selector, maximum, benchmark):
    """Expand and validate one comma-separated benchmark query selector."""
    normalized = selector.lower().replace(" ", "")
    if normalized == "all":
        return list(range(1, maximum + 1))

    query_ids = []
    for token in normalized.split(","):
        number = token.removeprefix("q")
        if not number.isdigit():
            raise ValueError(
                f"Bad {benchmark} query selector: {selector}"
            )
        query_id = int(number)
        if query_id < 1 or query_id > maximum:
            raise ValueError(
                f"{benchmark} query must be between q1 and q{maximum}: "
                f"q{query_id}"
            )
        query_ids.append(query_id)

    if not query_ids:
        raise ValueError(f"Bad {benchmark} query selector: {selector}")
    if len(query_ids) != len(set(query_ids)):
        raise ValueError(
            f"{benchmark} query selector contains duplicates: {selector}"
        )
    return query_ids


def build_query_weights(selector, maximum, benchmark):
    """Distribute one hundred weight points across selected queries."""
    query_ids = parse_query_ids(selector, maximum, benchmark)
    weights = [0] * maximum
    base, remainder = divmod(100, len(query_ids))
    for index, query_id in enumerate(query_ids):
        weights[query_id - 1] = base + (1 if index < remainder else 0)
    return weights


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Build BenchBase weights for selected benchmark queries."
    )
    parser.add_argument(
        "--benchmark",
        choices=sorted(MAX_QUERY_IDS),
        required=True,
    )
    parser.add_argument("--selector", required=True)
    return parser.parse_args()


def main():
    """Print one comma-separated BenchBase weight vector."""
    args = parse_args()
    maximum = MAX_QUERY_IDS[args.benchmark]
    benchmark = "TPC-DS" if args.benchmark == "tpcds" else "TPC-H"
    try:
        weights = build_query_weights(
            args.selector,
            maximum,
            benchmark,
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error
    print(",".join(str(weight) for weight in weights))


if __name__ == "__main__":
    main()
