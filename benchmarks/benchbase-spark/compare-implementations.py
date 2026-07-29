#!/usr/bin/env python3
"""Compare legacy and current Flight runs through their paired Direct controls."""

import argparse
import hashlib
import json
import statistics
from pathlib import Path


class ComparisonError(ValueError):
    """Reports incompatible or incomplete implementation benchmark runs."""


def read_artifact(path):
    """Read a benchmark result from a file or run directory."""
    candidate = path / "benchmark-result.json" if path.is_dir() else path
    if not candidate.is_file():
        raise ComparisonError(f"benchmark result not found: {candidate}")
    with candidate.open(encoding="utf-8") as source:
        artifact = json.load(source)
    if artifact.get("artifact_type") != "paired-comparison":
        raise ComparisonError(f"not a paired comparison artifact: {candidate}")
    return artifact


def nested(value, dotted_path):
    """Read a required value through a dotted object path."""
    current = value
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise ComparisonError(f"missing required field: {dotted_path}")
        current = current[part]
    return current


def dataset_signature(artifact):
    """Build a stable size/layout signature for generated Parquet shards."""
    manifest = nested(artifact, "dataset.manifest")
    nodes = []
    for node in manifest.get("flight_data", []):
        tables = []
        for table in node.get("tables", []):
            files = [
                {
                    "relative_path": file.get("relative_path"),
                    "bytes": file.get("bytes"),
                }
                for file in table.get("file_details", [])
            ]
            tables.append(
                {
                    "table": table.get("table"),
                    "bytes": table.get("bytes"),
                    "files": sorted(
                        files,
                        key=lambda item: (
                            str(item["relative_path"]),
                            item["bytes"] or 0,
                        ),
                    ),
                }
            )
        nodes.append(
            {
                "server_index": node.get("server_index"),
                "bytes": node.get("bytes"),
                "tables": sorted(tables, key=lambda item: str(item["table"])),
            }
        )
    payload = {
        "dataset": manifest.get("dataset"),
        "schema": manifest.get("schema"),
        "scale_factor": manifest.get("scale_factor"),
        "cluster_nodes": manifest.get("cluster_nodes"),
        "nodes": sorted(nodes, key=lambda item: item["server_index"] or 0),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def require_equal(legacy, current, paths):
    """Reject runs whose fairness-critical fields differ."""
    differences = []
    for path in paths:
        old_value = nested(legacy, path)
        new_value = nested(current, path)
        if old_value != new_value:
            differences.append(f"{path}: legacy={old_value!r}, current={new_value!r}")
    if differences:
        raise ComparisonError(
            "benchmark contracts differ:\n  " + "\n  ".join(differences)
        )


def validate_run(artifact, label):
    """Reject one failed, incorrect, or incomplete paired run."""
    if nested(artifact, "comparison.correctness.status") != "pass":
        raise ComparisonError(f"{label} correctness did not pass")
    if not nested(artifact, "comparison.validity.valid"):
        reasons = nested(artifact, "comparison.validity.reasons")
        raise ComparisonError(f"{label} comparison is invalid: {reasons}")
    complete_pairs = nested(
        artifact, "aggregate_summary.paired.complete_pairs"
    )
    minimum_pairs = nested(
        artifact, "run.policy.minimum_publishable_pairs"
    )
    if complete_pairs < minimum_pairs:
        raise ComparisonError(
            f"{label} has {complete_pairs} complete Flight/Direct pairs; "
            f"at least {minimum_pairs} are required"
        )


def query_digests(artifact):
    """Map logical query IDs to the SQL digests recorded by the harness."""
    return {
        query["logical_query_id"]: query["digest"]
        for query in nested(artifact, "queries")
    }


def distribution_median(artifact, path):
    """Read a non-zero median from an aggregate distribution."""
    value = nested(artifact, path)
    median = value.get("median") if isinstance(value, dict) else None
    if median is None or median <= 0:
        raise ComparisonError(f"missing positive median: {path}")
    return float(median)


def query_metrics(artifact, query_id):
    """Read Flight, Direct, and paired medians for one query."""
    prefix = "aggregate_summary"
    flight = distribution_median(
        artifact,
        f"{prefix}.engines.flight.queries.{query_id}"
        ".observation_median_latency_microseconds",
    )
    direct = distribution_median(
        artifact,
        f"{prefix}.engines.direct.queries.{query_id}"
        ".observation_median_latency_microseconds",
    )
    ratio = distribution_median(
        artifact,
        f"{prefix}.paired.queries.{query_id}"
        ".flight_to_direct_median_latency_ratio",
    )
    return {"flight_us": flight, "direct_us": direct, "flight_to_direct": ratio}


def runtime_warning(legacy, current, path, label):
    """Describe a non-fatal implementation-specific runtime difference."""
    old_value = nested(legacy, path)
    new_value = nested(current, path)
    if old_value == new_value:
        return None
    return f"{label}: legacy={old_value!r}, current={new_value!r}"


def compare_artifacts(legacy, current):
    """Build an honest cross-run comparison with Direct normalization."""
    validate_run(legacy, "legacy")
    validate_run(current, "current")
    require_equal(
        legacy,
        current,
        [
            "run.benchmark",
            "run.workload",
            "run.policy.warmup_seconds",
            "run.policy.measurement_seconds",
            "run.policy.cache_policy",
            "run.policy.repetitions",
            "run.policy.paired_observations",
            "run.policy.minimum_publishable_pairs",
            "run.policy.terminals",
            "run.policy.rate",
            "run.policy.starting_engine_order",
            "run.policy.engine_order_schedule",
            "run.configuration.spark",
            "run.configuration.hadoop",
            "run.configuration.jvm",
            "run.topology",
            "run.inputs",
        ],
    )

    legacy_digests = query_digests(legacy)
    current_digests = query_digests(current)
    if legacy_digests != current_digests:
        raise ComparisonError(
            "logical query SQL digests differ: "
            f"legacy={legacy_digests}, current={current_digests}"
        )

    legacy_signature = dataset_signature(legacy)
    current_signature = dataset_signature(current)
    if legacy_signature != current_signature:
        raise ComparisonError(
            "generated Parquet shard layouts differ; runs are not comparable"
        )

    legacy_queries = set(
        nested(legacy, "aggregate_summary.paired.queries")
    )
    current_queries = set(
        nested(current, "aggregate_summary.paired.queries")
    )
    if legacy_queries != current_queries or not legacy_queries:
        raise ComparisonError(
            "measured query sets differ or are empty: "
            f"legacy={sorted(legacy_queries)}, current={sorted(current_queries)}"
        )

    warnings = []
    for path, label in [
        (
            "run.configuration.flight.batch_size",
            "effective Flight batch size differs",
        ),
        (
            "run.configuration.flight.duckdb_threads",
            "effective DuckDB threads differ",
        ),
        (
            "run.configuration.flight.timing_log_level",
            "Flight timing logger support differs",
        ),
        (
            "run.runtime_dependencies.maven",
            "application dependency versions differ",
        ),
    ]:
        warning = runtime_warning(legacy, current, path, label)
        if warning:
            warnings.append(warning)

    legacy_generator = nested(
        legacy, "run.runtime_dependencies.generator.image_id"
    )
    current_generator = nested(
        current, "run.runtime_dependencies.generator.image_id"
    )
    if legacy_generator != current_generator:
        raise ComparisonError(
            "benchmark generator image IDs differ: "
            f"legacy={legacy_generator}, current={current_generator}"
        )
    if not str(legacy_generator).startswith("sha256:"):
        raise ComparisonError(
            f"benchmark generator image ID is unresolved: {legacy_generator}"
        )

    legacy_benchbase = nested(
        legacy, "run.runtime_dependencies.benchbase.image_id"
    )
    current_benchbase = nested(
        current, "run.runtime_dependencies.benchbase.image_id"
    )
    if legacy_benchbase != current_benchbase:
        raise ComparisonError(
            "BenchBase image IDs differ: "
            f"legacy={legacy_benchbase}, current={current_benchbase}"
        )
    if not str(legacy_benchbase).startswith("sha256:"):
        raise ComparisonError(
            f"BenchBase image ID is unresolved: {legacy_benchbase}"
        )

    legacy_image = nested(
        legacy, "run.runtime_dependencies.arrowflight.image_id"
    )
    current_image = nested(
        current, "run.runtime_dependencies.arrowflight.image_id"
    )
    if not str(legacy_image).startswith("sha256:") or not str(
        current_image
    ).startswith("sha256:"):
        raise ComparisonError(
            "legacy/current ArrowFlight image IDs must both be resolved"
        )
    if legacy_image == current_image:
        raise ComparisonError(
            "legacy and current resolve to the same ArrowFlight image ID"
        )

    rows = []
    for query_id in sorted(
        legacy_queries,
        key=lambda value: (
            (0, int(value[1:])) if value[1:].isdigit() else (1, value)
        ),
    ):
        old = query_metrics(legacy, query_id)
        new = query_metrics(current, query_id)
        direct_drift = max(old["direct_us"], new["direct_us"]) / min(
            old["direct_us"], new["direct_us"]
        )
        if direct_drift > 1.25:
            warnings.append(
                f"{query_id} Direct control drift is {direct_drift:.3f}x (>1.25x)"
            )
        rows.append(
            {
                "query": query_id,
                "legacy_flight_ms": old["flight_us"] / 1000,
                "current_flight_ms": new["flight_us"] / 1000,
                "raw_current_speedup": old["flight_us"] / new["flight_us"],
                "legacy_direct_ms": old["direct_us"] / 1000,
                "current_direct_ms": new["direct_us"] / 1000,
                "direct_control_drift": direct_drift,
                "legacy_flight_to_direct": old["flight_to_direct"],
                "current_flight_to_direct": new["flight_to_direct"],
                "direct_normalized_current_speedup": (
                    old["flight_to_direct"] / new["flight_to_direct"]
                ),
            }
        )

    return {
        "schema_version": "1.0.0",
        "method": "paired-direct-ratio-of-ratios",
        "legacy": {
            "run_id": nested(legacy, "run.id"),
            "source": nested(legacy, "run.source"),
            "arrowflight_image": nested(
                legacy, "run.runtime_dependencies.arrowflight"
            ),
        },
        "current": {
            "run_id": nested(current, "run.id"),
            "source": nested(current, "run.source"),
            "arrowflight_image": nested(
                current, "run.runtime_dependencies.arrowflight"
            ),
        },
        "matched_contract": {
            "workload": nested(legacy, "run.workload"),
            "policy": nested(legacy, "run.policy"),
            "topology": nested(legacy, "run.topology"),
            "dataset_layout_sha256": legacy_signature,
            "generator_image_id": legacy_generator,
            "benchbase_image_id": legacy_benchbase,
        },
        "warnings": warnings,
        "queries": rows,
        "summary": {
            "median_raw_current_speedup": statistics.median(
                row["raw_current_speedup"] for row in rows
            ),
            "median_direct_normalized_current_speedup": statistics.median(
                row["direct_normalized_current_speedup"] for row in rows
            ),
        },
    }


def markdown_report(comparison):
    """Render the implementation comparison as a compact Markdown report."""
    lines = [
        "# Legacy vs current ArrowFlight",
        "",
        f"- Legacy run: `{comparison['legacy']['run_id']}`",
        f"- Current run: `{comparison['current']['run_id']}`",
        "- Primary cross-run metric: `(legacy Flight / legacy Direct) / "
        "(current Flight / current Direct)`.",
        "- Speedup above 1.0 means the current implementation is faster.",
        "",
        "| Query | Legacy Flight, ms | Current Flight, ms | Raw speedup | "
        "Legacy F/D | Current F/D | Direct-normalized speedup | Direct drift |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in comparison["queries"]:
        lines.append(
            f"| {row['query'].upper()} | {row['legacy_flight_ms']:.3f} | "
            f"{row['current_flight_ms']:.3f} | {row['raw_current_speedup']:.3f}x | "
            f"{row['legacy_flight_to_direct']:.3f}x | "
            f"{row['current_flight_to_direct']:.3f}x | "
            f"{row['direct_normalized_current_speedup']:.3f}x | "
            f"{row['direct_control_drift']:.3f}x |"
        )
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- Median raw current speedup: "
            f"`{comparison['summary']['median_raw_current_speedup']:.3f}x`",
            f"- Median Direct-normalized current speedup: "
            f"`{comparison['summary']['median_direct_normalized_current_speedup']:.3f}x`",
        ]
    )
    if comparison["warnings"]:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in comparison["warnings"])
    return "\n".join(lines) + "\n"


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare legacy/current paired BenchBase result artifacts."
    )
    parser.add_argument("--legacy", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def main():
    """Write JSON and Markdown implementation comparison reports."""
    args = parse_args()
    comparison = compare_artifacts(
        read_artifact(args.legacy),
        read_artifact(args.current),
    )
    args.out.mkdir(parents=True, exist_ok=True)
    json_path = args.out / "implementation-comparison.json"
    markdown_path = args.out / "implementation-comparison.md"
    json_path.write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(markdown_report(comparison), encoding="utf-8")
    print(markdown_path)
    print(json_path)


if __name__ == "__main__":
    main()
