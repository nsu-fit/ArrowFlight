#!/usr/bin/env python3
"""Tests for legacy/current Direct-normalized benchmark comparison."""

import importlib.util
import unittest
from copy import deepcopy
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "compare_implementations",
    SCRIPT_DIR / "compare-implementations.py",
)
COMPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPARE)


def distribution(value):
    return {"median": value}


def artifact(run_id, image_id, flight, direct, ratio):
    """Build a minimal complete paired-comparison artifact."""
    query = "q1"
    return {
        "artifact_type": "paired-comparison",
        "run": {
            "id": run_id,
            "benchmark": "tpch",
            "source": {
                "git_sha": "a" * 40,
                "dirty": False,
                "dirty_files": [],
            },
            "workload": {"query_set": "q1", "scale_factor": 1.0},
            "policy": {
                "warmup_seconds": 20,
                "measurement_seconds": 60,
                "cache_policy": "warm-cache",
                "repetitions": 1,
                "paired_observations": 3,
                "minimum_publishable_pairs": 3,
                "terminals": 1,
                "rate": "unlimited",
                "starting_engine_order": ["flight", "direct"],
                "engine_order_schedule": [
                    {
                        "observation_index": 1,
                        "engine_order": ["flight", "direct"],
                    },
                    {
                        "observation_index": 2,
                        "engine_order": ["direct", "flight"],
                    },
                    {
                        "observation_index": 3,
                        "engine_order": ["flight", "direct"],
                    },
                ],
            },
            "configuration": {
                "spark": {
                    "master_url": "spark://spark-master:7077",
                    "sql_ansi_enabled": "true",
                    "direct_parquet_partitions": 3,
                    "thrift_server": "spark-thrift-server:10000",
                },
                "hadoop": {
                    "data_dir": "hdfs://hdfs-namenode:8020/bench",
                    "benchmark_path": "/bench",
                    "block_size_bytes": 1073741824,
                    "replication": 1,
                },
                "jvm": {"java_opts": "-Xmx2g"},
                "flight": {
                    "batch_size": 65536,
                    "duckdb_threads": "1",
                    "timing_log_level": "unsupported-by-legacy",
                },
            },
            "topology": {
                "cluster_nodes": 3,
                "flight_hosts": [
                    "flight-server-1",
                    "flight-server-2",
                    "flight-server-3",
                ],
                "flight_servers": [
                    "flight-server-1:32010",
                    "flight-server-2:32010",
                    "flight-server-3:32010",
                ],
                "host_resources": "16 logical CPU, 16 GiB RAM",
            },
            "runtime_dependencies": {
                "maven": {"spark": "3.5.1"},
                "arrowflight": {
                    "image_ref": f"arrowflight-{run_id}:latest",
                    "image_id": image_id,
                },
                "benchbase": {"image_id": "sha256:" + "b" * 64},
                "generator": {"image_id": "sha256:" + "c" * 64},
            },
            "inputs": {
                "benchmark_config": {
                    "path": "benchmarks/benchbase-spark/config/tpch.xml",
                    "sha256": "d" * 64,
                }
            },
        },
        "dataset": {
            "manifest": {
                "dataset": "tpch",
                "schema": "tpch",
                "scale_factor": 1.0,
                "cluster_nodes": 3,
                "flight_data": [
                    {
                        "server_index": 1,
                        "bytes": 42,
                        "tables": [
                            {
                                "table": "lineitem",
                                "bytes": 42,
                                "file_details": [
                                    {
                                        "relative_path": "tpch/lineitem/part-1.parquet",
                                        "bytes": 42,
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        },
        "queries": [
            {
                "logical_query_id": query,
                "digest": "e" * 64,
            }
        ],
        "comparison": {
            "correctness": {"status": "pass"},
            "validity": {"valid": True, "reasons": []},
        },
        "aggregate_summary": {
            "engines": {
                "flight": {
                    "queries": {
                        query: {
                            "observation_median_latency_microseconds": distribution(
                                flight
                            )
                        }
                    }
                },
                "direct": {
                    "queries": {
                        query: {
                            "observation_median_latency_microseconds": distribution(
                                direct
                            )
                        }
                    }
                },
            },
            "paired": {
                "complete_pairs": 3,
                "queries": {
                    query: {
                        "flight_to_direct_median_latency_ratio": distribution(
                            ratio
                        )
                    }
                },
            },
        },
    }


class CompareImplementationsTest(unittest.TestCase):
    """Validates fairness checks and ratio-of-ratios output."""

    def setUp(self):
        """Create compatible legacy and current artifacts."""
        self.legacy = artifact(
            "legacy", "sha256:" + "1" * 64, 4000000, 2000000, 2.0
        )
        self.current = artifact(
            "current", "sha256:" + "2" * 64, 1500000, 1500000, 1.0
        )

    def test_reports_raw_and_direct_normalized_speedup(self):
        """Current speedup is reported both raw and against Direct controls."""
        result = COMPARE.compare_artifacts(self.legacy, self.current)

        query = result["queries"][0]
        self.assertAlmostEqual(4 / 1.5, query["raw_current_speedup"])
        self.assertEqual(2.0, query["direct_normalized_current_speedup"])
        self.assertEqual(
            2.0,
            result["summary"]["median_direct_normalized_current_speedup"],
        )

    def test_rejects_mismatched_workload(self):
        """Scale-factor mismatches cannot produce a comparison."""
        mismatched = deepcopy(self.current)
        mismatched["run"]["workload"]["scale_factor"] = 10.0

        with self.assertRaisesRegex(COMPARE.ComparisonError, "workload"):
            COMPARE.compare_artifacts(self.legacy, mismatched)

    def test_rejects_mismatched_dataset_layout(self):
        """Different generated Parquet layouts cannot produce a comparison."""
        mismatched = deepcopy(self.current)
        mismatched["dataset"]["manifest"]["flight_data"][0]["bytes"] = 43

        with self.assertRaisesRegex(COMPARE.ComparisonError, "Parquet"):
            COMPARE.compare_artifacts(self.legacy, mismatched)

    def test_rejects_incomplete_pair_set(self):
        """Runs below the required complete-pair count are not comparable."""
        incomplete = deepcopy(self.current)
        incomplete["aggregate_summary"]["paired"]["complete_pairs"] = 2

        with self.assertRaisesRegex(COMPARE.ComparisonError, "at least 3"):
            COMPARE.compare_artifacts(self.legacy, incomplete)

    def test_rejects_mismatched_query_digest(self):
        """Different logical SQL cannot produce an implementation comparison."""
        mismatched = deepcopy(self.current)
        mismatched["queries"][0]["digest"] = "f" * 64

        with self.assertRaisesRegex(COMPARE.ComparisonError, "SQL digests"):
            COMPARE.compare_artifacts(self.legacy, mismatched)

    def test_warns_about_direct_control_drift(self):
        """Large Direct movement remains visible in the comparison report."""
        result = COMPARE.compare_artifacts(self.legacy, self.current)

        self.assertTrue(
            any("Direct control drift" in warning for warning in result["warnings"])
        )


if __name__ == "__main__":
    unittest.main()
