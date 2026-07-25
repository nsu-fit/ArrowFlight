#!/usr/bin/env python3
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("visualize-results.py")
SPEC = importlib.util.spec_from_file_location("visualize_results", MODULE_PATH)
VISUALIZE_RESULTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VISUALIZE_RESULTS)


class ReadCsvTest(unittest.TestCase):
    def test_read_csv_skips_blank_and_nul_only_beeline_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "query-q5.actual.csv"
            csv_path.write_text(
                "\n"
                "\0\0\n"
                " \0 \n"
                "name,value\n"
                "customer,ab\0cd\n"
                "\0\n"
                "\n",
                encoding="utf-8",
            )

            rows = VISUALIZE_RESULTS.read_csv(csv_path)

        self.assertEqual([{"name": "customer", "value": "abcd"}], rows)

    def test_read_csv_preserves_commas_between_paired_nul_quotes(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "query-q10.actual.csv"
            csv_path.write_text(
                "name,comment\n"
                "customer,\0regular, pending packages\0\n",
                encoding="utf-8",
            )

            rows = VISUALIZE_RESULTS.read_csv(csv_path)

        self.assertEqual(
            [{"name": "customer", "comment": "regular, pending packages"}],
            rows,
        )


class RowsEqualTest(unittest.TestCase):
    def test_matches_case_insensitive_columns_unordered_rows_and_float_formatting(self):
        expected = [
            {"CUSTOMER": "alice", "Revenue": "25.537587116854997"},
            {"CUSTOMER": "bob", "Revenue": "10.0"},
        ]
        actual = [
            {"customer": "bob", "revenue": "10.000000"},
            {"customer": "alice", "revenue": "25.537587"},
        ]

        self.assertTrue(VISUALIZE_RESULTS.rows_equal(expected, actual))

    def test_preserves_duplicate_row_counts(self):
        expected = [{"name": "alice"}, {"name": "alice"}, {"name": "bob"}]
        reordered = [{"NAME": "bob"}, {"NAME": "alice"}, {"NAME": "alice"}]
        missing_duplicate = [{"NAME": "bob"}, {"NAME": "alice"}, {"NAME": "bob"}]

        self.assertTrue(VISUALIZE_RESULTS.rows_equal(expected, reordered))
        self.assertFalse(VISUALIZE_RESULTS.rows_equal(expected, missing_duplicate))

    def test_numeric_tolerance_is_absolute_and_inclusive(self):
        expected = [{"value": "1.000000"}]

        self.assertTrue(
            VISUALIZE_RESULTS.rows_equal(expected, [{"VALUE": "1.000001"}])
        )
        self.assertFalse(
            VISUALIZE_RESULTS.rows_equal(expected, [{"VALUE": "1.0000011"}])
        )

    def test_non_numeric_values_require_exact_match(self):
        self.assertFalse(
            VISUALIZE_RESULTS.rows_equal(
                [{"status": "Complete"}],
                [{"STATUS": "complete"}],
            )
        )

    def test_ordered_comparison_rejects_reordered_rows(self):
        expected = [{"name": "alice"}, {"name": "bob"}]
        reordered = [{"NAME": "bob"}, {"NAME": "alice"}]

        self.assertFalse(
            VISUALIZE_RESULTS.rows_equal(expected, reordered, ordered=True)
        )
        self.assertTrue(VISUALIZE_RESULTS.rows_equal(expected, reordered))

    def test_ordered_comparison_keeps_column_and_numeric_normalization(self):
        expected = [{"CUSTOMER": "alice", "value": "1.000000"}]
        actual = [{"customer": "alice", "VALUE": "1.000001"}]

        self.assertTrue(
            VISUALIZE_RESULTS.rows_equal(expected, actual, ordered=True)
        )


class QueryReferenceOrderingTest(unittest.TestCase):
    def test_order_by_uses_sequence_while_other_queries_use_multiset(self):
        metadata = {
            "reference_queries": [
                {
                    "query_id": 1,
                    "name": "Q1",
                    "sql": "SELECT name FROM customer ORDER\n  BY name",
                    "expected_rows": [{"name": "alice"}, {"name": "bob"}],
                },
                {
                    "query_id": 2,
                    "name": "Q2",
                    "sql": "SELECT name FROM customer",
                    "expected_rows": [{"name": "alice"}, {"name": "bob"}],
                },
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            for query_id in (1, 2):
                (run_dir / f"query-q{query_id}.actual.csv").write_text(
                    "name\nbob\nalice\n",
                    encoding="utf-8",
                )

            report = VISUALIZE_RESULTS.query_reference_section(run_dir, metadata)

        self.assertIn('Q1: <span class="bad">DIFF</span>', report)
        self.assertIn('Q2: <span class="ok">MATCH</span>', report)
        self.assertIn("Compared as ordered sequence.", report)
        self.assertIn("Compared as unordered multiset.", report)


class PerQueryLatencyTest(unittest.TestCase):
    def test_aggregates_measured_raw_latency_by_query(self):
        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory)
            raw_path = results / "tpch_test.raw.csv"
            raw_path.write_text(
                "Transaction Name,Latency (microseconds)\n"
                "Q2,3000\n"
                "Q1,1000\n"
                "Q1,5000\n",
                encoding="utf-8",
            )

            rows = VISUALIZE_RESULTS.per_query_latency_rows(results, "tpch_test")

        self.assertEqual(
            [
                {"query": "Q1", "avg": 3.0, "samples": 2},
                {"query": "Q2", "avg": 3.0, "samples": 1},
            ],
            rows,
        )

    def test_grouped_chart_contains_query_labels_and_both_series(self):
        chart = VISUALIZE_RESULTS.svg_query_latency_chart(
            [{"query": "Q1", "avg": 10.0, "samples": 2}],
            [{"query": "Q22", "avg": 20.0, "samples": 3}],
            range(1, 23),
        )

        self.assertIn("q01", chart)
        self.assertIn("q22", chart)
        self.assertIn("Flight (ms)", chart)
        self.assertIn("Direct (ms)", chart)
        self.assertIn("Average Query Execution Time", chart)
        self.assertIn("average query execution time, ms", chart)


class StandardizedLatencyTest(unittest.TestCase):
    def test_read_config_preserves_positive_transaction_weights(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "tpch.config.xml"
            config_path.write_text(
                "<configuration><works><work>"
                "<weights>75,25,0</weights>"
                "</work></works></configuration>",
                encoding="utf-8",
            )

            config = VISUALIZE_RESULTS.read_config(config_path)

        self.assertEqual({"Q1": 75.0, "Q2": 25.0}, config["weights"])

    def test_uses_configured_mix_instead_of_measured_sample_counts(self):
        weights = VISUALIZE_RESULTS.normalized_query_weights(
            {"weights": {"Q1": 75, "Q2": 25}}
        )
        rows = [
            {"query": "Q1", "avg": 10.0, "samples": 1},
            {"query": "Q2", "avg": 30.0, "samples": 99},
        ]

        latency = VISUALIZE_RESULTS.standardized_weighted_latency(rows, weights)

        self.assertEqual(15.0, latency)

    def test_returns_none_when_configured_query_has_no_samples(self):
        weights = VISUALIZE_RESULTS.normalized_query_weights(
            {"weights": {"Q1": 50, "Q2": 50}}
        )

        latency = VISUALIZE_RESULTS.standardized_weighted_latency(
            [{"query": "Q1", "avg": 10.0, "samples": 1}],
            weights,
        )

        self.assertIsNone(latency)

    def test_compare_cards_displays_standardized_delta(self):
        def run(q1, q2):
            return {
                "summary": {
                    VISUALIZE_RESULTS.THROUGHPUT: 1,
                    "Latency Distribution": {},
                },
                "config": {"weights": {"Q1": 75, "Q2": 25}},
                "query_latency_rows": [
                    {"query": "Q1", "avg": q1, "samples": 1},
                    {"query": "Q2", "avg": q2, "samples": 99},
                ],
            }

        report = VISUALIZE_RESULTS.compare_cards(run(10, 30), run(20, 20))

        self.assertIn("Configured-mix avg latency ms", report)
        self.assertIn("-5.000 (-25.0%)", report)
        self.assertIn("sample counts do not affect it", report)


class ComparisonOrderNoticeTest(unittest.TestCase):
    def test_displays_execution_order_from_metadata_and_cache_warning(self):
        notice = VISUALIZE_RESULTS.comparison_order_notice(
            {"execution_order": ["flight", "direct"]}
        )

        self.assertIn("Recorded execution order: flight → direct.", notice)
        self.assertIn("warmed Spark, HDFS, and operating-system caches", notice)

    def test_warns_when_execution_order_is_missing(self):
        notice = VISUALIZE_RESULTS.comparison_order_notice({})

        self.assertIn("Execution order was not recorded.", notice)


if __name__ == "__main__":
    unittest.main()
