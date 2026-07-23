#!/usr/bin/env python3
import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("build-pages-site.py")
SPEC = importlib.util.spec_from_file_location("build_pages_site", MODULE_PATH)
BUILD_PAGES_SITE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD_PAGES_SITE)


class AllQueryChartTest(unittest.TestCase):
    def test_latest_all_compare_is_rendered_on_index(self):
        query_rows = [{"query": "Q1", "avg": 12.0, "samples": 4}]
        run = {
            "kind": "compare",
            "id": "tpch-compare-all-test",
            "title": "tpch-compare-all-test",
            "benchmark": "tpch",
            "path": "flight vs direct",
            "query": "all",
            "scale": 1,
            "timestamp": "2026-07-23T10:00:00",
            "report": "benchmarks/test/compare.report.html",
            "files": "benchmarks/test",
            "flight": {
                "throughput": 1,
                "avgMs": 12,
                "report": "",
                "queryLatencies": query_rows,
            },
            "direct": {
                "throughput": 1,
                "avgMs": 10,
                "report": "",
                "queryLatencies": query_rows,
            },
            "flightNodes": 4,
        }

        page = BUILD_PAGES_SITE.build_index([run])

        self.assertIn("Latest TPC-H Q1-Q22 Average Query Execution Time", page)
        self.assertIn("average query execution time, ms", page)
        self.assertIn("q01", page)
        self.assertIn("q22", page)
        self.assertIn("Flight (ms)", page)
        self.assertIn("Direct (ms)", page)


if __name__ == "__main__":
    unittest.main()
