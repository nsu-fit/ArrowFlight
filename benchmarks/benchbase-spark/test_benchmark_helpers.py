#!/usr/bin/env python3
import importlib.util
import sys
import types
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CAPTURE_RESULTS = load_module("capture_query_results", "capture-query-results.py")
sys.modules.setdefault("duckdb", types.ModuleType("duckdb"))
GENERATE_DATA = load_module("generate_duckdb_data", "generate-duckdb-data.py")


class AllQuerySelectorTest(unittest.TestCase):
    def test_capture_selector_expands_all_queries(self):
        self.assertEqual(set(range(1, 23)), CAPTURE_RESULTS.parse_query_ids("all"))

    def test_generator_selector_expands_all_queries(self):
        self.assertEqual(list(range(1, 23)), GENERATE_DATA.parse_query_ids("ALL"))


if __name__ == "__main__":
    unittest.main()
