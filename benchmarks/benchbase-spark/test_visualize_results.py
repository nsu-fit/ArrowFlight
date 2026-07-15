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
    def test_read_csv_removes_nul_bytes_from_beeline_output(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "query-q5.actual.csv"
            csv_path.write_text("name,value\ncustomer,ab\0cd\n", encoding="utf-8")

            rows = VISUALIZE_RESULTS.read_csv(csv_path)

        self.assertEqual([{"name": "customer", "value": "abcd"}], rows)


if __name__ == "__main__":
    unittest.main()
