#!/usr/bin/env python3
import importlib.util
import sys
import tempfile
import types
import unittest
import xml.etree.ElementTree as element_tree
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
REPEAT_WORK = load_module("repeat_work_phases", "repeat-work-phases.py")


class AllQuerySelectorTest(unittest.TestCase):
    def test_capture_selector_expands_all_queries(self):
        self.assertEqual(set(range(1, 23)), CAPTURE_RESULTS.parse_query_ids("all"))

    def test_generator_selector_expands_all_queries(self):
        self.assertEqual(list(range(1, 23)), GENERATE_DATA.parse_query_ids("ALL"))


class RepeatWorkPhasesTest(unittest.TestCase):
    def test_repeats_serial_phase_without_duplicating_transactions(self):
        config_text = """<?xml version="1.0"?>
<parameters>
  <works>
    <work>
      <serial>true</serial>
      <weights>50,50</weights>
    </work>
  </works>
  <transactiontypes>
    <transactiontype><name>Q1</name><id>1</id></transactiontype>
    <transactiontype><name>Q2</name><id>2</id></transactiontype>
  </transactiontypes>
</parameters>
"""
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "tpch.xml"
            config.write_text(config_text, encoding="utf-8")

            REPEAT_WORK.repeat_work_phases(config, 50)

            root = element_tree.parse(config).getroot()

        phases = root.findall("./works/work")
        self.assertEqual(50, len(phases))
        self.assertTrue(
            all(phase.findtext("weights") == "50,50" for phase in phases)
        )
        self.assertEqual(
            2, len(root.findall("./transactiontypes/transactiontype"))
        )

    def test_rejects_zero_repetitions(self):
        with self.assertRaisesRegex(ValueError, "positive integer"):
            REPEAT_WORK.repeat_work_phases(Path("unused.xml"), 0)


if __name__ == "__main__":
    unittest.main()
