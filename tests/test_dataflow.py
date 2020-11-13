import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dagrivane.dataflow import compile_plan, load_plan, run_plan, publish_bundle
from dagrivane.engine import run, validate


def plan(destination="release/orders.json"):
    return {
        "version": 1,
        "sources": [
            {
                "id": "orders",
                "path": "orders.csv",
                "contract": {
                    "name": "orders",
                    "version": 1,
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "region", "type": "string"},
                        {"name": "quantity", "type": "integer", "min": 1},
                        {"name": "price", "type": "decimal", "min": "0"},
                    ],
                },
            }
        ],
        "steps": [
            {
                "id": "priced",
                "input": "orders",
                "op": "calculate",
                "column": {"name": "total", "type": "decimal"},
                "expression": {
                    "op": "multiply",
                    "args": [{"column": "quantity"}, {"column": "price"}],
                },
            },
            {
                "id": "summary",
                "input": "priced",
                "op": "aggregate",
                "group_by": ["region"],
                "metrics": [
                    {"name": "revenue", "op": "sum", "column": "total"},
                    {"name": "orders", "op": "count"},
                ],
            },
        ],
        "output": "summary",
        "gates": [
            {"id": "regions", "kind": "unique", "columns": ["region"]},
            {
                "id": "revenue",
                "kind": "sum",
                "columns": ["revenue"],
                "min": "0.70",
                "max": "0.70",
            },
        ],
        "destination": destination,
    }


def input_file(folder):
    path = Path(folder) / "orders.csv"
    path.write_text("id,region,quantity,price\n1,EU,3,0.10\n2,EU,2,0.20\n")
    return path


class DataflowTests(unittest.TestCase):
    def test_real_csv_to_quality_gated_artifact(self):
        with tempfile.TemporaryDirectory() as folder:
            input_file(folder)
            definition = plan()
            report = run_plan(definition, folder)
            self.assertTrue(report["passed"])
            artifact = json.loads(
                (Path(folder) / definition["destination"]).read_text()
            )
            self.assertEqual(
                artifact["rows"], [{"region": "EU", "revenue": "0.70", "orders": 2}]
            )
            self.assertEqual(artifact["report"]["plan_sha256"], report["plan_sha256"])
            self.assertEqual(
                artifact["report"]["output"]["source_sha256"],
                [
                    hashlib.sha256(
                        (Path(folder) / "orders.csv").read_bytes()
                    ).hexdigest()
                ],
            )

    def test_quality_failure_never_publishes(self):
        with tempfile.TemporaryDirectory() as folder:
            input_file(folder)
            definition = plan()
            definition["gates"][1]["min"] = "10"
            definition["gates"][1].pop("max")
            report = run_plan(definition, folder)
            self.assertFalse(report["passed"])
            self.assertFalse((Path(folder) / definition["destination"]).exists())
            self.assertNotIn("artifact", report)

    def test_existing_artifact_is_preserved(self):
        with tempfile.TemporaryDirectory() as folder:
            input_file(folder)
            target = Path(folder) / "result.json"
            target.write_text("previous release")
            with self.assertRaises(FileExistsError):
                run_plan(plan("result.json"), folder)
            self.assertEqual(target.read_text(), "previous release")
            self.assertFalse(list(Path(folder).glob(".dagrivane-*")))

    def test_concurrent_publish_has_one_winner_and_no_torn_json(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "artifact.json"

            def publish(value):
                try:
                    publish_bundle(target, {"value": value})
                    return True
                except FileExistsError:
                    return False

            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(publish, [1, 2]))
            self.assertEqual(sum(outcomes), 1)
            self.assertIn(json.loads(target.read_text())["value"], [1, 2])
            self.assertFalse(list(Path(folder).glob(".dagrivane-*")))

    def test_pinned_source_detects_change(self):
        with tempfile.TemporaryDirectory() as folder:
            source = input_file(folder)
            definition = plan()
            definition["sources"][0]["sha256"] = hashlib.sha256(
                source.read_bytes()
            ).hexdigest()
            source.write_text(source.read_text().replace("0.10", "0.11"))
            with self.assertRaisesRegex(ValueError, "pinned hash"):
                run_plan(definition, folder)
            self.assertFalse((Path(folder) / definition["destination"]).exists())

    def test_whole_plan_is_compiled_before_reading_sources(self):
        definition = plan()
        definition["steps"][1]["metrics"][0]["column"] = "missing"
        with self.assertRaisesRegex(ValueError, "unknown column"):
            run_plan(definition, "/nonexistent/path")

    def test_compiled_plan_is_independent_of_original_mutation(self):
        definition = plan()
        compiled = compile_plan(definition)
        definition["sources"][0]["path"] = "missing.csv"
        with tempfile.TemporaryDirectory() as folder:
            input_file(folder)
            report, _ = compiled.execute(folder)
            self.assertTrue(report["passed"])

