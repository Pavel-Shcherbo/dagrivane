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

