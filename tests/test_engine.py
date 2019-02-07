import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from dagrivane.engine import run, validate


class EngineTests(unittest.TestCase):
    def test_report_retains_dependency_order(self):
        spec = {
            "tasks": [
                {
                    "id": "a-child",
                    "needs": ["z-parent"],
                    "command": [sys.executable, "-c", "pass"],
                },
                {"id": "z-parent", "command": [sys.executable, "-c", "pass"]},
            ]
        }
        with tempfile.TemporaryDirectory() as folder:
            result = run(spec, Path(folder) / "state.db", "ordered")
        self.assertEqual([task["id"] for task in result], ["z-parent", "a-child"])

