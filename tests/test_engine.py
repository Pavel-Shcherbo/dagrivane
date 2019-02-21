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

    def test_failed_dependency_does_not_block_independent_branch(self):
        spec = {
            "tasks": [
                {
                    "id": "bad",
                    "command": [sys.executable, "-c", "raise SystemExit(1)"],
                    "retries": 1,
                },
                {
                    "id": "child",
                    "needs": ["bad"],
                    "command": [sys.executable, "-c", "print('must not run')"],
                },
                {"id": "other", "command": [sys.executable, "-c", "print('ok')"]},
            ]
        }
        with tempfile.TemporaryDirectory() as folder:
            result = {x["id"]: x for x in run(spec, Path(folder) / "state.db", "test")}
        self.assertEqual(result["bad"]["attempts"], 2)
        self.assertEqual(result["child"]["status"], "blocked")
        self.assertEqual(result["other"]["status"], "succeeded")

    def test_resume_does_not_repeat_successful_side_effect(self):
        with tempfile.TemporaryDirectory() as folder:
            spec = {
                "tasks": [
                    {
                        "id": "write",
                        "command": [
                            sys.executable,
                            "-c",
                            "from pathlib import Path; p=Path('count'); p.write_text(p.read_text()+'x' if p.exists() else 'x')",
                        ],
                    }
                ]
            }
            state = Path(folder) / "state.db"
            run(spec, state, "one", folder)
            run(spec, state, "one", folder)
            self.assertEqual((Path(folder) / "count").read_text(), "x")
            with self.assertRaises(ValueError):
                run(
                    {"tasks": [{"id": "other", "command": ["echo", "different"]}]},
                    state,
                    "one",
                    folder,
                )
            with sqlite3.connect(state) as db:
                db.execute("UPDATE tasks SET status='running'")
            with self.assertRaisesRegex(ValueError, "unknown outcome"):
                run(spec, state, "one", folder)

    def test_graph_validation(self):
        for tasks in [
            [{"id": "a", "command": ["echo"], "needs": ["missing"]}],
            [{"id": "a", "command": ["echo"], "needs": ["a"]}],
            [{"id": "a", "command": "echo unsafe"}],
        ]:
            with self.assertRaises(ValueError):
                validate({"tasks": tasks})

    def test_timeout_is_a_failed_attempt_and_blocks_children(self):
        spec = {
            "tasks": [
                {
                    "id": "slow",
                    "command": [sys.executable, "-c", "import time; time.sleep(10)"],
                    "timeout": 0.1,
                },
                {
                    "id": "after",
                    "needs": ["slow"],
                    "command": [sys.executable, "-c", "pass"],
                },
            ]
        }
        with tempfile.TemporaryDirectory() as folder:
            result = run(spec, Path(folder) / "state.db", "timeout")
        self.assertEqual([item["status"] for item in result], ["failed", "blocked"])
        self.assertIn("exceeded", result[0]["output"])
