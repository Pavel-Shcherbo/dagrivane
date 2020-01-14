import concurrent.futures
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from dagrivane.control import ControlStore
from dagrivane.worker import Worker


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = ControlStore(self.root / "control.sqlite")
        self.workspace = self.store.create_workspace("Local")["id"]
        self.worker = Worker(
            self.store, self.root / "tasks", "test-worker", lease_seconds=5
        )

    def submit(self, tasks):
        workflow = self.store.register_workflow(
            self.workspace, "Example", {"tasks": tasks}, self.root
        )
        return self.store.enqueue(
            self.workspace, workflow["id"], workflow["version"], "test"
        )

    def test_worker_executes_a_real_graph_and_records_events(self):
        submitted = self.submit(
            [
                {
                    "id": "write",
                    "command": [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; Path('result').write_text('committed')",
                    ],
                },
                {
                    "id": "read",
                    "needs": ["write"],
                    "command": [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; assert Path('result').read_text()=='committed'",
                    ],
                },
            ]
        )
        result = self.worker.run_once()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual((self.root / "result").read_text(), "committed")
        saved = self.store.get_run(self.workspace, submitted["id"])
        self.assertEqual(saved["status"], "succeeded")
        events = self.store.events(self.workspace, submitted["id"])
        self.assertEqual(sum(event["kind"] == "task_finished" for event in events), 2)
        self.assertEqual(events[-1]["kind"], "finished")
        self.assertIsNone(self.worker.run_once())

    def test_failed_task_blocks_dependent_effect(self):
        self.submit(
            [
                {"id": "bad", "command": [sys.executable, "-c", "raise SystemExit(3)"]},
                {
                    "id": "after",
                    "needs": ["bad"],
                    "command": [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; Path('unexpected').touch()",
                    ],
                },
            ]
        )
        result = self.worker.run_once()
        self.assertEqual(result["status"], "failed")
        self.assertFalse((self.root / "unexpected").exists())
        self.assertEqual(result["result"]["tasks"][1]["status"], "blocked")

    def test_running_cancellation_stops_command_and_pending_tasks(self):
        submitted = self.submit(
            [
                {
                    "id": "slow",
                    "command": [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; import time; Path('started').touch(); time.sleep(10); Path('late').touch()",
                    ],
                },
                {
                    "id": "after",
                    "needs": ["slow"],
                    "command": [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; Path('after').touch()",
                    ],
                },
            ]
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.worker.run_once)
            deadline = time.monotonic() + 3
            while not (self.root / "started").exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue((self.root / "started").exists())
            self.store.cancel(self.workspace, submitted["id"])
            result = future.result(timeout=5)
        self.assertEqual(result["status"], "cancelled")
        self.assertFalse((self.root / "late").exists())
        self.assertFalse((self.root / "after").exists())
        self.assertEqual(
            self.store.get_run(self.workspace, submitted["id"])["status"], "cancelled"
        )

    def test_heartbeat_keeps_longer_run_owned(self):
        submitted = self.submit(
            [
                {
                    "id": "long",
                    "command": [sys.executable, "-c", "import time; time.sleep(5.5)"],
                }
            ]
        )
        result = self.worker.run_once()
        self.assertTrue(result["ownership_retained"])
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(
            self.store.get_run(self.workspace, submitted["id"])["status"], "succeeded"
        )

