"""Durable workbench operations, scoped workers and isolated input provenance."""

import concurrent.futures
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from dagrivane.control import ControlStore, Conflict, NotFound
from dagrivane.operations import Operations
from dagrivane.run_workspace import create_run_workspace
from dagrivane.worker import Worker
from dagrivane.web import evaluate


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "ops"
        self.operations = Operations(self.root)

    def initialize(self):
        state = self.operations.initialize()
        return state, ControlStore(self.operations.database)

    def test_overview_does_not_initialize_and_bootstrap_is_atomic(self):
        self.assertFalse(self.operations.overview()["initialized"])
        self.assertFalse(self.root.exists())
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            states = list(pool.map(lambda _: self.operations.initialize(), range(4)))
        self.assertEqual(len({state["workspace"]["id"] for state in states}), 1)
        store = ControlStore(self.operations.database)
        self.assertEqual(len(store.list_workspaces()), 1)
        self.assertEqual(len(states[0]["versions"]), 1)

    def test_pinned_version_executes_and_reopen_retains_actual_result(self):
        state, store = self.initialize()
        submitted = self.operations.submit(1, "one")["submission"]
        result = self.operations.work(submitted["id"])
        self.assertEqual(result["detail"]["run"]["status"], "succeeded")
        self.assertTrue(result["detail"]["output"]["passed"])
        self.assertEqual(
            result["detail"]["output"]["rows"],
            [{"currency": "USD", "revenue": "112.00", "orders": 4}],
        )
        reopened = Operations(self.root).inspect(submitted["id"])
        self.assertEqual(reopened["run"]["result"], result["detail"]["run"]["result"])
        self.assertTrue(
            any(event["kind"] == "workspace_prepared" for event in reopened["events"])
        )
        self.assertNotIn("lease_token", json.dumps(reopened))
        self.assertEqual(self.operations.work(submitted["id"])["worker"], None)

    def test_output_recheck_detects_later_damage_without_rewriting_run_outcome(self):
        self.initialize()
        run_id = self.operations.submit(1, "damaged-output")["submission"]["id"]
        self.assertTrue(self.operations.work(run_id)["detail"]["output"]["passed"])
        output = (
            self.operations.worker_root
            / "workspaces"
            / run_id
            / ".local/typed-summary.json"
        )
        document = json.loads(output.read_text())
        document["rows"][0]["revenue"] = "999.00"
        output.write_text(json.dumps(document))
        current = self.operations.inspect(run_id)
        self.assertEqual(current["run"]["status"], "succeeded")
        self.assertFalse(current["output"]["passed"])

    def test_retained_multibyte_output_obeys_per_task_and_total_byte_budgets(self):
        state, store = self.initialize()
        workflow = store.register_workflow(
            state["workspace"]["id"],
            "Bounded logs",
            {
                "tasks": [
                    {
                        "id": "log-" + str(index),
                        "command": [sys.executable, "-c", "print('€' * 3000 + 'TAIL')"],
                    }
                    for index in range(9)
                ]
            },
            self.root,
        )
        run_id = store.enqueue(
            state["workspace"]["id"], workflow["id"], workflow["version"], "multibyte"
        )["id"]
        worked = Worker(store, self.root / "log-worker", "log-check").run_once(
            workspace_id=state["workspace"]["id"], run_id=run_id
        )
        self.assertEqual(worked["status"], "succeeded")
        tasks = store.get_run(state["workspace"]["id"], run_id)["result"]["tasks"]
        sizes = [len(task["output"].encode("utf-8")) for task in tasks]
        self.assertTrue(all(size <= 8192 for size in sizes))
        self.assertLessEqual(sum(sizes), 65536)
        self.assertTrue(all(task["output_truncated"] for task in tasks))
        self.assertTrue(tasks[0]["output"].endswith("TAIL\n"))

