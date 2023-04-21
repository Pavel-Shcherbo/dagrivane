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

    def test_two_runs_use_fresh_directories_and_source_template_stays_unmodified(self):
        self.initialize()
        first = self.operations.submit(1, "first")["submission"]["id"]
        second = self.operations.submit(1, "second")["submission"]["id"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(self.operations.work, [first, second]))
        self.assertTrue(
            all(row["detail"]["run"]["status"] == "succeeded" for row in results)
        )
        roots = [
            self.operations.worker_root / "workspaces" / run_id
            for run_id in [first, second]
        ]
        self.assertTrue(
            all((root / ".local/typed-summary.json").is_file() for root in roots)
        )
        self.assertFalse(list((self.root / "templates").rglob(".local")))

    def test_failed_version_keeps_old_success_and_blocks_dependents(self):
        self.initialize()
        published = self.operations.publish_version("quality-failure", 1)
        self.assertEqual(published["workflow"]["version"], 2)
        first = self.operations.submit(1, "old")["submission"]["id"]
        second = self.operations.submit(2, "new")["submission"]["id"]
        self.assertEqual(
            self.operations.work(first)["detail"]["run"]["status"], "succeeded"
        )
        failed = self.operations.work(second)["detail"]
        self.assertEqual(failed["run"]["status"], "failed")
        self.assertEqual(
            [task["status"] for task in failed["tasks"]],
            ["failed", "blocked", "blocked"],
        )
        self.assertTrue(failed["tasks"][0]["output"])
        self.assertIsNone(failed["output"])
        with self.assertRaises(Conflict):
            self.operations.publish_version("standard", 1)
        self.assertFalse(
            self.operations.publish_version("quality-failure", 1)["workflow"]["changed"]
        )

    def test_queued_cancel_and_explicit_retry_create_separate_pinned_run(self):
        self.initialize()
        original = self.operations.submit(1, "queued")["submission"]["id"]
        self.assertEqual(self.operations.cancel(original)["run"]["status"], "cancelled")
        retried = self.operations.retry(original, "retry")["submission"]
        self.assertNotEqual(retried["id"], original)
        self.assertEqual(
            self.operations.retry(original, "retry")["submission"]["id"], retried["id"]
        )
        self.assertEqual(
            self.operations.work(retried["id"])["detail"]["run"]["status"], "succeeded"
        )
        self.assertEqual(
            self.operations.inspect(original)["run"]["status"], "cancelled"
        )
        self.assertEqual(
            self.operations.inspect(retried["id"])["run"]["retry_of"], original
        )

    def test_running_cancellation_records_remaining_tasks_without_release_output(self):
        self.initialize()
        self.operations.publish_version("cancellation", 1)
        run_id = self.operations.submit(2, "slow")["submission"]["id"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.operations.work, run_id)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                detail = self.operations.inspect(run_id)
                if any(
                    task["id"] == "pause" and task["status"] == "running"
                    for task in detail["tasks"]
                ):
                    break
                time.sleep(0.02)
            else:
                self.fail("cancellation control never entered running pause")
            self.operations.cancel(run_id)
            result = future.result(timeout=8)["detail"]
        self.assertEqual(result["run"]["status"], "cancelled")
        self.assertEqual(result["tasks"][0]["status"], "cancelled")
        self.assertIsNone(result["output"])
        self.assertFalse(
            (
                self.operations.worker_root
                / "workspaces"
                / run_id
                / ".local/typed-summary.json"
            ).exists()
        )

    def test_unknown_workflow_is_not_executed_or_cancelled_by_operations(self):
        state, store = self.initialize()
        workspace = state["workspace"]["id"]
        marker = self.root / "unexpected"
        workflow = store.register_workflow(
            workspace,
            "Other",
            {
                "tasks": [
                    {
                        "id": "write",
                        "command": [
                            sys.executable,
                            "-c",
                            f"open({str(marker)!r},'w').write('bad')",
                        ],
                    }
                ]
            },
            self.root,
        )
        other = store.enqueue(workspace, workflow["id"], 1, "other")
        for action in [self.operations.work, self.operations.cancel]:
            with self.assertRaisesRegex(ValueError, "does not belong"):
                action(other["id"])
        run_id = self.operations.submit(1, "own")["submission"]["id"]
        self.assertEqual(
            self.operations.work(run_id)["detail"]["run"]["status"], "succeeded"
        )
        self.assertEqual(store.get_run(workspace, other["id"])["status"], "queued")
        self.assertFalse(marker.exists())

    def test_tampered_registered_spec_is_rejected_before_process_execution(self):
        state, store = self.initialize()
        run_id = self.operations.submit(1, "own")["submission"]["id"]
        marker = self.root / "unexpected"
        with store.transaction() as db:
            altered = {
                "tasks": [
                    {
                        "id": "bad",
                        "command": [
                            sys.executable,
                            "-c",
                            f"open({str(marker)!r},'w').write('bad')",
                        ],
                    }
                ]
            }
            db.execute("UPDATE cp_versions SET specification=?", (json.dumps(altered),))
        result = self.operations.work(run_id)["detail"]
        self.assertEqual(result["run"]["status"], "failed")
        self.assertFalse(marker.exists())
        self.assertIn("reviewed", result["run"]["result"]["error"])
        self.assertFalse(result["workflow"]["approved"])

    def test_event_and_run_pages_have_stable_cursors(self):
        self.initialize()
        for index in range(27):
            self.operations.submit(1, "run-" + str(index))
        latest = self.operations.overview()
        older = self.operations.overview(before=latest["next_cursor"])
        self.assertEqual(len(latest["runs"]), 25)
        self.assertEqual(len(older["runs"]), 2)
        self.assertIsNone(older["next_cursor"])
        self.assertFalse(
            {row["id"] for row in latest["runs"]} & {row["id"] for row in older["runs"]}
        )
        run_id = latest["runs"][0]["id"]
        self.operations.work(run_id)
        first = self.operations.inspect(run_id)
        self.assertEqual(
            self.operations.inspect(run_id, after=first["event_cursor"])["events"], []
        )

    def test_web_rejects_injected_commands_and_bad_fields_before_state_creation(self):
        root = Path(self.temp.name) / "web-state"
        with patch.dict("os.environ", {"DAGRIVANE_STATE_DIR": str(root)}):
            for request in [
                {"mode": "control", "action": "work", "run_id": "../../outside"},
                {"mode": "control", "action": "initialize", "command": ["bad"]},
                {"mode": "control", "action": "overview", "status": []},
                {"mode": "control", "action": "overview", "before": 2**64},
                {
                    "mode": "control",
                    "action": "inspect",
                    "run_id": "00000000-0000-0000-0000-000000000001",
                    "after": 2**64,
                },
                {
                    "mode": "control",
                    "action": "submit",
                    "version": True,
                    "event_key": "bad",
                },
                {
                    "mode": "control",
                    "action": "publish",
                    "profile": "shell",
                    "expected_version": 1,
                },
            ]:
                with self.subTest(request=request), self.assertRaises(ValueError):
                    evaluate(request)
            self.assertFalse(root.exists())
            initialized = evaluate({"mode": "control", "action": "initialize"})
            self.assertTrue(initialized["initialized"])
            submitted = evaluate(
                {
                    "mode": "control",
                    "action": "submit",
                    "version": 1,
                    "event_key": "http",
                }
            )
            result = evaluate(
                {
                    "mode": "control",
                    "action": "work",
                    "run_id": submitted["submission"]["id"],
                }
            )
            self.assertEqual(result["detail"]["run"]["status"], "succeeded")


class ScopedWorkspaceTests(unittest.TestCase):
    def test_scoped_claim_never_reaps_or_claims_another_workspace(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            store = ControlStore(root / "control.sqlite")
            a = store.create_workspace("A")["id"]
            b = store.create_workspace("B")["id"]
            spec = {
                "tasks": [{"id": "noop", "command": [sys.executable, "-c", "pass"]}]
            }
            wa = store.register_workflow(a, "A", spec, root)
            wb = store.register_workflow(b, "B", spec, root)
            rb = store.enqueue(b, wb["id"], 1, "B", now=0)
            claimed = store.claim("B-worker", 5, now=1, workspace_id=b, run_id=rb["id"])
            ra = store.enqueue(a, wa["id"], 1, "A", now=10)
            selected = store.claim(
                "A-worker", 5, now=10, workspace_id=a, run_id=ra["id"]
            )
            self.assertEqual(selected["id"], ra["id"])
            self.assertEqual(store.get_run(b, rb["id"])["status"], "running")
            with self.assertRaises(ValueError):
                store.claim("bad", run_id=rb["id"])
            with self.assertRaises(NotFound):
                store.claim("bad", workspace_id=a, run_id=rb["id"])

