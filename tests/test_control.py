from contextlib import closing
import concurrent.futures
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from dagrivane.control import Conflict, ControlStore, LostLease, NotFound


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / "control.db"
        self.store = ControlStore(self.path)
        self.workspace = self.store.create_workspace("Research", concurrency=2, now=1)[
            "id"
        ]
        self.spec = {
            "tasks": [{"id": "check", "command": [sys.executable, "-c", "print('ok')"]}]
        }
        self.workflow = self.store.register_workflow(
            self.workspace, "Control", self.spec, self.root, now=2
        )

    def submit(self, key="event", **kwargs):
        return self.store.enqueue(
            self.workspace,
            self.workflow["id"],
            self.workflow["version"],
            key,
            now=3,
            **kwargs,
        )

    def test_versions_are_immutable_and_unchanged_registration_is_idempotent(self):
        same = self.store.register_workflow(
            self.workspace, "Control", self.spec, self.root, now=4
        )
        changed = {
            "tasks": [{"id": "different", "command": [sys.executable, "-c", "pass"]}]
        }
        new = self.store.register_workflow(
            self.workspace, "Control", changed, self.root, now=5
        )
        self.assertEqual(same["version"], 1)
        self.assertFalse(same["changed"])
        self.assertEqual(new["version"], 2)
        original = self.store.workflow(self.workspace, self.workflow["id"], 1)
        self.assertEqual(original["specification"], self.spec)
        self.assertEqual(self.store.list_workflows(self.workspace)[0]["version"], 2)

    def test_submission_key_deduplicates_and_rejects_different_payload(self):
        first = self.submit()
        again = self.submit()
        self.assertEqual(first["id"], again["id"])
        self.assertFalse(again["created"])
        with self.assertRaises(Conflict):
            self.submit(priority=10)
        self.assertEqual(len(self.store.list_runs(self.workspace)), 1)

    def test_workspace_scope_applies_to_workflows_runs_and_events(self):
        other = self.store.create_workspace("Other")["id"]
        run = self.submit()
        with self.assertRaises(NotFound):
            self.store.get_run(other, run["id"])
        with self.assertRaises(NotFound):
            self.store.events(other, run["id"])
        with self.assertRaises(NotFound):
            self.store.enqueue(other, self.workflow["id"], 1, "foreign")
        self.assertEqual(self.store.list_runs(other), [])

    def test_concurrent_claims_respect_ownership_and_workspace_limit(self):
        for i in range(10):
            self.submit(str(i))

        def claim(i):
            return ControlStore(self.path).claim(f"worker-{i}", now=10)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            claims = list(executor.map(claim, range(8)))
        actual = [claim for claim in claims if claim]
        self.assertEqual(len(actual), 2)
        self.assertEqual(len({claim["id"] for claim in actual}), 2)
        self.assertEqual(len(self.store.list_runs(self.workspace, status="queued")), 8)

    def test_expired_ownership_is_fenced_and_requires_explicit_retry(self):
        run = self.submit()
        owner = self.store.claim("first", lease_seconds=5, now=10)
        with self.assertRaises(LostLease):
            self.store.heartbeat(run["id"], "first", owner["lease_token"], now=15)
        self.assertIsNone(self.store.claim("next", now=16))
        self.assertEqual(
            self.store.get_run(self.workspace, run["id"])["status"], "orphaned"
        )
        with self.assertRaises(LostLease):
            self.store.finish(
                run["id"], "first", owner["lease_token"], "succeeded", {}, now=16
            )
        retry = self.store.retry(self.workspace, run["id"], "operator-retry", now=17)
        self.assertNotEqual(run["id"], retry["id"])
        fresh = self.store.claim("next", now=18)
        self.assertEqual(fresh["retry_of"], run["id"])
        self.assertNotEqual(fresh["lease_token"], owner["lease_token"])

    def test_cancel_queued_run_never_becomes_worker_work(self):
        run = self.submit()
        self.assertEqual(
            self.store.cancel(self.workspace, run["id"], now=4)["status"], "cancelled"
        )
        self.assertIsNone(self.store.claim("worker", now=5))
        with self.assertRaises(Conflict):
            self.store.cancel(self.workspace, run["id"], now=6)

    def test_running_cancellation_is_visible_and_cannot_finish_as_success(self):
        run = self.submit()
        owner = self.store.claim("worker", now=10)
        self.store.cancel(self.workspace, run["id"], now=11)
        heartbeat = self.store.heartbeat(
            run["id"], "worker", owner["lease_token"], now=12
        )
        self.assertTrue(heartbeat["cancel_requested"])
        with self.assertRaises(Conflict):
            self.store.finish(
                run["id"], "worker", owner["lease_token"], "succeeded", {}, now=13
            )
        self.store.finish(
            run["id"],
            "worker",
            owner["lease_token"],
            "cancelled",
            {"effect_review": "no external effects"},
            now=14,
        )
        self.assertEqual(
            self.store.get_run(self.workspace, run["id"])["status"], "cancelled"
        )

    def test_event_cursor_is_stable_and_public_result_omits_token(self):
        run = self.submit()
        owner = self.store.claim("worker", now=10)
        self.store.append_event(
            run["id"],
            "worker",
            owner["lease_token"],
            "task_started",
            {"task": "check"},
            now=11,
        )
        self.store.finish(
            run["id"],
            "worker",
            owner["lease_token"],
            "succeeded",
            {"passed": True},
            now=12,
        )
        first = self.store.events(self.workspace, run["id"], limit=2)
        second = self.store.events(
            self.workspace, run["id"], after=first[-1]["sequence"]
        )
        self.assertEqual(
            [row["kind"] for row in first + second],
            ["submitted", "claimed", "task_started", "finished"],
        )
        self.assertEqual(len({row["sequence"] for row in first + second}), 4)
        public = self.store.get_run(self.workspace, run["id"])
        self.assertNotIn("lease_token", public)
        self.assertEqual(public["result"], {"passed": True})

    def test_schedule_coalesces_missed_intervals_and_pins_version(self):
        schedule = self.store.create_schedule(
            self.workspace, self.workflow["id"], 1, 60, 100
        )
        first = self.store.tick(now=100)
        self.assertEqual(len(first), 1)
        self.assertEqual(self.store.tick(now=100), [])
        changed = {"tasks": [{"id": "new", "command": [sys.executable, "-c", "pass"]}]}
        self.store.register_workflow(
            self.workspace, "Control", changed, self.root, now=200
        )
        later = self.store.tick(now=1000)
        self.assertEqual(len(later), 1)
        self.assertEqual(
            self.store.get_run(self.workspace, later[0]["id"])["version"], 1
        )
        scheduled = self.store.events(self.workspace, later[0]["id"])[-1]
        self.assertEqual(scheduled["payload"]["coalesced_periods"], 14)
        self.store.set_schedule_enabled(self.workspace, schedule["id"], False)
        self.assertEqual(self.store.tick(now=2000), [])

    def test_schedule_isolated_from_other_workspace(self):
        other = self.store.create_workspace("Other")["id"]
        schedule = self.store.create_schedule(
            self.workspace, self.workflow["id"], 1, 60, 100
        )
        with self.assertRaises(NotFound):
            self.store.set_schedule_enabled(other, schedule["id"], False)

    def test_reopen_preserves_runs_and_does_not_reset_database(self):
        run = self.submit()
        reopened = ControlStore(self.path)
        self.assertEqual(
            reopened.get_run(self.workspace, run["id"])["status"], "queued"
        )
        self.assertEqual(len(reopened.list_workflows(self.workspace)), 1)

    def test_concurrent_initialization_is_serialized(self):
        for attempt in range(3):
            path = self.root / f"new-{attempt}.db"
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                stores = list(executor.map(lambda _: ControlStore(path), range(6)))
            self.assertEqual(stores[0].list_workspaces(), [])

    def test_newer_schema_is_not_downgraded(self):
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute("PRAGMA user_version=99")
        with self.assertRaises(Conflict):
            ControlStore(self.path)
        with closing(sqlite3.connect(self.path)) as connection, connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0], 99
            )

    def test_invalid_parameters_rejected_before_changes(self):
        with self.assertRaises(ValueError):
            self.store.create_workspace("Bad", concurrency=True)
        with self.assertRaises(ValueError):
            self.store.claim("worker", lease_seconds=0)
        with self.assertRaises(ValueError):
            self.store.list_runs(self.workspace, status="unknown")
        with self.assertRaises(ValueError):
            self.store.workflow(self.workspace, self.workflow["id"], True)
        self.assertEqual(self.store.list_runs(self.workspace), [])
