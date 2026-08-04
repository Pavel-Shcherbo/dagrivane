"""Calendar snapshots, receipt atomicity, input fencing and actual HTTP controls."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import copy
import csv
from decimal import Decimal
import http.client
import io
import json
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

from dagrivane.calendar_desk import CalendarDesk
from dagrivane.control import ControlStore, Conflict, ControlError
from dagrivane.operations import Operations
from dagrivane.schedule_evidence import verify_schedule_evidence
from dagrivane.schedule_model import digest
from dagrivane.resources import examples_directory
from tests.test_schedules import interval, cron


class CalendarDeskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "operations"
        self.desk = CalendarDesk(self.root)
        self.counter = 0
        meta = self.op("initialize")
        self.workspace, self.workflow = meta["workspace_id"], meta["workflow_id"]
        self.version = self.op(
            "version", workspace=self.workspace, workflow=self.workflow, version=1
        )

    def op(self, action, **fields):
        return self.desk.dispatch({"mode": "calendar", "action": action, **fields})

    def write(self, action, request_key=None, **fields):
        self.counter += 1
        return self.op(
            action,
            workspace=self.workspace,
            request_key=request_key or "request-" + str(self.counter),
            **fields,
        )

    def create(self, definition=None, **fields):
        value = self.op("preview", definition=definition or interval())
        return self.write(
            "create",
            workflow=self.workflow,
            version=1,
            workflow_sha256=self.version["fingerprint"],
            preview_sha256=value["plan_sha256"],
            definition=value["definition"],
            now=100,
            **fields,
        )

    def view(self, schedule, **fields):
        return self.op("view", workspace=self.workspace, schedule=schedule, **fields)

    def test_overview_and_preview_do_not_create_an_absent_database(self):
        missing = CalendarDesk(Path(self.temp.name) / "missing")
        self.assertFalse(
            missing.dispatch({"mode": "calendar", "action": "overview"})["initialized"]
        )
        result = missing.dispatch(
            {"mode": "calendar", "action": "preview", "definition": interval()}
        )
        self.assertEqual(result["items"][0]["due"], 120)
        self.assertFalse(missing.database.exists())
        self.assertEqual(self.op("initialize")["workspace_id"], self.workspace)
        self.assertEqual(len(ControlStore(self.desk.database).list_workspaces()), 1)

    def test_creation_uses_exact_preview_and_pinned_workflow(self):
        value = self.op("preview", definition=interval())
        for field in ("preview_sha256", "workflow_sha256"):
            args = {
                "workflow": self.workflow,
                "version": 1,
                "workflow_sha256": self.version["fingerprint"],
                "preview_sha256": value["plan_sha256"],
                "definition": value["definition"],
                "now": 100,
            }
            args[field] = "0" * 64
            with self.assertRaises(ValueError):
                self.write("create", **args)
        self.assertEqual(self.op("schedules", workspace=self.workspace)["items"], [])
        created = self.create()
        self.assertEqual(created["definition"], value["definition"])
        self.assertEqual(created["plan_sha256"], value["plan_sha256"])

    def test_changed_definition_cannot_reuse_old_clock_preview(self):
        value = self.op("preview", definition=interval())
        changed = copy.deepcopy(value["definition"])
        changed["trigger"]["seconds"] = 120
        with self.assertRaisesRegex(ValueError, "reviewed preview"):
            self.write(
                "create",
                workflow=self.workflow,
                version=1,
                workflow_sha256=self.version["fingerprint"],
                preview_sha256=value["plan_sha256"],
                definition=changed,
                now=100,
            )

    def test_retained_fold_preview_survives_host_timezone_source_removal(self):
        value = self.op(
            "preview",
            definition=cron(
                "30 1 * * *", "2024-11-03T00:00:00Z", end="2024-11-04T00:00:00Z"
            ),
        )
        self.assertEqual(
            [item["utc"] for item in value["items"]],
            ["2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"],
        )
        self.assertEqual([item["fold"] for item in value["items"]], [0, 1])
        with patch("dagrivane.schedule_clock.TZPATH", ()):
            created = self.write(
                "create",
                workflow=self.workflow,
                version=1,
                workflow_sha256=self.version["fingerprint"],
                preview_sha256=value["plan_sha256"],
                definition=value["definition"],
                now=100,
            )
            self.assertTrue(self.view(created["id"])["audit"]["passed"])

    def test_pause_resume_generation_fence_and_exact_receipt_recovery(self):
        created = self.create()
        paused = self.write(
            "set-enabled",
            request_key="pause",
            schedule=created["id"],
            enabled=False,
            generation=1,
            reason="Maintenance",
            now=110,
        )
        self.assertEqual(paused["generation"], 2)
        self.desk = CalendarDesk(self.root)
        self.assertEqual(
            paused,
            self.write(
                "set-enabled",
                request_key="pause",
                schedule=created["id"],
                enabled=False,
                generation=1,
                reason="Maintenance",
                now=110,
            ),
        )
        self.assertEqual(
            paused, self.op("receipt", workspace=self.workspace, request_key="pause")
        )
        with self.assertRaises(Conflict):
            self.write(
                "set-enabled",
                schedule=created["id"],
                enabled=True,
                generation=1,
                reason="Stale screen",
                now=111,
            )
        self.write(
            "set-enabled",
            schedule=created["id"],
            enabled=True,
            generation=2,
            reason="Resume",
            now=112,
        )
        self.assertEqual(self.view(created["id"])["schedule"]["generation"], 3)
        self.assertEqual(self.view(created["id"])["schedule"]["next_due"], 120)

    def test_concurrent_pause_retries_keep_one_event_and_one_response(self):
        created = self.create()

        def pause(_):
            return CalendarDesk(self.root).dispatch(
                {
                    "mode": "calendar",
                    "action": "set-enabled",
                    "workspace": self.workspace,
                    "schedule": created["id"],
                    "request_key": "parallel",
                    "enabled": False,
                    "generation": 1,
                    "reason": "Parallel retry",
                    "now": 110,
                }
            )

        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(pause, range(4)))
        self.assertTrue(all(value == values[0] for value in values))
        with closing(sqlite3.connect(self.desk.database)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT count(*) FROM cp_timed_events WHERE kind='enabled_changed'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                db.execute(
                    "SELECT count(*) FROM wb_calendar_receipts WHERE request_key='parallel'"
                ).fetchone()[0],
                1,
            )

    def test_dispatch_cursor_queue_and_response_commit_together(self):
        created = self.create()
        original = self.desk.operate

        def interrupted(*args):
            original(*args)
            raise RuntimeError("response write interrupted")

        with patch.object(self.desk, "operate", side_effect=interrupted):
            with self.assertRaises(RuntimeError):
                self.write("dispatch", request_key="dispatch", now=240, limit=3)
        with closing(sqlite3.connect(self.desk.database)) as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM cp_runs").fetchone()[0], 0
            )
            self.assertEqual(
                db.execute("SELECT count(*) FROM cp_timed_occurrences").fetchone()[0], 0
            )
            self.assertEqual(
                db.execute("SELECT count(*) FROM cp_timed_receipts").fetchone()[0], 0
            )
        self.assertEqual(self.view(created["id"])["schedule"]["next_due"], 120)
        result = self.write("dispatch", request_key="dispatch", now=240, limit=3)
        self.assertEqual(result["outcome_count"], 3)
        self.assertEqual(
            result, self.write("dispatch", request_key="dispatch", now=240, limit=3)
        )
        with self.assertRaisesRegex(ValueError, "different inputs"):
            self.write("dispatch", request_key="dispatch", now=300, limit=3)

    def test_response_size_failure_rolls_back_pause_and_event(self):
        created = self.create()
        with patch("dagrivane.calendar_desk.MAX_RESPONSE", 1):
            with self.assertRaises(ValueError):
                self.write(
                    "set-enabled",
                    schedule=created["id"],
                    enabled=False,
                    generation=1,
                    reason="Budget",
                    now=110,
                )
        value = self.view(created["id"])
        self.assertTrue(value["schedule"]["enabled"])
        self.assertEqual(len(value["events"]["items"]), 1)

    def test_schedule_outcomes_and_events_have_explicit_page_cursors(self):
        created = self.create()
        self.write("dispatch", now=300, limit=4)
        first = self.view(created["id"], limit=2)
        second = self.view(
            created["id"], limit=2, due_after=first["occurrences"]["next_cursor"]
        )
        self.assertEqual(
            [
                item["due"]
                for item in first["occurrences"]["items"]
                + second["occurrences"]["items"]
            ],
            [120, 180, 240, 300],
        )
        self.assertTrue(first["occurrences"]["has_more"])
        self.assertFalse(second["occurrences"]["has_more"])
        self.assertTrue(first["audit"]["passed"])
        self.assertEqual(first["schedule"], second["schedule"])

    def test_tampered_receipt_and_decision_history_are_not_green(self):
        created = self.create(request_key="saved-create")
        with closing(sqlite3.connect(self.desk.database)) as db, db:
            db.execute(
                "UPDATE wb_calendar_receipts SET result=? WHERE request_key='saved-create'",
                (b"{}",),
            )
        with self.assertRaisesRegex(ValueError, "response identity"):
            self.op("receipt", workspace=self.workspace, request_key="saved-create")
        with closing(sqlite3.connect(self.desk.database)) as db, db:
            db.execute(
                "UPDATE cp_timed_events SET sha256=? WHERE schedule_id=?",
                ("0" * 64, created["id"]),
            )
        value = self.view(created["id"])
        self.assertFalse(value["audit"]["passed"])
        self.assertIsNotNone(value["audit"]["error"])
        with self.assertRaises(ValueError):
            self.write("export", schedule=created["id"])

    def test_large_audit_refusal_keeps_paged_inspection_available(self):
        created = self.create()
        self.write("dispatch", now=180, limit=2)
        with patch("dagrivane.schedule_evidence.MAX_OCCURRENCES", 1):
            value = self.view(created["id"], limit=1)
            self.assertFalse(value["audit"]["passed"])
            self.assertEqual(len(value["occurrences"]["items"]), 1)
            self.assertTrue(value["occurrences"]["has_more"])
            with self.assertRaises(Conflict):
                self.write("export", schedule=created["id"])

    def test_workspace_schedule_workflow_and_run_bindings_are_scoped(self):
        created = self.create()
        other = ControlStore(self.desk.database).create_workspace("Other")["id"]
        with self.assertRaises(ControlError):
            self.op("view", workspace=other, schedule=created["id"])
        with self.assertRaises(ControlError):
            self.op("version", workspace=other, workflow=self.workflow, version=1)
        with self.assertRaises(ValueError):
            self.op("receipt", workspace=other, request_key="request-1")
        self.write("dispatch", now=120, limit=1)
        run = self.view(created["id"])["occurrences"]["items"][0]["run_id"]
        second = self.create()
        with self.assertRaisesRegex(ValueError, "does not belong"):
            self.op("run", workspace=self.workspace, schedule=second["id"], run=run)

    def test_independent_run_effects_and_offline_export_after_store_removal(self):
        created = self.create()
        self.write("dispatch", now=120, limit=1)
        run = self.view(created["id"])["occurrences"]["items"][0]["run_id"]
        self.assertEqual(
            self.op("run", workspace=self.workspace, schedule=created["id"], run=run)[
                "status"
            ],
            "queued",
        )
        Operations(self.root).work(run)
        inspected = self.op(
            "run", workspace=self.workspace, schedule=created["id"], run=run
        )
        self.assertEqual(inspected["status"], "succeeded")
        raw = json.loads(
            (
                self.root / "worker/workspaces" / run / ".local/typed-summary.json"
            ).read_bytes()
        )
        with (examples_directory() / "orders.csv").open(newline="") as stream:
            source = list(csv.DictReader(stream))
        total = sum((Decimal(row["amount"]) for row in source), Decimal(0))
        self.assertEqual(
            raw["rows"],
            [{"currency": "USD", "orders": len(source), "revenue": str(total)}],
        )
        evidence = self.write("export", request_key="export", schedule=created["id"])
        self.assertEqual(
            evidence, self.write("export", request_key="export", schedule=created["id"])
        )
        self.desk.database.unlink()
        # The browser parses and serializes the outer response; the document stays text.
        transported = json.loads(json.dumps(evidence))
        document = json.loads(transported["content"])
        self.assertIsInstance(document["runs"][0]["created"], float)
        self.assertEqual(evidence["schedule"]["id"], document["schedule"]["id"])
        proof = verify_schedule_evidence(document)
        self.assertTrue(proof["passed"])
        self.assertFalse(proof["execution_verified"])
        self.assertFalse(proof["source_authenticated"])

