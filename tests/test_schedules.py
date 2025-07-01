"""Retained recurrence clocks, atomic dispatch, overlap and failure/restart evidence."""

import base64
import concurrent.futures
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from dagrivane.control import Conflict, ControlStore, NotFound
from dagrivane.schedule_clock import instant, iso
from dagrivane.schedule_model import compile_schedule, digest, preview_schedule
from dagrivane.schedule_store import ScheduleStore
from dagrivane.schedule_evidence import (
    export_schedule,
    save_evidence,
    verify_schedule_evidence,
)
from dagrivane.worker import Worker


def interval(**changes):
    value = {
        "version": 1,
        "name": "Minute pipeline",
        "trigger": {"kind": "interval", "seconds": 60},
        "start_at": 120,
        "end_at": None,
        "misfire": {"policy": "catch_up", "max_lateness_seconds": None},
        "overlap": "allow",
        "priority": 0,
    }
    value.update(changes)
    return value


def cron(
    expression, start, zone="America/New_York", fold="both", end=None, match="and"
):
    return interval(
        trigger={
            "kind": "cron",
            "expression": expression,
            "timezone": zone,
            "fold": fold,
            "gap": "skip",
            "day_match": match,
        },
        start_at=start,
        end_at=end,
    )


class ScheduleClockTests(unittest.TestCase):
    def test_strict_instants(self):
        self.assertEqual(
            instant("2024-01-01T03:00:00+03:00"), instant("2024-01-01T00:00:00Z")
        )
        self.assertEqual(instant("120"), 120)
        for value in [
            True,
            -1,
            0.5,
            float("nan"),
            float("inf"),
            "2024-01-01",
            "2024-01-01T00:00:00.1Z",
            4133980800,
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                instant(value)

    def test_interval_anchor_inclusive_bounds_and_reverse(self):
        clock = compile_schedule(interval(end_at=300)).clock()
        self.assertEqual(
            [clock.next(t) for t in (0, 120, 121, 240, 241)], [120, 120, 180, 240, None]
        )
        self.assertEqual(
            [clock.previous(t) for t in (0, 119, 120, 179, 180, 999)],
            [None, None, 120, 120, 180, 240],
        )

    def test_five_fields_lists_ranges_and_steps(self):
        value = cron("0,30 9-10 * * 1-5", "2024-03-08T00:00:00Z")
        items = preview_schedule(value, limit=5)["items"]
        self.assertEqual(
            [item["utc"] for item in items],
            [
                "2024-03-08T14:00:00Z",
                "2024-03-08T14:30:00Z",
                "2024-03-08T15:00:00Z",
                "2024-03-08T15:30:00Z",
                "2024-03-11T13:00:00Z",
            ],
        )
        fields = (
            compile_schedule(cron("*/15 * * * *", "2024-01-01T00:00:00Z", "UTC"))
            .clock()
            .fields
        )
        self.assertEqual(fields[0], (0, 15, 30, 45))

    def test_fold_first_second_and_both(self):
        for policy, expected in [
            ("first", ["2024-11-03T05:30:00Z"]),
            ("second", ["2024-11-03T06:30:00Z"]),
            ("both", ["2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"]),
        ]:
            with self.subTest(policy=policy):
                value = cron(
                    "30 1 * * *",
                    "2024-11-03T00:00:00Z",
                    fold=policy,
                    end="2024-11-04T00:00:00Z",
                )
                result = preview_schedule(value, limit=5)
                self.assertEqual([item["utc"] for item in result["items"]], expected)
                self.assertFalse(result["has_more"])

    def test_gap_skips_instead_of_inventing_local_time(self):
        result = preview_schedule(cron("30 2 * * *", "2024-03-09T00:00:00Z"), limit=3)
        self.assertEqual(
            [item["utc"] for item in result["items"]],
            ["2024-03-09T07:30:00Z", "2024-03-11T06:30:00Z", "2024-03-12T06:30:00Z"],
        )

    def test_half_hour_fold_and_date_line_gap(self):
        value = cron(
            "45 1 * * *",
            "2024-04-06T00:00:00Z",
            "Australia/Lord_Howe",
            end="2024-04-07T03:00:00Z",
        )
        items = preview_schedule(value, limit=5)["items"]
        self.assertEqual(
            [item["utc"] for item in items],
            ["2024-04-06T14:45:00Z", "2024-04-06T15:15:00Z"],
        )
        value = cron(
            "0 12 * * *",
            "2011-12-29T00:00:00Z",
            "Pacific/Apia",
            end="2012-01-01T00:00:00Z",
        )
        locals = [
            item["local"][:10] for item in preview_schedule(value, limit=5)["items"]
        ]
        self.assertNotIn("2011-12-30", locals)
        self.assertIn("2011-12-31", locals)

    def test_reverse_recursions_agree_with_forward_fold_order(self):
        clock = compile_schedule(cron("30 1 * * *", "2024-11-03T00:00:00Z")).clock()
        first = instant("2024-11-03T05:30:00Z")
        second = instant("2024-11-03T06:30:00Z")
        self.assertEqual(clock.previous(second), second)
        self.assertEqual(clock.previous(second - 1), first)
        self.assertEqual(clock.next(first + 1), second)

    def test_leap_day_and_explicit_day_matching(self):
        value = cron("0 0 29 2 *", "2025-01-01T00:00:00Z", "UTC")
        self.assertEqual(
            preview_schedule(value, limit=1)["items"][0]["utc"], "2028-02-29T00:00:00Z"
        )
        value = cron("0 0 1 * 1", "2024-05-01T00:00:00Z", "UTC", match="or")
        self.assertEqual(
            [item["utc"] for item in preview_schedule(value, limit=2)["items"]],
            ["2024-05-01T00:00:00Z", "2024-05-06T00:00:00Z"],
        )
        value["trigger"]["day_match"] = "and"
        self.assertEqual(
            preview_schedule(value, limit=1)["items"][0]["utc"], "2024-07-01T00:00:00Z"
        )

    def test_frozen_timezone_does_not_need_system_data_on_replay(self):
        plan = compile_schedule(cron("30 9 * * *", "2024-01-01T00:00:00Z"))
        with (
            patch("dagrivane.schedule_clock.TZPATH", ()),
            patch(
                "dagrivane.schedule_clock.resources.files",
                side_effect=ModuleNotFoundError,
            ),
        ):
            self.assertEqual(compile_schedule(plan.definition).sha256, plan.sha256)
            self.assertEqual(
                preview_schedule(plan.definition, limit=1)["items"][0]["utc"],
                "2024-01-01T14:30:00Z",
            )
        broken = copy.deepcopy(plan.definition)
        broken["trigger"]["timezone"]["sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            compile_schedule(broken)

    def test_invalid_contracts_and_cron_fields(self):
        values = []
        for expression in [
            "* * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * * 13 *",
            "* * * * 7",
            "1/2 * * * *",
            "*/0 * * * *",
            "@daily",
            "* * * * MON",
            "30-10 * * * *",
        ]:
            values.append(cron(expression, "2024-01-01T00:00:00Z"))
        values.extend(
            [
                interval(priority=True),
                interval(start_at=True),
                interval(end_at=119),
                interval(overlap="replace"),
                interval(trigger={"kind": "interval", "seconds": 1}),
                interval(misfire={"policy": "skip", "max_lateness_seconds": None}),
            ]
        )
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                compile_schedule(value)

    def test_impossible_cron_has_bounded_search(self):
        with self.assertRaisesRegex(ValueError, "ten-year"):
            preview_schedule(cron("0 0 30 2 *", "2024-01-01T00:00:00Z", "UTC"), limit=1)
        result = preview_schedule(
            cron(
                "0 0 30 2 *", "2024-01-01T00:00:00Z", "UTC", end="2025-01-01T00:00:00Z"
            ),
            limit=1,
        )
        self.assertEqual(result["items"], [])


class ScheduleStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "control.sqlite"
        self.control = ControlStore(self.path)
        self.store = ScheduleStore(self.control)
        self.workspace = self.control.create_workspace("Scheduled", now=0)["id"]
        self.spec = {
            "tasks": [
                {
                    "id": "write",
                    "command": [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; Path('scheduled-result').write_text('42')",
                    ],
                }
            ]
        }
        self.workflow = self.control.register_workflow(
            self.workspace, "Example", self.spec, self.root, now=0
        )

    def create(self, definition=None, key="create"):
        return self.store.create(
            self.workspace, self.workflow["id"], 1, definition or interval(), key, now=0
        )

    def dispatch(self, key="tick", now=300, limit=100):
        return self.store.dispatch(self.workspace, key, now=now, limit=limit)

    def proof(self, schedule):
        document = export_schedule(self.store, self.workspace, schedule["id"])
        self.assertTrue(verify_schedule_evidence(document)["passed"])
        return document

    def test_catch_up_persists_each_due_and_next_cursor(self):
        schedule = self.create()
        result = self.dispatch()
        self.assertEqual(
            [o["due"] for o in result["schedules"][0]["outcomes"]], [120, 180, 240, 300]
        )
        self.assertEqual(result["schedules"][0]["next_due"], 360)
        self.assertEqual(len(self.proof(schedule)["runs"]), 4)

    def test_coalesce_and_skip_have_explicit_omitted_ranges(self):
        for index, policy in enumerate(("coalesce", "skip")):
            schedule = self.create(
                interval(misfire={"policy": policy, "max_lateness_seconds": 1000}),
                key=str(index),
            )
        result = self.dispatch()
        for item in result["schedules"]:
            self.assertEqual([o["due"] for o in item["outcomes"]], [300])
            self.assertEqual(item["omitted_ranges"][0]["count"], 3)
        for item in self.store.list(self.workspace)["items"]:
            self.proof(item)

    def test_expired_backlog_advances_without_an_unbounded_loop(self):
        schedule = self.create(
            interval(misfire={"policy": "skip", "max_lateness_seconds": 0})
        )
        result = self.dispatch(now=3000001)["schedules"][0]
        self.assertEqual(result["outcomes"], [])
        self.assertGreater(result["omitted_ranges"][0]["count"], 40000)
        self.assertGreater(result["next_due"], 3000001)
        self.proof(schedule)

    def test_grace_boundary_is_inclusive(self):
        schedule = self.create(
            interval(misfire={"policy": "catch_up", "max_lateness_seconds": 60})
        )
        item = self.dispatch()["schedules"][0]
        self.assertEqual([o["due"] for o in item["outcomes"]], [240, 300])
        self.assertEqual(item["omitted_ranges"][0]["through_due"], 180)
        self.proof(schedule)

    def test_bounded_batches_continue_without_repeating_slots(self):
        schedule = self.create()
        first = self.dispatch("one", limit=2)
        second = self.dispatch("two", limit=2)
        self.assertTrue(first["due_remaining"])
        self.assertFalse(second["due_remaining"])
        self.assertEqual(first["outcome_count"] + second["outcome_count"], 4)
        self.assertEqual(len(self.proof(schedule)["occurrences"]), 4)

    def test_receipt_retry_after_restart_is_identical(self):
        schedule = self.create()
        first = self.dispatch()
        self.store = ScheduleStore(ControlStore(self.path))
        self.assertEqual(first, self.dispatch())
        self.assertEqual(len(self.proof(schedule)["runs"]), 4)
        with self.assertRaises(Conflict):
            self.dispatch(now=301)

    def test_concurrent_dispatchers_do_not_duplicate(self):
        schedule = self.create()

        def tick(i):
            store = ScheduleStore(ControlStore(self.path))
            return store.dispatch(self.workspace, str(i), now=300, limit=2)

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            values = list(pool.map(tick, range(6)))
        self.assertEqual(sum(v["outcome_count"] for v in values), 4)
        self.assertEqual(len(self.proof(schedule)["runs"]), 4)

    def test_transaction_failure_rolls_back_cursor_queue_history_and_receipt(self):
        schedule = self.create()
        before = self.proof(schedule)
        original = self.control._enqueue
        count = 0

        def failing(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise RuntimeError("interrupted dispatch")
            return original(*args, **kwargs)

        with (
            patch.object(self.control, "_enqueue", failing),
            self.assertRaises(RuntimeError),
        ):
            self.dispatch()
        self.assertEqual(before, self.proof(schedule))
        self.assertEqual(self.dispatch()["outcome_count"], 4)

    def test_overlap_skip_includes_queued_work(self):
        schedule = self.create(interval(overlap="skip"))
        item = self.dispatch()["schedules"][0]
        self.assertEqual(
            [o["outcome"] for o in item["outcomes"]],
            ["submitted"] + ["skipped_overlap"] * 3,
        )
        self.assertEqual(len(self.proof(schedule)["runs"]), 1)

    def test_overlap_defer_keeps_due_then_resumes_after_worker_success(self):
        schedule = self.create(interval(overlap="defer", end_at=300))
        first = self.dispatch("one", now=180)["schedules"][0]
        self.assertEqual(first["next_due"], 180)
        self.assertIsNotNone(first["deferred_by"])
        worker = Worker(
            self.control, self.root / "task-state", "schedule-worker", lease_seconds=5
        )
        self.assertEqual(worker.run_once()["status"], "succeeded")
        self.assertEqual((self.root / "scheduled-result").read_text(), "42")
        second = self.dispatch("two", now=180)["schedules"][0]
        self.assertEqual([o["due"] for o in second["outcomes"]], [180])
        self.assertEqual(len(self.proof(schedule)["runs"]), 2)

    def test_pause_resume_requires_generation_and_keeps_cursor(self):
        schedule = self.create()
        paused = self.store.set_enabled(
            self.workspace, schedule["id"], False, 1, "maintenance", now=200
        )
        self.assertEqual(self.dispatch()["outcome_count"], 0)
        with self.assertRaises(Conflict):
            self.store.set_enabled(
                self.workspace, schedule["id"], True, 1, "stale", now=300
            )
        resumed = self.store.set_enabled(
            self.workspace, schedule["id"], True, paused["generation"], "ready", now=300
        )
        self.assertEqual(resumed["next_due"], 120)
        self.assertEqual(self.dispatch("new")["outcome_count"], 4)
        self.proof(schedule)

    def test_pinned_version_and_create_idempotence(self):
        schedule = self.create()
        changed = {
            "tasks": [{"id": "other", "command": [sys.executable, "-c", "pass"]}]
        }
        self.control.register_workflow(
            self.workspace, "Example", changed, self.root, now=1
        )
        self.assertEqual(self.create()["id"], schedule["id"])
        with self.assertRaises(Conflict):
            self.create(interval(priority=1))
        self.dispatch()
        self.assertTrue(
            all(run["version"] == 1 for run in self.proof(schedule)["runs"])
        )

    def test_workspace_scope_and_pagination(self):
        one = self.create()
        self.create(key="other")
        other = self.control.create_workspace("Other")["id"]
        for fn in [
            lambda: self.store.inspect(other, one["id"]),
            lambda: self.store.events(other, one["id"]),
            lambda: self.store.occurrences(other, one["id"]),
            lambda: export_schedule(self.store, other, one["id"]),
        ]:
            with self.assertRaises(NotFound):
                fn()
        self.assertEqual(self.store.list(other)["items"], [])
        page = self.store.list(self.workspace, limit=1)
        self.assertTrue(page["has_more"])
        self.assertEqual(
            len(self.store.list(self.workspace, after=page["next_cursor"])["items"]), 1
        )
        self.dispatch()
        page = self.store.occurrences(self.workspace, one["id"], limit=2)
        self.assertTrue(page["has_more"])
        self.assertEqual(
            len(
                self.store.occurrences(
                    self.workspace, one["id"], after=page["next_cursor"]
                )["items"]
            ),
            2,
        )

    def test_export_tamper_rehash_and_create_only_output(self):
        schedule = self.create()
        self.dispatch()
        document = self.proof(schedule)
        path = self.root / "export.json"
        save_evidence(document, path)
        with self.assertRaises(FileExistsError):
            save_evidence(document, path)
        forged = copy.deepcopy(document)
        forged["schedule"]["next_due"] = 600
        forged["sha256"] = digest({k: v for k, v in forged.items() if k != "sha256"})
        with self.assertRaisesRegex(ValueError, "differs from history"):
            verify_schedule_evidence(forged)
        forged = copy.deepcopy(document)
        forged["runs"][0]["version"] = 2
        forged["sha256"] = digest({k: v for k, v in forged.items() if k != "sha256"})
        with self.assertRaisesRegex(ValueError, "pinned inputs"):
            verify_schedule_evidence(forged)

    def test_frozen_fold_dispatch_and_offline_verification(self):
        schedule = self.create(
            cron("30 1 * * *", "2024-11-03T00:00:00Z", end="2024-11-04T00:00:00Z")
        )
        result = self.dispatch(now=instant("2024-11-03T07:00:00Z"))
        self.assertEqual(
            [iso(o["due"]) for o in result["schedules"][0]["outcomes"]],
            ["2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"],
        )
        with patch("dagrivane.schedule_clock.TZPATH", ()):
            self.proof(schedule)

    def test_complete_audit_refuses_exceeding_limits(self):
        schedule = self.create()
        self.dispatch()
        with (
            patch("dagrivane.schedule_evidence.MAX_EVENTS", 1),
            self.assertRaises(Conflict),
        ):
            self.proof(schedule)

    def test_invalid_inputs_do_not_write_schedule(self):
        with self.assertRaises(ValueError):
            self.create(interval(priority=True))
        self.assertEqual(self.store.list(self.workspace)["items"], [])
        with self.assertRaises(NotFound):
            self.store.create(
                self.workspace, self.workflow["id"], 999, interval(), "bad", now=0
            )

    def test_round_robin_head_prevents_backlog_starvation(self):
        schedules = [self.create(key=str(i)) for i in range(6)]
        visited = []
        for i in range(6):
            result = self.dispatch(str(i), now=60000, limit=20)
            self.assertEqual(result["outcome_count"], 20)
            self.assertEqual(len(result["schedules"]), 1)
            visited.append(result["schedules"][0]["schedule_id"])
        self.assertEqual(set(visited), {s["id"] for s in schedules})
        for schedule in schedules:
            self.proof(schedule)

    def test_same_dispatch_key_concurrently_returns_one_identical_receipt(self):
        schedule = self.create()

        def tick(_):
            return ScheduleStore(ControlStore(self.path)).dispatch(
                self.workspace, "same", now=300
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(tick, range(4)))
        self.assertTrue(all(value == results[0] for value in results))
        self.assertEqual(len(self.proof(schedule)["runs"]), 4)

    def test_receipt_round_robin_head_is_not_advanced_by_replay(self):
        for i in range(3):
            self.create(key=str(i))
        first = self.dispatch("one", now=60000, limit=20)
        self.assertEqual(first, self.dispatch("one", now=60000, limit=20))
        second = self.dispatch("two", now=60000, limit=20)
        self.assertNotEqual(
            first["schedules"][0]["schedule_id"], second["schedules"][0]["schedule_id"]
        )

