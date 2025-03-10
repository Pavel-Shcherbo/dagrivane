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

