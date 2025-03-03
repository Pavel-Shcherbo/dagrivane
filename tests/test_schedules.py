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

