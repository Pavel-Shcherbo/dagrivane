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

