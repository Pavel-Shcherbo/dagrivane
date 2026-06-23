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

