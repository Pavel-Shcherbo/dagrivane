"""Coherent calendar views and input-bound local operator response receipts."""

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
import uuid

from .control import ControlError, ControlStore, canonical, text
from .operations import Operations
from .schedule_clock import instant, iso
from .schedule_evidence import export_schedule, verify_schedule_evidence
from .schedule_model import compile_schedule, digest, preview_schedule
from .schedule_store import ScheduleStore, record

MAX_RESPONSE = 16 * 1024 * 1024


def fields(value, required, optional=()):
    if (
        not isinstance(value, dict)
        or not set(required) <= set(value)
        or set(value) - set(required) - set(optional)
    ):
        raise ValueError("calendar request has missing or unsupported fields")


def identity(value, label):
    try:
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise ValueError()
    except (ValueError, AttributeError):
        raise ValueError(label + " requires a canonical UUID") from None
    return value


def page_size(value):
    if type(value) is not int or not 1 <= value <= 100:
        raise ValueError("calendar page size must be 1–100")
    return value


def bounded(value):
    if len(json.dumps(value, allow_nan=False).encode()) > MAX_RESPONSE:
        raise ValueError(
            "calendar response exceeds 16 MiB; use the CLI for large evidence"
        )
    return value


class BoundControl(ControlStore):
    def __init__(self, control, db):
        self.path, self.db = control.path, db

    @contextmanager
    def transaction(self):
        yield self.db

    @contextmanager
    def reading(self):
        yield self.db


class BoundSchedules(ScheduleStore):
    def __init__(self, control):
        self.control = control


class CalendarDesk:
    def __init__(self, root):
        self.root = Path(root)
        self.database = self.root / "control.sqlite"

    def control(self):
        if not self.database.is_file():
            raise ValueError("initialize a workspace before managing schedules")
        control = ControlStore(self.database)
        ScheduleStore(control)
        with control.transaction() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS wb_calendar_receipts(workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),request_key TEXT NOT NULL,request_sha256 TEXT NOT NULL,result BLOB NOT NULL,result_sha256 TEXT NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(workspace_id,request_key))"
            )
        return control

