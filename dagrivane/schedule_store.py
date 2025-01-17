"""Workspace-scoped retained schedules, durable cursors and append-only decisions."""

import json
import uuid

from .control import Conflict, ControlStore, NotFound, canonical, limit_value, text
from .schedule_clock import instant
from .schedule_model import compile_schedule, digest

SCHEMA = """
CREATE TABLE IF NOT EXISTS cp_timed_schedules(
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
 workflow_id TEXT NOT NULL, version INTEGER NOT NULL, request_key TEXT NOT NULL,
 submission_hash TEXT NOT NULL, definition TEXT NOT NULL, plan_sha256 TEXT NOT NULL,
 next_due INTEGER, enabled INTEGER NOT NULL, generation INTEGER NOT NULL,
 created INTEGER NOT NULL,
 UNIQUE(workspace_id,request_key),
 FOREIGN KEY(workflow_id,version) REFERENCES cp_versions(workflow_id,version)
);
CREATE INDEX IF NOT EXISTS cp_timed_due ON cp_timed_schedules(workspace_id,enabled,next_due,id);
CREATE TABLE IF NOT EXISTS cp_timed_events(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 schedule_id TEXT NOT NULL REFERENCES cp_timed_schedules(id), kind TEXT NOT NULL,
 payload TEXT NOT NULL, previous_sha256 TEXT NOT NULL, sha256 TEXT NOT NULL, created INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_timed_history ON cp_timed_events(schedule_id,sequence);
CREATE TABLE IF NOT EXISTS cp_timed_occurrences(
 schedule_id TEXT NOT NULL REFERENCES cp_timed_schedules(id), due INTEGER NOT NULL,
 outcome TEXT NOT NULL, run_id TEXT REFERENCES cp_runs(id), created INTEGER NOT NULL,
 PRIMARY KEY(schedule_id,due)
);
CREATE INDEX IF NOT EXISTS cp_timed_run ON cp_timed_occurrences(run_id);
CREATE TABLE IF NOT EXISTS cp_timed_dispatch_heads(
 workspace_id TEXT PRIMARY KEY REFERENCES cp_workspaces(id),
 last_schedule TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_timed_receipts(
 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id), request_key TEXT NOT NULL,
 request_sha256 TEXT NOT NULL, result TEXT NOT NULL, created INTEGER NOT NULL,
 PRIMARY KEY(workspace_id,request_key)
);
"""


def expected_generation(value):
    if type(value) is not int or value < 1:
        raise ValueError("expected generation must be a positive integer")
    return value


def event(connection, schedule_id, kind, payload, now):
    prior = connection.execute(
        "SELECT sha256 FROM cp_timed_events WHERE schedule_id=? ORDER BY sequence DESC LIMIT 1",
        (schedule_id,),
    ).fetchone()
    parent = prior[0] if prior else digest([])
    content = {
        "schedule_id": schedule_id,
        "kind": kind,
        "payload": payload,
        "previous_sha256": parent,
        "created": now,
    }
    sha = digest(content)
    connection.execute(
        "INSERT INTO cp_timed_events(schedule_id,kind,payload,previous_sha256,sha256,created) VALUES(?,?,?,?,?,?)",
        (schedule_id, kind, canonical(payload), parent, sha, now),
    )
    return sha


def record(row):
    value = dict(row)
    if type(value["enabled"]) is not int or value["enabled"] not in (0, 1):
        raise Conflict("retained schedule enabled flag is invalid")
    value["definition"] = json.loads(value["definition"])
    value["enabled"] = bool(value["enabled"])
    return value


class ScheduleStore:
    def __init__(self, control):
        if not isinstance(control, ControlStore):
            raise ValueError("scheduler requires the durable workflow control store")
        self.control = control
        with control.transaction() as connection:
            for sql in SCHEMA.split(";"):
                if sql.strip():
                    connection.execute(sql)

