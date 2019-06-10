"""Versioned workflows, durable run submission and fenced worker ownership."""

import hashlib
import json
import math
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .engine import validate


class ControlError(Exception):
    """A rejected control-plane operation."""


class NotFound(ControlError):
    pass


class Conflict(ControlError):
    pass


class LostLease(ControlError):
    pass


STATES = {
    "queued",
    "running",
    "cancel_requested",
    "cancelled",
    "succeeded",
    "failed",
    "orphaned",
}
TERMINAL = {"cancelled", "succeeded", "failed", "orphaned"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def timestamp(now):
    value = time.time() if now is None else now
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("timestamp must be a finite nonnegative number")
    return value


def text(value, field, maximum=200):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{field} must contain 1 to {maximum} characters")
    return value


def limit_value(value):
    if type(value) is not int or not 1 <= value <= 500:
        raise ValueError("limit must be between 1 and 500")
    return value


SCHEMA = """
CREATE TABLE cp_workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    concurrency INTEGER NOT NULL CHECK (concurrency > 0),
    created REAL NOT NULL
);
CREATE TABLE cp_workflows (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
    name TEXT NOT NULL,
    created REAL NOT NULL,
    UNIQUE (workspace_id, name)
);
CREATE TABLE cp_versions (
    workflow_id TEXT NOT NULL REFERENCES cp_workflows(id),
    version INTEGER NOT NULL,
    specification TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    directory TEXT NOT NULL,
    created REAL NOT NULL,
    PRIMARY KEY (workflow_id, version)
);
CREATE TABLE cp_runs (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
    workflow_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    event_key TEXT NOT NULL,
    submission_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    created REAL NOT NULL,
    started REAL,
    finished REAL,
    worker TEXT,
    lease_token TEXT,
    lease_until REAL,
    result TEXT,
    retry_of TEXT REFERENCES cp_runs(id),
    UNIQUE (workspace_id, event_key),
    FOREIGN KEY (workflow_id, version) REFERENCES cp_versions(workflow_id, version)
);
CREATE INDEX cp_runs_queue ON cp_runs(status, priority DESC, created);
CREATE INDEX cp_runs_workspace ON cp_runs(workspace_id, status);
CREATE TABLE cp_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES cp_runs(id),
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    created REAL NOT NULL
);
CREATE INDEX cp_events_run ON cp_events(run_id, sequence);
CREATE TABLE cp_schedules (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    interval_seconds INTEGER NOT NULL,
    next_due REAL NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (workflow_id, version) REFERENCES cp_versions(workflow_id, version)
);
"""


class ControlStore:
    """SQLite control state. Workspaces partition records, not operating-system access."""

    def __init__(self, path):
        self.path = Path(path)
        if str(self.path) == ":memory:":
            raise ValueError("control state requires a durable database file")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        connection = self._connect()
        try:
            self._enable_wal(connection)
            connection.execute("BEGIN IMMEDIATE")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version > 1:
                raise Conflict("control database requires a newer application version")
            if version == 0:
                for statement in SCHEMA.split(";"):
                    if statement.strip():
                        connection.execute(statement)
                connection.execute("PRAGMA user_version=1")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _enable_wal(connection):
        deadline = time.monotonic() + 10
        while True:
            try:
                mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                if mode != "wal":
                    mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode != "wal":
                    raise Conflict("control database could not enter WAL mode")
                return
            except sqlite3.OperationalError as error:
                locked = "locked" in str(error).lower() or "busy" in str(error).lower()
                if not locked or time.monotonic() >= deadline:
                    raise
                time.sleep(0.02)

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextmanager
    def transaction(self):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

