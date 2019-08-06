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

    @contextmanager
    def reading(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def event(connection, run_id, kind, payload, now):
        connection.execute(
            "INSERT INTO cp_events(run_id,kind,payload,created) VALUES (?,?,?,?)",
            (run_id, kind, canonical(payload), now),
        )

    @staticmethod
    def workspace(connection, workspace_id):
        row = connection.execute(
            "SELECT * FROM cp_workspaces WHERE id=?", (workspace_id,)
        ).fetchone()
        if row is None:
            raise NotFound("workspace not found")
        return row

    def create_workspace(self, name, concurrency=2, now=None):
        text(name, "workspace name")
        if type(concurrency) is not int or not 1 <= concurrency <= 100:
            raise ValueError("workspace concurrency must be between 1 and 100")
        now = timestamp(now)
        workspace_id = str(uuid.uuid4())
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO cp_workspaces VALUES (?,?,?,?)",
                (workspace_id, name, concurrency, now),
            )
        return {
            "id": workspace_id,
            "name": name,
            "concurrency": concurrency,
            "created": now,
        }

    def list_workspaces(self):
        with self.reading() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM cp_workspaces ORDER BY created,id"
                )
            ]

    def register_workflow(self, workspace_id, name, specification, directory, now=None):
        text(name, "workflow name")
        validate(specification)
        content = canonical(specification)
        directory = str(Path(directory).resolve())
        if not Path(directory).is_dir():
            raise ValueError("workflow directory must exist")
        fingerprint = hashlib.sha256(
            canonical({"specification": specification, "directory": directory}).encode()
        ).hexdigest()
        now = timestamp(now)
        with self.transaction() as connection:
            self.workspace(connection, workspace_id)
            workflow = connection.execute(
                "SELECT id FROM cp_workflows WHERE workspace_id=? AND name=?",
                (workspace_id, name),
            ).fetchone()
            if workflow is None:
                workflow_id = str(uuid.uuid4())
                connection.execute(
                    "INSERT INTO cp_workflows VALUES (?,?,?,?)",
                    (workflow_id, workspace_id, name, now),
                )
                version = 1
            else:
                workflow_id = workflow["id"]
                latest = connection.execute(
                    "SELECT * FROM cp_versions WHERE workflow_id=? ORDER BY version DESC LIMIT 1",
                    (workflow_id,),
                ).fetchone()
                if latest["fingerprint"] == fingerprint:
                    return {
                        "id": workflow_id,
                        "name": name,
                        "version": latest["version"],
                        "fingerprint": fingerprint,
                        "changed": False,
                    }
                version = latest["version"] + 1
            connection.execute(
                "INSERT INTO cp_versions VALUES (?,?,?,?,?,?)",
                (workflow_id, version, content, fingerprint, directory, now),
            )
        return {
            "id": workflow_id,
            "name": name,
            "version": version,
            "fingerprint": fingerprint,
            "changed": True,
        }

    @staticmethod
    def version_record(connection, workspace_id, workflow_id, version):
        if type(version) is not int or version < 1:
            raise ValueError("workflow version must be a positive integer")
        row = connection.execute(
            "SELECT v.*,w.name,w.workspace_id FROM cp_versions v JOIN cp_workflows w ON w.id=v.workflow_id WHERE w.workspace_id=? AND v.workflow_id=? AND v.version=?",
            (workspace_id, workflow_id, version),
        ).fetchone()
        if row is None:
            raise NotFound("workflow version not found in this workspace")
        return row

    def workflow(self, workspace_id, workflow_id, version):
        with self.reading() as connection:
            row = dict(
                self.version_record(connection, workspace_id, workflow_id, version)
            )
            row["specification"] = json.loads(row["specification"])
            return row

    def list_workflows(self, workspace_id):
        with self.reading() as connection:
            self.workspace(connection, workspace_id)
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT w.id,w.name,w.created,MAX(v.version) AS version FROM cp_workflows w JOIN cp_versions v ON v.workflow_id=w.id WHERE w.workspace_id=? GROUP BY w.id ORDER BY w.name,w.id",
                    (workspace_id,),
                )
            ]

    def _enqueue(
        self,
        connection,
        workspace_id,
        workflow_id,
        version,
        event_key,
        priority,
        now,
        retry_of=None,
    ):
        self.version_record(connection, workspace_id, workflow_id, version)
        text(event_key, "event key", 500)
        if type(priority) is not int or not -100 <= priority <= 100:
            raise ValueError("priority must be an integer between -100 and 100")
        identity = hashlib.sha256(
            canonical(
                {
                    "workflow": workflow_id,
                    "version": version,
                    "priority": priority,
                    "retry_of": retry_of,
                }
            ).encode()
        ).hexdigest()
        prior = connection.execute(
            "SELECT * FROM cp_runs WHERE workspace_id=? AND event_key=?",
            (workspace_id, event_key),
        ).fetchone()
        if prior is not None:
            if prior["submission_hash"] != identity:
                raise Conflict("event key was already used for a different submission")
            return {"id": prior["id"], "status": prior["status"], "created": False}
        run_id = str(uuid.uuid4())
        connection.execute(
            "INSERT INTO cp_runs(id,workspace_id,workflow_id,version,event_key,submission_hash,status,priority,created,retry_of) VALUES (?,?,?,?,?,?,'queued',?,?,?)",
            (
                run_id,
                workspace_id,
                workflow_id,
                version,
                event_key,
                identity,
                priority,
                now,
                retry_of,
            ),
        )
        self.event(
            connection,
            run_id,
            "submitted",
            {"version": version, "priority": priority, "retry_of": retry_of},
            now,
        )
        return {"id": run_id, "status": "queued", "created": True}

    def enqueue(
        self, workspace_id, workflow_id, version, event_key, priority=0, now=None
    ):
        now = timestamp(now)
        with self.transaction() as connection:
            return self._enqueue(
                connection, workspace_id, workflow_id, version, event_key, priority, now
            )

    @staticmethod
    def run_record(connection, workspace_id, run_id):
        row = connection.execute(
            "SELECT * FROM cp_runs WHERE workspace_id=? AND id=?",
            (workspace_id, run_id),
        ).fetchone()
        if row is None:
            raise NotFound("run not found in this workspace")
        return row

    @staticmethod
    def public_run(row):
        data = dict(row)
        data.pop("lease_token", None)
        data.pop("submission_hash", None)
        if data["result"] is not None:
            data["result"] = json.loads(data["result"])
        return data

    def get_run(self, workspace_id, run_id):
        with self.reading() as connection:
            return self.public_run(self.run_record(connection, workspace_id, run_id))

    def list_runs(self, workspace_id, status=None, limit=100):
        limit_value(limit)
        if status is not None and status not in STATES:
            raise ValueError("unknown run status")
        with self.reading() as connection:
            self.workspace(connection, workspace_id)
            if status is None:
                rows = connection.execute(
                    "SELECT * FROM cp_runs WHERE workspace_id=? ORDER BY created DESC,id DESC LIMIT ?",
                    (workspace_id, limit),
                )
            else:
                rows = connection.execute(
                    "SELECT * FROM cp_runs WHERE workspace_id=? AND status=? ORDER BY created DESC,id DESC LIMIT ?",
                    (workspace_id, status, limit),
                )
            return [self.public_run(row) for row in rows]

    @staticmethod
    def lease_duration(seconds):
        if (
            type(seconds) not in (int, float)
            or not math.isfinite(seconds)
            or not 5 <= seconds <= 3600
        ):
            raise ValueError("lease duration must be between 5 and 3600 seconds")
        return seconds

    def claim(self, worker, lease_seconds=30, now=None):
        text(worker, "worker")
        self.lease_duration(lease_seconds)
        now = timestamp(now)
        with self.transaction() as connection:
            expired = connection.execute(
                "SELECT id FROM cp_runs WHERE status IN ('running','cancel_requested') AND lease_until<=?",
                (now,),
            ).fetchall()
            for row in expired:
                connection.execute(
                    "UPDATE cp_runs SET status='orphaned',finished=?,lease_token=NULL WHERE id=?",
                    (now, row["id"]),
                )
                self.event(
                    connection,
                    row["id"],
                    "lease_expired",
                    {"reconciliation_required": True},
                    now,
                )
            row = connection.execute(
                """SELECT r.* FROM cp_runs r JOIN cp_workspaces w ON w.id=r.workspace_id
                WHERE r.status='queued' AND
                (SELECT COUNT(*) FROM cp_runs active WHERE active.workspace_id=r.workspace_id AND active.status IN ('running','cancel_requested')) < w.concurrency
                ORDER BY r.priority DESC,r.created,r.id LIMIT 1"""
            ).fetchone()
            if row is None:
                return None
            token = str(uuid.uuid4())
            connection.execute(
                "UPDATE cp_runs SET status='running',started=?,worker=?,lease_token=?,lease_until=? WHERE id=?",
                (now, worker, token, now + lease_seconds, row["id"]),
            )
            self.event(
                connection,
                row["id"],
                "claimed",
                {"worker": worker, "lease_until": now + lease_seconds},
                now,
            )
            claimed = dict(
                connection.execute(
                    "SELECT * FROM cp_runs WHERE id=?", (row["id"],)
                ).fetchone()
            )
            version = self.version_record(
                connection,
                claimed["workspace_id"],
                claimed["workflow_id"],
                claimed["version"],
            )
            claimed["specification"] = json.loads(version["specification"])
            claimed["directory"] = version["directory"]
            return claimed

    @staticmethod
    def owned(connection, run_id, worker, token, now):
        row = connection.execute(
            "SELECT * FROM cp_runs WHERE id=?", (run_id,)
        ).fetchone()
        if (
            row is None
            or row["worker"] != worker
            or row["lease_token"] != token
            or row["status"] not in {"running", "cancel_requested"}
            or row["lease_until"] <= now
        ):
            raise LostLease("worker no longer owns this run")
        return row

    def heartbeat(self, run_id, worker, token, lease_seconds=30, now=None):
        self.lease_duration(lease_seconds)
        now = timestamp(now)
        with self.transaction() as connection:
            row = self.owned(connection, run_id, worker, token, now)
            connection.execute(
                "UPDATE cp_runs SET lease_until=? WHERE id=?",
                (now + lease_seconds, run_id),
            )
            return {
                "lease_until": now + lease_seconds,
                "cancel_requested": row["status"] == "cancel_requested",
            }

    def append_event(self, run_id, worker, token, kind, payload, now=None):
        text(kind, "event kind", 100)
        now = timestamp(now)
        with self.transaction() as connection:
            self.owned(connection, run_id, worker, token, now)
            self.event(connection, run_id, kind, payload, now)

    def finish(self, run_id, worker, token, status, result, now=None):
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("worker may finish as succeeded, failed or cancelled")
        payload = canonical(result)
        now = timestamp(now)
        with self.transaction() as connection:
            row = self.owned(connection, run_id, worker, token, now)
            if row["status"] == "cancel_requested" and status != "cancelled":
                raise Conflict(
                    "cancellation was requested; reconcile effects before finishing as cancelled"
                )
            connection.execute(
                "UPDATE cp_runs SET status=?,finished=?,result=?,lease_token=NULL WHERE id=?",
                (status, now, payload, run_id),
            )
            self.event(connection, run_id, "finished", {"status": status}, now)

    def cancel(self, workspace_id, run_id, now=None):
        now = timestamp(now)
        with self.transaction() as connection:
            row = self.run_record(connection, workspace_id, run_id)
            if row["status"] in TERMINAL:
                raise Conflict("terminal runs cannot be cancelled")
            status = "cancelled" if row["status"] == "queued" else "cancel_requested"
            connection.execute(
                "UPDATE cp_runs SET status=?,finished=? WHERE id=?",
                (status, now if status == "cancelled" else None, run_id),
            )
            if status != row["status"]:
                self.event(
                    connection,
                    run_id,
                    "cancelled" if status == "cancelled" else "cancellation_requested",
                    {},
                    now,
                )
            return {"id": run_id, "status": status}

