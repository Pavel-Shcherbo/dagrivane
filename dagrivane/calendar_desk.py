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

    @staticmethod
    def checked_version(control, workspace, workflow, version):
        row = control.workflow(workspace, workflow, version)
        if (
            digest(
                {"specification": row["specification"], "directory": row["directory"]}
            )
            != row["fingerprint"]
        ):
            raise ValueError("saved workflow content differs from its pinned identity")
        return row

    @staticmethod
    def template(now):
        return {
            "version": 1,
            "name": "Orders every five minutes",
            "trigger": {"kind": "interval", "seconds": 300},
            "start_at": iso((now // 60 + 1) * 60),
            "end_at": None,
            "misfire": {"policy": "coalesce", "max_lateness_seconds": 86400},
            "overlap": "defer",
            "priority": 0,
        }

    @staticmethod
    def receipt(row):
        result = json.loads(bytes(row["result"]))
        if digest(result) != row["result_sha256"]:
            raise ValueError("saved calendar response identity changed")
        return bounded(result)

    def dispatch(self, params):
        if not isinstance(params, dict):
            raise ValueError("calendar request requires an object")
        if params.get("mode") != "calendar":
            raise ValueError("calendar request requires mode calendar")
        action = params.get("action")
        if action == "initialize":
            fields(params, ("mode", "action"))
            operations = Operations(self.root)
            initialized = operations.initialize()
            return {
                "workspace_id": initialized["workspace"]["id"],
                "workflow_id": initialized["workflow"]["id"],
            }
        if action == "preview":
            fields(params, ("mode", "action", "definition"), ("after", "limit"))
            return bounded(
                preview_schedule(
                    params["definition"],
                    after=params.get("after"),
                    limit=params.get("limit", 8),
                )
            )
        if action == "overview" and not self.database.is_file():
            fields(params, ("mode", "action"), ("after", "limit"))
            now = instant(int(time.time()))
            return {
                "initialized": False,
                "workspaces": [],
                "has_more": False,
                "next_cursor": None,
                "now": now,
                "template": self.template(now),
            }
        control = self.control()
        read_actions = (
            "overview",
            "workspace",
            "workflows",
            "version",
            "schedules",
            "view",
            "run",
            "receipt",
        )
        if action in read_actions:
            with control.reading() as db:
                db.execute("BEGIN")
                bound = BoundControl(control, db)
                return bounded(self.read(bound, BoundSchedules(bound), params))
        workspace = identity(params.get("workspace"), "workspace")
        request_key = text(params.get("request_key"), "calendar request key", 200)
        request_sha = digest(params)
        with control.transaction() as db:
            control.workspace(db, workspace)
            prior = db.execute(
                "SELECT * FROM wb_calendar_receipts WHERE workspace_id=? AND request_key=?",
                (workspace, request_key),
            ).fetchone()
            if prior:
                if prior["request_sha256"] != request_sha:
                    raise ValueError("calendar request key identifies different inputs")
                return self.receipt(prior)
            count, size = db.execute(
                "SELECT count(*),COALESCE(sum(length(result)),0) FROM wb_calendar_receipts"
            ).fetchone()
            if count >= 10000:
                raise ValueError("calendar desk reached 10,000 retained responses")
            bound = BoundControl(control, db)
            result = bounded(self.operate(bound, BoundSchedules(bound), params))
            raw = canonical(result).encode()
            if size + len(raw) > 256 * 1024 * 1024:
                raise ValueError("retained calendar responses exceed 256 MiB")
            db.execute(
                "INSERT INTO wb_calendar_receipts VALUES(?,?,?,?,?,?)",
                (
                    workspace,
                    request_key,
                    request_sha,
                    raw,
                    digest(result),
                    int(time.time()),
                ),
            )
            return result

    def read(self, control, schedules, params):
        action = params["action"]
        base = ("mode", "action")
        if action == "overview":
            fields(params, base, ("after", "limit"))
            after = params.get("after", "")
            if after:
                identity(after, "workspace cursor")
            elif not isinstance(after, str):
                raise ValueError("workspace cursor must be text")
            limit = page_size(params.get("limit", 100))
            rows = control.db.execute(
                "SELECT * FROM cp_workspaces WHERE id>? ORDER BY id LIMIT ?",
                (after, limit + 1),
            ).fetchall()
            now = instant(int(time.time()))
            return {
                "initialized": True,
                "workspaces": [dict(row) for row in rows[:limit]],
                "has_more": len(rows) > limit,
                "next_cursor": rows[limit - 1]["id"] if len(rows) > limit else None,
                "now": now,
                "template": self.template(now),
            }
        workspace = identity(params.get("workspace"), "workspace")
        control.workspace(control.db, workspace)
        if action == "workspace":
            fields(params, (*base, "workspace"))
            return dict(control.workspace(control.db, workspace))
        if action == "workflows":
            fields(params, (*base, "workspace"), ("after", "limit"))
            after = params.get("after", "")
            if after:
                identity(after, "workflow cursor")
            elif not isinstance(after, str):
                raise ValueError("workflow cursor must be text")
            limit = page_size(params.get("limit", 100))
            rows = control.db.execute(
                "SELECT w.id,w.name,MAX(v.version) AS version FROM cp_workflows w JOIN cp_versions v ON v.workflow_id=w.id WHERE w.workspace_id=? AND w.id>? GROUP BY w.id ORDER BY w.id LIMIT ?",
                (workspace, after, limit + 1),
            ).fetchall()
            return {
                "items": [dict(row) for row in rows[:limit]],
                "has_more": len(rows) > limit,
                "next_cursor": rows[limit - 1]["id"] if len(rows) > limit else None,
            }
        if action == "version":
            fields(params, (*base, "workspace", "workflow", "version"))
            row = self.checked_version(
                control,
                workspace,
                identity(params["workflow"], "workflow"),
                params["version"],
            )
            return {
                "workflow": row["workflow_id"],
                "name": row["name"],
                "version": row["version"],
                "fingerprint": row["fingerprint"],
                "specification": row["specification"],
                "directory": row["directory"],
            }
        if action == "schedules":
            fields(params, (*base, "workspace"), ("after", "limit"))
            return schedules.list(
                workspace,
                after=params.get("after", ""),
                limit=page_size(params.get("limit", 100)),
            )
        if action == "receipt":
            fields(params, (*base, "workspace", "request_key"))
            request_key = text(params["request_key"], "calendar request key", 200)
            row = control.db.execute(
                "SELECT * FROM wb_calendar_receipts WHERE workspace_id=? AND request_key=?",
                (workspace, request_key),
            ).fetchone()
            if row is None:
                raise ValueError("calendar response not found in this workspace")
            return self.receipt(row)
        if action == "run":
            fields(params, (*base, "workspace", "schedule", "run"))
            schedule = identity(params["schedule"], "schedule")
            schedules.row(control.db, workspace, schedule)
            run = identity(params["run"], "run")
            if (
                control.db.execute(
                    "SELECT run_id FROM cp_timed_occurrences WHERE schedule_id=? AND run_id=?",
                    (schedule, run),
                ).fetchone()
                is None
            ):
                raise ValueError("run does not belong to this schedule")
            return control.get_run(workspace, run)
        fields(
            params,
            (*base, "workspace", "schedule"),
            ("event_after", "due_after", "limit"),
        )
        schedule = identity(params["schedule"], "schedule")
        value = schedules.inspect(workspace, schedule)
        limit = page_size(params.get("limit", 25))
        events = schedules.events(
            workspace, schedule, after=params.get("event_after", 0), limit=limit
        )
        occurrences = schedules.occurrences(
            workspace, schedule, after=params.get("due_after", -1), limit=limit
        )
        try:
            proof = verify_schedule_evidence(
                export_schedule(schedules, workspace, schedule)
            )
            audit = {**proof, "error": None}
        except (ValueError, ControlError) as error:
            audit = {"passed": False, "complete": False, "error": str(error)}
        preview = (
            preview_schedule(value["definition"], after=value["next_due"], limit=8)
            if value["next_due"] is not None
            else {"items": [], "has_more": False, "next_cursor": None}
        )
        return {
            "schedule": value,
            "workflow": self.checked_version(
                control, workspace, value["workflow_id"], value["version"]
            ),
            "events": events,
            "occurrences": occurrences,
            "preview": preview,
            "audit": audit,
            "observed_at": instant(int(time.time())),
        }

    @staticmethod
    def operate(control, schedules, params):
        base = ("mode", "action", "workspace", "request_key")
        action = params["action"]
        workspace = params["workspace"]
        if action == "create":
            fields(
                params,
                (
                    *base,
                    "workflow",
                    "version",
                    "workflow_sha256",
                    "definition",
                    "preview_sha256",
                    "now",
                ),
            )
            workflow = identity(params["workflow"], "workflow")
            version = CalendarDesk.checked_version(
                control, workspace, workflow, params["version"]
            )
            if version["fingerprint"] != params["workflow_sha256"]:
                raise ValueError("workflow version differs from the reviewed selection")
            plan = compile_schedule(params["definition"])
            if plan.sha256 != params["preview_sha256"]:
                raise ValueError(
                    "schedule definition differs from the reviewed preview"
                )
            return schedules.create(
                workspace,
                workflow,
                params["version"],
                plan.definition,
                params["request_key"],
                now=params["now"],
            )
        if action == "set-enabled":
            fields(
                params, (*base, "schedule", "enabled", "generation", "reason", "now")
            )
            return schedules.set_enabled(
                workspace,
                identity(params["schedule"], "schedule"),
                params["enabled"],
                params["generation"],
                params["reason"],
                now=params["now"],
            )
        if action == "dispatch":
            fields(params, (*base, "now", "limit"))
            now = instant(params["now"])
            for row in control.db.execute(
                "SELECT DISTINCT workflow_id,version FROM cp_timed_schedules WHERE workspace_id=? AND enabled=1 AND next_due<=?",
                (workspace, now),
            ):
                CalendarDesk.checked_version(
                    control, workspace, row["workflow_id"], row["version"]
                )
            return schedules.dispatch(
                workspace,
                "calendar:" + params["request_key"],
                now=params["now"],
                limit=page_size(params["limit"]),
            )
        if action == "export":
            fields(params, (*base, "schedule"))
            document = export_schedule(
                schedules, workspace, identity(params["schedule"], "schedule")
            )
            return {
                "content": canonical(document) + "\n",
                "schedule": {
                    "id": document["schedule"]["id"],
                    "generation": document["schedule"]["generation"],
                },
            }
        raise ValueError("unknown calendar action")


