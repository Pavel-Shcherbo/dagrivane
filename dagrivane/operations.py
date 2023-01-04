"""Durable local order-workflow operations over reviewed installation templates."""

import copy
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

from .catalog_contracts import table_bundle
from .control import ControlStore, Conflict, canonical
from .quality import compile_gates
from .resources import examples_directory
from .worker import Worker

PROFILES = {
    "standard": "Order checks",
    "quality-failure": "Quality failure control",
    "cancellation": "Cancellation control",
}


class Operations:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.database = self.root / "control.sqlite"
        self.worker_root = self.root / "worker"

    def _reviewed(self, prepare=False):
        examples = examples_directory()
        files = {}
        for name in ["orders.csv", "release_typed.py"]:
            with (examples / name).open("rb") as stream:
                raw = stream.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError("reviewed input exceeds its byte budget")
            files[name] = raw
        base = json.loads((examples / "typed-pipeline.json").read_text())
        base["execution_policy"] = "isolated_inputs"
        base["inputs"] = [
            {"path": name, "sha256": hashlib.sha256(raw).hexdigest()}
            for name, raw in sorted(files.items())
        ]
        for task in base["tasks"]:
            if "command" in task and task["command"][0] == "python3":
                task["command"][0] = sys.executable
        source_id = hashlib.sha256(
            canonical({"specification": base, "inputs": base["inputs"]}).encode()
        ).hexdigest()
        directory = self.root / "templates" / source_id
        if directory.is_symlink():
            raise ValueError("reviewed template cannot be a symlink")
        if prepare:
            directory.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not directory.exists():
                temporary = Path(
                    tempfile.mkdtemp(prefix=".inputs-", dir=directory.parent)
                )
                try:
                    for name, raw in files.items():
                        (temporary / name).write_bytes(raw)
                    (temporary / "manifest.json").write_text(canonical(base["inputs"]))
                    try:
                        os.rename(temporary, directory)
                    except OSError:
                        if not directory.is_dir() or directory.is_symlink():
                            raise
                finally:
                    shutil.rmtree(temporary, ignore_errors=True)
            for name, raw in files.items():
                path = directory / name
                if path.is_symlink() or not path.is_file() or path.read_bytes() != raw:
                    raise ValueError("reviewed template content changed")
        result = {}
        for profile, label in PROFILES.items():
            spec = copy.deepcopy(base)
            if profile == "quality-failure":
                spec["tasks"][0]["dataflow"]["gates"][1].update(min=5, max=5)
            elif profile == "cancellation":
                spec["tasks"].insert(
                    0,
                    {
                        "id": "pause",
                        "command": [sys.executable, "-c", "import time; time.sleep(4)"],
                        "timeout": 10,
                    },
                )
                spec["tasks"][1]["needs"] = ["pause"]
            fingerprint = hashlib.sha256(
                canonical({"specification": spec, "directory": str(directory)}).encode()
            ).hexdigest()
            result[profile] = {
                "profile": profile,
                "label": label,
                "specification": spec,
                "directory": str(directory),
                "fingerprint": fingerprint,
            }
        return result

    def _context(self, required=True):
        if not self.database.is_file():
            if required:
                raise ValueError("create the local operations workspace first")
            return None, None
        store = ControlStore(self.database)
        with store.reading() as db:
            exists = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='wb_operations'"
            ).fetchone()
            row = (
                db.execute("SELECT * FROM wb_operations WHERE singleton=1").fetchone()
                if exists
                else None
            )
            if row is None:
                if required:
                    raise ValueError("create the local operations workspace first")
                return store, None
            if row["schema_version"] != 1:
                raise ValueError("unsupported operations workspace schema")
            return store, dict(row)

    def initialize(self):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        reviewed = self._reviewed(prepare=True)
        store = ControlStore(self.database)
        with store.transaction() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS wb_operations(singleton INTEGER PRIMARY KEY,schema_version INTEGER NOT NULL,workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),workflow_id TEXT NOT NULL REFERENCES cp_workflows(id))"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS wb_versions(version INTEGER PRIMARY KEY,profile TEXT NOT NULL,fingerprint TEXT NOT NULL)"
            )
            current = db.execute(
                "SELECT * FROM wb_operations WHERE singleton=1"
            ).fetchone()
            if current is not None:
                if current["schema_version"] != 1:
                    raise ValueError("unsupported operations workspace schema")
            else:
                workspace = store._create_workspace(db, "Local orders", concurrency=2)
                template = reviewed["standard"]
                workflow = store._register_workflow(
                    db,
                    workspace["id"],
                    "Order checks",
                    template["specification"],
                    template["directory"],
                )
                db.execute(
                    "INSERT INTO wb_operations VALUES(1,1,?,?)",
                    (workspace["id"], workflow["id"]),
                )
                db.execute(
                    "INSERT INTO wb_versions VALUES(?,?,?)",
                    (workflow["version"], "standard", workflow["fingerprint"]),
                )
        return self.overview()

    def _version(self, store, meta, version, reviewed=None):
        record = store.workflow(meta["workspace_id"], meta["workflow_id"], version)
        with store.reading() as db:
            mapping = db.execute(
                "SELECT * FROM wb_versions WHERE version=?", (version,)
            ).fetchone()
        templates = reviewed if reviewed is not None else self._reviewed()
        profile = mapping["profile"] if mapping else None
        template = templates.get(profile)
        approved = bool(
            template
            and mapping["fingerprint"]
            == record["fingerprint"]
            == template["fingerprint"]
            and record["specification"] == template["specification"]
            and record["directory"] == template["directory"]
        )
        spec = record["specification"]
        return {
            "version": version,
            "profile": profile,
            "label": PROFILES.get(profile, "Unreviewed workflow"),
            "fingerprint": record["fingerprint"],
            "approved": approved,
            "created": record["created"],
            "tasks": [
                {
                    "id": task["id"],
                    "needs": task.get("needs", []),
                    "kind": "dataflow" if "dataflow" in task else "command",
                }
                for task in spec["tasks"]
            ],
            "inputs": spec.get("inputs", []),
        }, record

    def overview(self, *, before=None, status=None):
        store, meta = self._context(required=False)
        if meta is None:
            return {
                "initialized": False,
                "profiles": [
                    {"id": key, "label": label} for key, label in PROFILES.items()
                ],
            }
        reviewed = self._reviewed()
        page = store.version_page(meta["workspace_id"], meta["workflow_id"], limit=100)
        versions = [
            self._version(store, meta, item["version"], reviewed)[0]
            for item in page["versions"]
        ]
        queue = store.run_page(
            meta["workspace_id"], before=before, status=status, limit=25
        )
        return {
            "initialized": True,
            "workspace": {
                "id": meta["workspace_id"],
                "name": "Local orders",
                "concurrency": 2,
            },
            "workflow": {"id": meta["workflow_id"], "name": "Order checks"},
            "profiles": [
                {"id": key, "label": label} for key, label in PROFILES.items()
            ],
            "versions": versions,
            **queue,
            "limits": {"versions": 100, "runs": 1000, "page": 25},
            "execution_scope": "Reviewed installation workflows; fresh isolated inputs per run; trusted local processes, not an OS sandbox.",
        }

    def publish_version(self, profile, expected_version):
        if profile not in PROFILES:
            raise ValueError("unknown reviewed workflow profile")
        store, meta = self._context()
        template = self._reviewed(prepare=True)[profile]
        with store.transaction() as db:
            latest = db.execute(
                "SELECT version,fingerprint FROM cp_versions WHERE workflow_id=? ORDER BY version DESC LIMIT 1",
                (meta["workflow_id"],),
            ).fetchone()
            if (
                latest["fingerprint"] != template["fingerprint"]
                and latest["version"] != expected_version
            ):
                raise Conflict("workflow version changed; refresh before registering")
            if (
                latest["fingerprint"] != template["fingerprint"]
                and latest["version"] >= 100
            ):
                raise ValueError("operations workspace reached its 100-version limit")
            workflow = store._register_workflow(
                db,
                meta["workspace_id"],
                "Order checks",
                template["specification"],
                template["directory"],
            )
            db.execute(
                "INSERT OR IGNORE INTO wb_versions VALUES(?,?,?)",
                (workflow["version"], profile, workflow["fingerprint"]),
            )
        return {"workflow": workflow, "overview": self.overview()}

    def _approved(self, store, meta, version):
        public, record = self._version(store, meta, version)
        if not public["approved"]:
            raise ValueError(
                "workflow differs from reviewed installation templates; register a current version"
            )
        return public, record

    def submit(self, version, event_key, priority=0):
        store, meta = self._context()
        self._approved(store, meta, version)
        with store.transaction() as db:
            prior = db.execute(
                "SELECT id FROM cp_runs WHERE workspace_id=? AND event_key=?",
                (meta["workspace_id"], event_key),
            ).fetchone()
            if (
                prior is None
                and db.execute(
                    "SELECT count(*) FROM cp_runs WHERE workspace_id=?",
                    (meta["workspace_id"],),
                ).fetchone()[0]
                >= 1000
            ):
                raise ValueError("operations workspace reached its 1,000-run limit")
            submitted = store._enqueue(
                db,
                meta["workspace_id"],
                meta["workflow_id"],
                version,
                event_key,
                priority,
                time.time(),
            )
        return {"submission": submitted, "detail": self.inspect(submitted["id"])}

    def inspect(self, run_id, *, after=0):
        store, meta = self._context()
        run = store.get_run(meta["workspace_id"], run_id)
        if run["workflow_id"] != meta["workflow_id"]:
            raise ValueError("run does not belong to the operations workflow")
        version, record = self._version(store, meta, run["version"])
        events = store.events(meta["workspace_id"], run_id, after=after, limit=51)
        history = store.events(meta["workspace_id"], run_id, after=0, limit=200)
        tasks = {
            task["id"]: {**task, "status": "pending", "attempts": 0, "output": ""}
            for task in version["tasks"]
        }
        for event in history:
            payload = event["payload"]
            task = tasks.get(payload.get("task")) if isinstance(payload, dict) else None
            if task is None:
                continue
            if event["kind"] == "task_started":
                task.update(status="running", attempts=payload.get("attempt", 0))
            elif event["kind"] == "task_finished":
                task.update(
                    status=payload.get("status", "unknown"),
                    attempts=payload.get("attempt", 0),
                )
            elif event["kind"] == "task_blocked":
                task["status"] = "blocked"
            elif event["kind"] == "task_cancelled":
                task["status"] = "cancelled"
        for item in (run.get("result") or {}).get("tasks", []):
            if item.get("id") in tasks:
                tasks[item["id"]].update(item)
        output = None
        if (
            tasks.get("aggregate", {}).get("status") == "succeeded"
            and version["approved"]
        ):
            target = (
                self.worker_root / "workspaces" / run_id / ".local/typed-summary.json"
            )
            try:
                if any(
                    item.is_symlink()
                    for item in [target, *target.parents]
                    if item.is_relative_to(self.worker_root)
                ):
                    raise ValueError("run output cannot be a symbolic link")
                if target.exists():
                    with target.open("rb") as stream:
                        raw = stream.read(1024 * 1024 + 1)
                    if len(raw) > 1024 * 1024:
                        raise ValueError("run output exceeds 1 MiB")
                    table, _ = table_bundle(raw)
                    rules = record["specification"]["tasks"][-2]["dataflow"]["gates"]
                    checks = [
                        rule.evaluate(table)
                        for rule in compile_gates(rules, table.contract)
                    ]
                    output = {
                        "passed": all(check["passed"] for check in checks),
                        "rows": table.json_rows(),
                        "sha256": hashlib.sha256(raw).hexdigest(),
                        "data_sha256": table.digest(),
                        "checks": checks,
                    }
            except (ValueError, OSError) as error:
                output = {"passed": False, "error": str(error)}
        page = events[:50]
        return {
            "run": run,
            "workflow": version,
            "tasks": list(tasks.values()),
            "events": page,
            "event_cursor": page[-1]["sequence"] if page else after,
            "more_events": len(events) > 50,
            "task_history_complete": len(history) < 200,
            "output": output,
            "output_scope": "Current run file checked against its registered quality gates on refresh.",
        }

