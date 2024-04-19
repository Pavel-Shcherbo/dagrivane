"""Durable reviewed ingestion controls without browser-defined URLs or code."""

import base64
import copy
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
import time

from .catalog import Catalog
from .catalog_contracts import fingerprint, json_bytes, parse_json, table_bundle, token
from .http_audit import audit_source
from .http_contract import endpoint
from .http_runner import poll_source
from .http_store import HTTPSourceStore
from .ingest_audit import audit as audit_ingestion
from .ingest_publish import configure_target, publish_next
from .ingest_store import IngestStore
from .ingestion_views import (
    PROFILES,
    require_profile,
    provider_state,
    project_view,
    revision_in_view,
)
from .resources import examples_directory

LOCK = threading.Lock()
OPERATION_RESULT_BUDGET = 32 * 1024 * 1024
SCHEMA = [
    "CREATE TABLE IF NOT EXISTS desk_meta(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL,origin TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS desk_providers(profile TEXT PRIMARY KEY,corrected INTEGER NOT NULL,retry_sent INTEGER NOT NULL,requests INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS desk_operations(seq INTEGER PRIMARY KEY AUTOINCREMENT,profile TEXT NOT NULL,request_key TEXT NOT NULL,action TEXT NOT NULL,request_sha256 TEXT NOT NULL,status TEXT NOT NULL,result BLOB,error TEXT,receipt_sha256 TEXT NOT NULL DEFAULT '',started_at REAL NOT NULL,finished_at REAL,UNIQUE(profile,request_key))",
]


def loopback_origin(value):
    normalized = endpoint(value)
    if not re.fullmatch(r"http://127\.0\.0\.1:[1-9][0-9]{0,4}/?", normalized):
        raise ValueError("ingestion workbench needs its actual loopback HTTP origin")
    return normalized.rstrip("/")


def digest(value, label="identity"):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(label + " requires lowercase SHA-256")
    return value


class IngestionDesk:
    def __init__(self, root, origin=None):
        self.root = Path(root)
        self.path = self.root / "ingestion.sqlite"
        self.origin = loopback_origin(origin) if origin is not None else None

    def _stores(self):
        store = IngestStore(self.path)
        sources = HTTPSourceStore(store)
        with store._tx(False) as db:
            marker = db.execute(
                "SELECT version,origin FROM desk_meta WHERE singleton=1"
            ).fetchone()
            if marker is None or marker["version"] != 1:
                raise ValueError("unsupported ingestion workbench schema")
        return store, sources

    def _writable_origin(self, store, sources, profile):
        with store._tx(False) as db:
            stored = db.execute(
                "SELECT origin FROM desk_meta WHERE singleton=1"
            ).fetchone()["origin"]
        with store._tx(False) as db:
            _, config, _, _ = sources._source(db, profile)
        if self.origin is None or stored != self.origin:
            raise ValueError(
                "source origin changed; resume on the original workbench port before polling"
            )
        if self.origin is not None and (
            config["url"] != self.origin + "/api/ingestion-source/" + profile
            or config["authentication"] is not None
            or config["request"]["redirects"] != 0
        ):
            raise ValueError(
                "source configuration differs from the reviewed loopback endpoint"
            )

    def initialize(self):
        if self.origin is None:
            raise ValueError(
                "ingestion initialization requires a running loopback workbench"
            )
        if not LOCK.acquire(blocking=False):
            raise ValueError("another ingestion operation is running")
        try:
            store = IngestStore(self.path, create=True)
            sources = HTTPSourceStore(store)
            with store._tx() as db:
                for statement in SCHEMA:
                    db.execute(statement)
                marker = db.execute(
                    "SELECT version,origin FROM desk_meta WHERE singleton=1"
                ).fetchone()
                if marker is not None and (
                    marker["version"] != 1 or marker["origin"] != self.origin
                ):
                    raise ValueError(
                        "workbench origin differs from retained source configuration; use the original port"
                    )
                db.execute(
                    "INSERT OR IGNORE INTO desk_meta VALUES(1,1,?)", (self.origin,)
                )
            directory = examples_directory()
            catalog = Catalog(self.root / "catalog")
            for profile in PROFILES:
                stream = json.loads((directory / "incremental-stream.json").read_text())
                stream["target"] = {
                    "workspace": "ingestion-workbench",
                    "dataset": profile,
                    "policy_version": 1,
                }
                store.register(profile, stream)
                config = json.loads((directory / "http-change-source.json").read_text())
                config["url"] = self.origin + "/api/ingestion-source/" + profile
                config["request"].update(timeout_seconds=2, attempts=2, redirects=0)
                sources.register(profile, profile, config)
                configure_target(store, profile, catalog)
                with store._tx() as db:
                    db.execute(
                        "INSERT OR IGNORE INTO desk_providers VALUES(?,0,0,0)",
                        (profile,),
                    )
            return self.overview()
        finally:
            LOCK.release()

    def overview(self):
        if not self.path.exists():
            return {"initialized": False, "items": []}
        store, sources = self._stores()
        items = []
        with store._tx(False) as db:
            for profile, label in PROFILES.items():
                registered = db.execute(
                    "SELECT name FROM http_sources WHERE name=?", (profile,)
                ).fetchone()
                if (
                    registered is None
                    or db.execute(
                        "SELECT profile FROM desk_providers WHERE profile=?", (profile,)
                    ).fetchone()
                    is None
                ):
                    continue
                revision, source, provider, _ = revision_in_view(db, sources, profile)
                items.append(
                    {
                        "profile": profile,
                        "label": label,
                        "revision": revision,
                        "source": source,
                        "provider": provider,
                        "stream": store._status(db, profile),
                    }
                )
        return {
            "initialized": len(items) == len(PROFILES),
            "items": items,
            "local_only": True,
        }

    def inspect(self, profile, **cursors):
        require_profile(profile)
        store, sources = self._stores()
        with store._tx(False) as db:
            return project_view(db, store, sources, profile, **cursors)

    def source_response(self, profile, cursor):
        require_profile(profile)
        store, _ = self._stores()
        with store._tx() as db:
            state = provider_state(db, profile)
            retry = profile == "retry" and not state["retry_sent"]
            db.execute(
                "UPDATE desk_providers SET requests=requests+1,retry_sent=max(retry_sent,?) WHERE profile=?",
                (int(retry), profile),
            )
        if retry:
            return (
                503,
                {"error": "synthetic transient source failure"},
                {"Retry-After": "0"},
            )
        if cursor == "tail":
            return 204, None, {}
        if cursor not in (None, "page2"):
            return 409, {"error": "unknown synthetic cursor"}, {}
        pages = json.loads(
            (examples_directory() / "ingestion-control-pages.json").read_text()
        )
        payload = copy.deepcopy(pages[0 if cursor is None else 1])
        if profile == "quality" and not state["corrected"] and cursor is None:
            payload["events"][0]["row"]["amount"] = "2000"
        return 200, payload, {}

    def _receipt(self, row):
        return {
            key: row[key]
            for key in (
                "profile",
                "request_key",
                "action",
                "request_sha256",
                "status",
                "error",
                "started_at",
                "finished_at",
            )
        } | {
            "result": (
                parse_json(bytes(row["result"])) if row["result"] is not None else None
            )
        }

    def _seal(self, db, profile, request_key):
        row = db.execute(
            "SELECT * FROM desk_operations WHERE profile=? AND request_key=?",
            (profile, request_key),
        ).fetchone()
        db.execute(
            "UPDATE desk_operations SET receipt_sha256=? WHERE profile=? AND request_key=?",
            (fingerprint(self._receipt(row)), profile, request_key),
        )

    def _operation(self, row):
        if row["receipt_sha256"] != fingerprint(self._receipt(row)):
            raise ValueError("ingestion operation receipt fingerprint mismatch")
        result = parse_json(bytes(row["result"])) if row["result"] is not None else None
        return {
            "operation": {
                "receipt_sha256": row["receipt_sha256"],
                "sequence": row["seq"],
                "profile": row["profile"],
                "request_key": row["request_key"],
                "action": row["action"],
                "status": row["status"],
                "error": row["error"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "recovery": (
                    "Refresh durable source state before a new request; a started receipt is not proof of rollback"
                    if row["status"] == "started"
                    else None
                ),
            },
            "outcome": result,
        }

    def mutate(
        self,
        profile,
        action,
        *,
        request_key,
        expected_revision,
        page_id=None,
        reason=None,
    ):
        require_profile(profile)
        token(request_key, "ingestion request key")
        digest(expected_revision, "view revision")
        if action not in ("poll", "publish", "repair", "discard"):
            raise ValueError("unknown ingestion operation")
        if action in ("repair", "discard"):
            digest(page_id, "retained page")
            if (
                not isinstance(reason, str)
                or not reason.strip()
                or len(reason) > 512
                or any(ord(char) < 32 for char in reason)
            ):
                raise ValueError(
                    "review requires a reason of 1–512 printable characters"
                )
        payload = {
            "profile": profile,
            "action": action,
            "expected_revision": expected_revision,
            "page_id": page_id,
            "reason": reason,
        }
        identity = fingerprint(payload)
        store, sources = self._stores()
        if not LOCK.acquire(blocking=False):
            raise ValueError(
                "another ingestion operation is running; refresh its result"
            )
        try:
            with store._tx() as db:
                previous = db.execute(
                    "SELECT * FROM desk_operations WHERE profile=? AND request_key=?",
                    (profile, request_key),
                ).fetchone()
                if previous is not None:
                    if previous["request_sha256"] != identity:
                        raise ValueError(
                            "request key conflicts with different ingestion controls"
                        )
                    return self._operation(previous)
                revision, source, provider, publications = revision_in_view(
                    db, sources, profile
                )
                if revision != expected_revision:
                    raise ValueError(
                        "source view changed; refresh before applying this action"
                    )
                if (
                    db.execute(
                        "SELECT coalesce(sum(length(result)),0) FROM desk_operations"
                    ).fetchone()[0]
                    >= OPERATION_RESULT_BUDGET
                ):
                    raise ValueError(
                        "ingestion operation results reached the 32 MiB logical budget"
                    )
                if (
                    db.execute("SELECT count(*) FROM desk_operations").fetchone()[0]
                    >= 2000
                ):
                    raise ValueError(
                        "ingestion workbench reached 2,000 retained operation receipts"
                    )
                db.execute(
                    "INSERT INTO desk_operations(profile,request_key,action,request_sha256,status,started_at) VALUES(?,?,?,?,'started',?)",
                    (profile, request_key, action, identity, time.time()),
                )
                self._seal(db, profile, request_key)
            result = None
            error = None
            try:
                if action == "poll":
                    self._writable_origin(store, sources, profile)
                    result = poll_source(
                        sources,
                        profile,
                        max_pages=1,
                        deadline_seconds=10,
                        lease_seconds=15,
                        expected_chain=source["chain_sha256"],
                    )
                elif action == "publish":
                    head = next(
                        (
                            row["id"]
                            for row in publications
                            if row["status"] != "published"
                        ),
                        None,
                    )
                    result = (
                        publish_next(
                            store,
                            profile,
                            Catalog(self.root / "catalog"),
                            expected_batch_id=head,
                        )
                        if head is not None
                        else {"published": False, "reason": "no_pending_batch"}
                    )
                elif action == "discard":
                    result = sources.discard(profile, page_id, reason)
                else:
                    if profile != "quality":
                        raise ValueError(
                            "only the synthetic quality source has a correction control"
                        )
                    with store._tx() as db:
                        current = sources._public(db, profile)
                        if (
                            current["lease_active"]
                            or not current["pending"]
                            or current["pending"]["id"] != page_id
                            or current["pending"]["state"] != "blocked"
                        ):
                            raise ValueError(
                                "correction requires the current blocked page and an idle source"
                            )
                        db.execute(
                            "UPDATE desk_providers SET corrected=1 WHERE profile=?",
                            (profile,),
                        )
                    result = {
                        "corrected": True,
                        "retained_page_unchanged": page_id,
                        "review_reason": reason,
                        "next_step": "Discard the blocked page with a reason, then poll the unchanged cursor",
                    }
                if len(json_bytes(result)) > 2 * 1024 * 1024:
                    raise ValueError("operation result exceeds 2 MiB")
            except (ValueError, OSError, sqlite3.Error) as problem:
                error = str(problem)[:2048]
                result = None
            with store._tx() as db:
                encoded = json_bytes(result) if result is not None else None
                used = db.execute(
                    "SELECT coalesce(sum(length(result)),0) FROM desk_operations"
                ).fetchone()[0]
                if used + len(encoded or b"") > OPERATION_RESULT_BUDGET:
                    encoded = None
                    error = "operation result exceeds the retained receipt budget; inspect durable source state before a new request"
                db.execute(
                    "UPDATE desk_operations SET status=?,result=?,error=?,finished_at=? WHERE profile=? AND request_key=?",
                    (
                        "failed" if error else "completed",
                        encoded,
                        error,
                        time.time(),
                        profile,
                        request_key,
                    ),
                )
                self._seal(db, profile, request_key)
                return self._operation(
                    db.execute(
                        "SELECT * FROM desk_operations WHERE profile=? AND request_key=?",
                        (profile, request_key),
                    ).fetchone()
                )
        finally:
            LOCK.release()

    def operation(self, profile, request_key):
        require_profile(profile)
        token(request_key, "ingestion request key")
        store, _ = self._stores()
        with store._tx(False) as db:
            row = db.execute(
                "SELECT * FROM desk_operations WHERE profile=? AND request_key=?",
                (profile, request_key),
            ).fetchone()
            if row is None:
                raise ValueError("unknown operation receipt for this source")
            return self._operation(row)

    def page(self, profile, page_id):
        require_profile(profile)
        digest(page_id, "retained page")
        _, sources = self._stores()
        value = sources.page(profile, page_id, include_raw=True)
        raw = value.pop("raw")
        return {
            **value,
            "raw_base64": base64.b64encode(raw).decode(),
            "raw_bytes": len(raw),
            "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "content_scope": "Complete retained entity, base64 encoded; metadata and content identities are not source authentication",
        }

    def artifact(self, profile, generation):
        require_profile(profile)
        store, _ = self._stores()
        with store._tx(False) as db:
            row = db.execute(
                "SELECT p.artifact,b.bundle_sha256 FROM batches b JOIN publications p ON p.stream=b.stream AND p.batch_id=b.id WHERE b.stream=? AND b.generation=? AND p.status='published'",
                (profile, generation),
            ).fetchone()
            if row is None:
                raise ValueError("generation has no acknowledged catalog artifact")
            saved = parse_json(row["artifact"].encode())
        catalog = Catalog(self.root / "catalog")
        artifact = catalog.artifact("ingestion-workbench", saved["id"])
        if (
            artifact != saved
            or artifact["sha256"] != row["bundle_sha256"]
            or artifact["name"] != profile
        ):
            raise ValueError("acknowledged artifact differs from its source generation")
        verification = catalog.verify_artifact("ingestion-workbench", artifact["id"])
        raw = catalog.objects.get(artifact["sha256"])
        table, _ = table_bundle(raw)
        return {
            "artifact": artifact,
            "verification": verification,
            "bundle_base64": base64.b64encode(raw).decode(),
            "bundle_sha256": hashlib.sha256(raw).hexdigest(),
            "table_summary": table.summary(),
            "scope": "Verified local catalog content; no external publication",
        }

    def audit(self, profile):
        require_profile(profile)
        store, sources = self._stores()
        source = audit_source(sources, profile, limit=100)
        ingestion = audit_ingestion(
            store, profile, limit=100, catalog=Catalog(self.root / "catalog")
        )
        operations = []
        with store._tx(False) as db:
            rows = db.execute(
                "SELECT * FROM desk_operations WHERE profile=? ORDER BY seq LIMIT 101",
                (profile,),
            ).fetchall()
            receipt_bytes = 0
            complete = len(rows) <= 100
            for row in rows[:100]:
                receipt_bytes += len(row["result"] or b"")
                if receipt_bytes > 16 * 1024 * 1024:
                    complete = False
                    break
                try:
                    self._operation(row)
                    operations.append({"sequence": row["seq"], "passed": True})
                except ValueError as error:
                    operations.append(
                        {"sequence": row["seq"], "passed": False, "error": str(error)}
                    )
        receipts = {
            "items": operations,
            "complete": complete,
            "checked_bytes": receipt_bytes,
            "passed": complete and all(item["passed"] for item in operations),
        }
        return {
            "operations": receipts,
            "source": source,
            "ingestion": ingestion,
            "passed": source["passed"] and ingestion["passed"] and receipts["passed"],
            "scope": "Each bounded audit is coherent in its own read view; not a cross-database transaction or source authentication",
        }
