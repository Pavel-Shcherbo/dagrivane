"""Immutable migrated bases and replay of their complete bounded source histories."""

import base64
import hashlib
import json

from .catalog_contracts import fingerprint, json_bytes, parse_json
from .ingest_model import batch_definition, reduce_events, stream_definition
from .stream_migration_model import compile_projection, project_state, projection_report
from .tables import Table

SCHEMA = [
    "CREATE TABLE IF NOT EXISTS migration_bases(stream TEXT PRIMARY KEY REFERENCES streams(name),payload BLOB NOT NULL,sha256 TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS migration_seen(stream TEXT NOT NULL REFERENCES streams(name),event_id TEXT NOT NULL,content_sha256 TEXT NOT NULL,PRIMARY KEY(stream,event_id))",
    "CREATE TABLE IF NOT EXISTS migration_retired(stream TEXT PRIMARY KEY REFERENCES streams(name),successor TEXT NOT NULL REFERENCES streams(name),migration_sha256 TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS migration_requests(request_key TEXT PRIMARY KEY,migration_sha256 TEXT NOT NULL,receipt BLOB NOT NULL)",
]


def exists(db):
    return (
        db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='migration_bases'"
        ).fetchone()
        is not None
    )


def retired(db, name):
    if not exists(db):
        return None
    row = db.execute(
        "SELECT successor,migration_sha256 FROM migration_retired WHERE stream=?",
        (name,),
    ).fetchone()
    return dict(row) if row else None


def require_active(db, name):
    value = retired(db, name)
    if value is not None:
        raise ValueError(
            "stream is retired by migration; use successor " + value["successor"]
        )


def load_base(db, name):
    if not exists(db):
        return None
    row = db.execute("SELECT * FROM migration_bases WHERE stream=?", (name,)).fetchone()
    if row is None:
        return None
    document = parse_json(bytes(row["payload"]))
    if (
        document.get("sha256") != row["sha256"]
        or fingerprint({k: v for k, v in document.items() if k != "sha256"})
        != row["sha256"]
    ):
        raise ValueError("migrated base identity changed")
    return document


def initial_state(db, name):
    base = load_base(db, name)
    if base is None:
        return {}, {}, None, 0, fingerprint([]), None, None
    verify_base(base)
    source = base["origin"]
    records = base["records"]
    seen = base["seen"]
    return (
        records,
        seen,
        source["watermark"],
        source["offset"],
        base["projection"]["state_sha256"],
        base["sha256"],
        source["artifact"]["id"] if source["artifact"] else None,
    )


def bootstrap_batch(raw, definition, base, *, _value=None):
    value = parse_json(raw) if _value is None else _value
    if isinstance(value, dict) and value.get("version") == 2:
        if (
            base is None
            or set(value)
            != {"version", "kind", "base_sha256", "after", "through", "events"}
            or type(value["version"]) is not int
            or value["kind"] != "migration_bootstrap"
            or value["base_sha256"] != base["sha256"]
            or type(value["after"]) is not int
            or type(value["through"]) is not int
            or value["after"] != base["origin"]["offset"]
            or value["through"] != value["after"]
            or value["events"] != []
        ):
            raise ValueError("invalid migration bootstrap batch")
        return {**value, "source_sha256": [base["sha256"]]}
    return batch_definition(raw, definition, _value=value)


