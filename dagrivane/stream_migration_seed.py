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


