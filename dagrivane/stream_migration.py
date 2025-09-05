"""Reviewed state-fork migration, source quiescence and atomic bootstrap outbox."""

import base64
import json
import os
import time
from pathlib import Path

from .catalog_contracts import (
    fingerprint,
    json_bytes,
    parse_json,
    policy_definition,
    token,
)
from .ingest_audit import audit_in_view
from .ingest_model import stream_definition
from .stream_migration_model import compile_projection, project_state, projection_report
from .stream_migration_seed import (
    SCHEMA,
    exists,
    load_base,
    require_active,
    verify_base,
)


def connector_checkpoint(db, name):
    if (
        db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='http_sources'"
        ).fetchone()
        is None
    ):
        return None
    row = db.execute("SELECT * FROM http_sources WHERE stream=?", (name,)).fetchone()
    if row is None:
        return None
    active = row["claim"] is not None and (
        row["lease_until"] is None or row["lease_until"] > time.time()
    )
    if active or row["pending"] is not None:
        raise ValueError(
            "HTTP source must release its lease and resolve its pending page before migration"
        )
    return {
        key: row[key]
        for key in (
            "name",
            "configuration_sha",
            "offset",
            "generation",
            "cursor",
            "round",
            "chain",
        )
    }


def origin_in_view(store, db, name):
    stream, definition = store._stream(db, name)
    publications = db.execute(
        "SELECT b.*,p.status AS publication_status,p.artifact FROM batches b JOIN publications p ON p.stream=b.stream AND p.batch_id=b.id WHERE b.stream=? ORDER BY generation",
        (name,),
    ).fetchall()
    if any(row["publication_status"] != "published" for row in publications):
        raise ValueError("publish and acknowledge every source batch before migration")
    history = [
        {
            "raw": base64.b64encode(bytes(row["raw"])).decode(),
            "receipt": json.loads(row["receipt"]),
            "artifact": json.loads(row["artifact"]),
        }
        for row in publications
    ]
    artifact = history[-1]["artifact"] if history else None
    origin = {
        "store_id": store.id,
        "stream": name,
        "definition": definition,
        "generation": stream["generation"],
        "offset": stream["offset"],
        "watermark": stream["watermark"],
        "state_sha256": stream["state_sha256"],
        "seen_count": stream["seen_count"],
        "target_root": stream["target_root"],
        "artifact": artifact,
        "connector": connector_checkpoint(db, name),
    }
    return origin, history


