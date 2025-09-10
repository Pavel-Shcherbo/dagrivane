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


def prepare_migration(store, proposal, *, catalog):
    with store._tx(False) as db:
        name = proposal.get("source") if isinstance(proposal, dict) else None
        require_active(db, name)
        source, definition = store._stream(db, name)
        plan = compile_projection(definition, proposal)
        if db.execute(
            "SELECT name FROM streams WHERE name=?", (plan["successor"],)
        ).fetchone():
            raise ValueError("successor stream already exists")
        if source["target_root"] is not None and source["target_root"] != str(
            catalog.root
        ):
            raise ValueError(
                "migration catalog differs from the source publication destination"
            )
        checked = audit_in_view(store, db, name, catalog=catalog)
        if not checked["passed"] or not checked["complete"]:
            raise ValueError(
                "migration requires a complete passing source replay audit"
            )
        origin, history = origin_in_view(store, db, name)
        parent = load_base(db, name)
        from .stream_migration_seed import replay_origin

        shell = {"origin": origin, "history": history, "source_base": parent}
        records, seen = replay_origin(shell)
        projected, _, _, _ = project_state(definition, plan, records)
        document = {
            "format": "dagrivane.stream-migration.v1",
            "destination_root": str(catalog.root),
            "origin": origin,
            "source_base": parent,
            "history": history,
            "plan": plan,
            "records": projected,
            "seen": seen,
            "projection": projection_report(definition, plan, records),
        }
        document["sha256"] = fingerprint(document)
        verify_base(document)
        return document


