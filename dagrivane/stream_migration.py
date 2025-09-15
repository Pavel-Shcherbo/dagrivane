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


def configure_migration_policy(document, catalog):
    verify_base(document)
    if str(catalog.root) != document["destination_root"]:
        raise ValueError("migration policy destination differs from reviewed catalog")
    definition = document["plan"]["definition"]
    target = definition["target"]
    if (
        document["origin"]["target_root"] is not None
        and str(catalog.root) != document["origin"]["target_root"]
    ):
        raise ValueError(
            "migration policy catalog differs from the reviewed destination"
        )
    catalog.create_workspace(target["workspace"])
    return catalog.register_policy(
        target["workspace"],
        target["dataset"],
        target["policy_version"],
        definition["contract"],
        definition["gates"],
        allow_breaking=True,
    )


def apply_migration(store, document, *, catalog, request_key):
    token(request_key, "migration request key")
    verify_base(document)
    if str(catalog.root) != document["destination_root"]:
        raise ValueError("migration destination differs from reviewed catalog")
    plan = document["plan"]
    target = plan["definition"]["target"]
    if document["origin"]["store_id"] != store.id:
        raise ValueError("migration belongs to another ingestion store")
    policy, _, _ = policy_definition(
        plan["definition"]["contract"],
        plan["definition"]["gates"],
        target["policy_version"],
    )
    if catalog.policy(target["workspace"], target["dataset"], target["policy_version"])[
        "sha256"
    ] != fingerprint(policy):
        raise ValueError("target policy differs from the reviewed migration definition")
    with store._tx() as db:
        for sql in SCHEMA:
            db.execute(sql)
        previous = db.execute(
            "SELECT * FROM migration_requests WHERE request_key=?", (request_key,)
        ).fetchone()
        if previous is not None:
            if previous["migration_sha256"] != document["sha256"]:
                raise ValueError(
                    "migration request key identifies another reviewed plan"
                )
            return parse_json(bytes(previous["receipt"]))
        require_active(db, plan["source"])
        if db.execute(
            "SELECT name FROM streams WHERE name=?", (plan["successor"],)
        ).fetchone():
            raise ValueError("successor stream already exists")
        origin, history = origin_in_view(store, db, plan["source"])
        if (
            origin != document["origin"]
            or history != document["history"]
            or load_base(db, plan["source"]) != document["source_base"]
        ):
            raise ValueError(
                "source checkpoint or publication chain changed after migration review"
            )
        if origin["target_root"] is not None and origin["target_root"] != str(
            catalog.root
        ):
            raise ValueError("migration destination changed")
        checked = audit_in_view(store, db, plan["source"], catalog=catalog)
        if not checked["passed"] or not checked["complete"]:
            raise ValueError("source is not a complete verified migration checkpoint")
        if db.execute("SELECT count(*) FROM streams").fetchone()[0] >= 1000:
            raise ValueError("ingestion store exceeds 1,000 streams")
        definition = stream_definition(plan["definition"])
        db.execute(
            "INSERT INTO streams VALUES(?,?,?,0,?,?,?,?,?)",
            (
                plan["successor"],
                json_bytes(definition).decode(),
                fingerprint(definition),
                origin["offset"],
                origin["watermark"],
                document["projection"]["state_sha256"],
                len(document["seen"]),
                str(catalog.root),
            ),
        )
        db.execute(
            "INSERT INTO migration_bases VALUES(?,?,?)",
            (plan["successor"], json_bytes(document), document["sha256"]),
        )
        db.executemany(
            "INSERT INTO migration_seen VALUES(?,?,?)",
            [
                (plan["successor"], event_id, sha)
                for event_id, sha in sorted(document["seen"].items())
            ],
        )
        db.executemany(
            "INSERT INTO records VALUES(?,?,?)",
            [
                (plan["successor"], key, json_bytes(item).decode())
                for key, item in sorted(document["records"].items())
            ],
        )
        raw = json_bytes(
            {
                "version": 2,
                "kind": "migration_bootstrap",
                "base_sha256": document["sha256"],
                "after": origin["offset"],
                "through": origin["offset"],
                "events": [],
            }
        )
        bootstrap = store._ingest(
            db, plan["successor"], raw, request_key="migration:" + document["sha256"]
        )
        db.execute(
            "INSERT INTO migration_retired VALUES(?,?,?)",
            (plan["source"], plan["successor"], document["sha256"]),
        )
        receipt = {
            "migration_sha256": document["sha256"],
            "source": plan["source"],
            "successor": plan["successor"],
            "source_generation": origin["generation"],
            "offset": origin["offset"],
            "rows": document["projection"]["rows"],
            "tombstones": document["projection"]["tombstones"],
            "bootstrap": bootstrap,
            "source_frozen": True,
            "catalog_policy_sha256": fingerprint(policy),
        }
        db.execute(
            "INSERT INTO migration_requests VALUES(?,?,?)",
            (request_key, document["sha256"], json_bytes(receipt)),
        )
        return receipt


def write_migration(document, path):
    verify_base(document)
    path = Path(path)
    raw = json_bytes(document) + b"\n"
    if len(raw) > 64 * 1024 * 1024:
        raise ValueError("serialized migration proof exceeds 64 MiB")
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
    return {
        "path": str(path),
        "sha256": document["sha256"],
        "rows": document["projection"]["rows"],
    }
