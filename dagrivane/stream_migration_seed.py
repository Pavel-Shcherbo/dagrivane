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


def decode_stream_batch(db, name, raw, definition):
    value = parse_json(raw)
    base = (
        load_base(db, name)
        if isinstance(value, dict) and value.get("version") == 2
        else None
    )
    return bootstrap_batch(raw, definition, base, _value=value), base


def replay_origin(document, depth=0):
    if depth > 4:
        raise ValueError("migration lineage exceeds four predecessor migrations")
    source = document["origin"]
    definition = stream_definition(source["definition"])
    records = {}
    seen = {}
    watermark = None
    position = 0
    state_sha = fingerprint([])
    previous = None
    artifact = None
    parent = document["source_base"]
    if parent is not None:
        verify_base(parent, depth + 1)
        if (
            parent["plan"]["successor"] != source["stream"]
            or parent["plan"]["definition"] != definition
        ):
            raise ValueError("predecessor base differs from source identity")
        records = parent["records"]
        seen = dict(parent["seen"])
        watermark = parent["origin"]["watermark"]
        position = parent["origin"]["offset"]
        state_sha = parent["projection"]["state_sha256"]
        previous = parent["sha256"]
        artifact = parent["origin"]["artifact"]
    history = document["history"]
    if not isinstance(history, list) or len(history) > 1000:
        raise ValueError("migration proof exceeds 1,000 source batches")
    raw_bytes = 0
    event_count = 0
    for generation, entry in enumerate(history, 1):
        raw = base64.b64decode(entry["raw"], validate=True)
        raw_bytes += len(raw)
        if raw_bytes > 64 * 1024 * 1024:
            raise ValueError("migration source history exceeds 64 MiB raw data")
        batch = bootstrap_batch(raw, definition, parent)
        if batch.get("version") == 2 and generation != 1:
            raise ValueError("bootstrap must be the first successor generation")
        if batch["after"] != position:
            raise ValueError("migration source checkpoint chain has a gap")
        event_count += len(batch["events"])
        if event_count > 100000:
            raise ValueError("migration proof exceeds 100,000 source events")
        before = state_sha
        records, seen, watermark, counts, outcomes, table, checks, state_sha = (
            reduce_events(definition, records, seen, watermark, batch["events"])
        )
        raw_sha = hashlib.sha256(raw).hexdigest()
        batch_id = fingerprint(
            [source["store_id"], source["stream"], fingerprint(definition), raw_sha]
        )
        sources = list(
            dict.fromkeys(
                [raw_sha, *batch["source_sha256"], *([previous] if previous else [])]
            )
        )
        table = Table(table.contract, table.records, tuple(sources))
        bundle = json_bytes(
            {
                "format": "dagrivane.table.v1",
                "report": {
                    "passed": True,
                    "plan_sha256": fingerprint(definition),
                    "output": table.summary(),
                    "checks": checks,
                },
                "rows": table.json_rows(),
            }
        )
        sha = hashlib.sha256(bundle).hexdigest()
        expected = {
            "stream": source["stream"],
            "batch_id": batch_id,
            "generation": generation,
            "after": position,
            "through": batch["through"],
            "raw_sha256": raw_sha,
            "previous_state_sha256": before,
            "state_sha256": state_sha,
            "bundle_sha256": sha,
            "data_sha256": table.digest(),
            "watermark": watermark,
            "counts": counts,
            "outcomes": outcomes,
            "rows": len(table.records),
            "tombstones": sum(item["row"] is None for item in records.values()),
        }
        if any(entry["receipt"].get(k) != v for k, v in expected.items()):
            raise ValueError("migration source receipt differs from raw replay")
        published = entry["artifact"]
        if (
            not isinstance(published, dict)
            or published.get("sha256") != sha
            or published.get("kind") != "table"
            or published.get("workspace") != definition["target"]["workspace"]
            or published.get("name") != definition["target"]["dataset"]
            or published.get("policy_version") != definition["target"]["policy_version"]
            or published.get("parents") != ([artifact["id"]] if artifact else [])
        ):
            raise ValueError("migration source publication or ancestry differs")
        artifact = published
        previous = sha
        position = batch["through"]
    if (
        source["generation"] != len(history)
        or source["offset"] != position
        or source["watermark"] != watermark
        or source["state_sha256"] != state_sha
        or source["seen_count"] != len(seen)
        or source["artifact"] != artifact
    ):
        raise ValueError("migration source checkpoint differs from replayed history")
    return records, seen


