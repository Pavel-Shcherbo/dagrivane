"""Replay bounded raw ingestion history against checkpoints, outbox and row caches."""

import hashlib
import json

from .catalog_contracts import fingerprint, json_bytes
from .ingest_model import batch_definition, reduce_events
from .tables import Table


def audit(store, name, *, limit=1000, catalog=None):
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("ingestion audit limit must be 1–1000 batches")
    errors = []
    checked = 0
    raw_bytes = 0
    event_count = 0
    complete = True
    with store._tx(False) as db:
        stream, definition = store._stream(db, name)
        rows = db.execute(
            "SELECT id,generation FROM batches WHERE stream=? ORDER BY generation LIMIT ?",
            (name, limit + 1),
        ).fetchall()
        complete = len(rows) <= limit
        records = {}
        seen = {}
        watermark = None
        position = 0
        previous_state = fingerprint([])
        previous_bundle = None
        previous_artifact = None
        publication_gap = False
        owners = {}
        for number, entry in enumerate(rows[:limit], 1):
            try:
                raw_row = db.execute(
                    "SELECT * FROM batches WHERE stream=? AND id=?", (name, entry["id"])
                ).fetchone()
                raw = bytes(raw_row["raw"])
                raw_bytes += len(raw)
                if raw_bytes > 64 * 1024 * 1024:
                    complete = False
                    break
                batch = batch_definition(raw, definition)
                event_count += len(batch["events"])
                if event_count > 100000:
                    complete = False
                    break
                raw_sha = hashlib.sha256(raw).hexdigest()
                if (
                    entry["generation"] != number
                    or batch["after"] != position
                    or raw_row["raw_sha256"] != raw_sha
                    or fingerprint(
                        [store.id, name, stream["definition_sha256"], raw_sha]
                    )
                    != entry["id"]
                ):
                    raise ValueError("raw batch or checkpoint chain identity mismatch")
                prior_ids = set(seen)
                records, seen, watermark, counts, outcomes, table, checks, state_sha = (
                    reduce_events(definition, records, seen, watermark, batch["events"])
                )
                for event_id in seen.keys() - prior_ids:
                    owners[event_id] = entry["id"]
                receipt = json.loads(raw_row["receipt"])
                expected = {
                    "stream": name,
                    "batch_id": entry["id"],
                    "generation": number,
                    "after": position,
                    "through": batch["through"],
                    "raw_sha256": raw_sha,
                    "previous_state_sha256": previous_state,
                    "state_sha256": state_sha,
                    "data_sha256": table.digest(),
                    "watermark": watermark,
                    "counts": counts,
                    "outcomes": outcomes,
                    "rows": len(table.records),
                    "tombstones": sum(item["row"] is None for item in records.values()),
                }
                if any(receipt.get(key) != value for key, value in expected.items()):
                    raise ValueError("stored batch receipt differs from replay")
                sources = list(
                    dict.fromkeys(
                        [
                            raw_sha,
                            *batch["source_sha256"],
                            *([previous_bundle] if previous_bundle else []),
                        ]
                    )
                )
                table = Table(table.contract, table.records, tuple(sources))
                bundle = json_bytes(
                    {
                        "format": "dagrivane.table.v1",
                        "report": {
                            "passed": True,
                            "plan_sha256": stream["definition_sha256"],
                            "output": table.summary(),
                            "checks": checks,
                        },
                        "rows": table.json_rows(),
                    }
                )
                bundle_sha = hashlib.sha256(bundle).hexdigest()
                if (
                    bytes(raw_row["bundle"]) != bundle
                    or raw_row["bundle_sha256"] != bundle_sha
                    or receipt.get("bundle_sha256") != bundle_sha
                ):
                    raise ValueError("outbox bundle differs from replay")
                publication = db.execute(
                    "SELECT * FROM publications WHERE stream=? AND batch_id=?",
                    (name, entry["id"]),
                ).fetchone()
                if publication is None or publication["status"] not in {
                    "pending",
                    "leased",
                    "published",
                }:
                    raise ValueError("missing or invalid publication state")
                if publication["status"] == "published":
                    if publication_gap:
                        raise ValueError(
                            "publication order contains an unacknowledged gap"
                        )
                    artifact = (
                        json.loads(publication["artifact"])
                        if publication["artifact"]
                        else None
                    )
                    if (
                        artifact is None
                        or artifact.get("sha256") != bundle_sha
                        or artifact.get("workspace")
                        != definition["target"]["workspace"]
                        or artifact.get("name") != definition["target"]["dataset"]
                        or artifact.get("policy_version")
                        != definition["target"]["policy_version"]
                        or artifact.get("kind") != "table"
                        or sorted(artifact.get("parents", []))
                        != ([previous_artifact] if previous_artifact else [])
                    ):
                        raise ValueError(
                            "published artifact binding or ancestry mismatch"
                        )
                    if catalog is not None:
                        if str(catalog.root) != stream["target_root"]:
                            raise ValueError(
                                "audit catalog differs from bound publication destination"
                            )
                        verified = catalog.verify_artifact(
                            definition["target"]["workspace"], artifact["id"]
                        )
                        actual = catalog.artifact(
                            definition["target"]["workspace"], artifact["id"]
                        )
                        if not verified["passed"] or actual != artifact:
                            raise ValueError(
                                "catalog artifact differs from stored acknowledgement"
                            )
                    previous_artifact = artifact["id"]
                else:
                    publication_gap = True
                position = batch["through"]
                previous_state = state_sha
                previous_bundle = bundle_sha
                checked += 1
            except (ValueError, KeyError, TypeError, OSError) as error:
                errors.append({"generation": entry["generation"], "error": str(error)})
                break
        if complete and not errors:
            actual_records = store._records(db, name)
            count = db.execute(
                "SELECT count(*) FROM events WHERE stream=?", (name,)
            ).fetchone()[0]
            actual_seen = {}
            actual_owners = {}
            if count == len(seen):
                for row in db.execute(
                    "SELECT event_id,content_sha256,batch_id FROM events WHERE stream=?",
                    (name,),
                ):
                    actual_seen[row["event_id"]] = row["content_sha256"]
                    actual_owners[row["event_id"]] = row["batch_id"]
            if (
                actual_records != records
                or actual_seen != seen
                or actual_owners != owners
                or stream["generation"] != checked
                or stream["offset"] != position
                or stream["watermark"] != watermark
                or stream["state_sha256"] != previous_state
                or stream["seen_count"] != len(seen)
            ):
                errors.append(
                    {
                        "error": "materialized checkpoint, event deduplication or row cache differs from replay"
                    }
                )
            if db.execute("PRAGMA foreign_key_check").fetchone():
                errors.append({"error": "ingestion foreign key inconsistency"})
    return {
        "passed": complete and not errors,
        "complete": complete,
        "checked_batches": checked,
        "replayed_events": event_count,
        "raw_bytes": raw_bytes,
        "catalog_checked": catalog is not None,
        "errors": errors,
    }
