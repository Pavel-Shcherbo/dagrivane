"""Recompute retained HTTP identities and their atomic ingestion checkpoints."""

import hashlib

from .catalog_contracts import fingerprint, json_bytes, parse_json, table_bundle
from .contracts import Contract
from .tables import Table
from .quality import compile_gates
from .http_contract import adapt_page
from .ingest_audit import audit_in_view


def audit_source(store, name, *, limit=1000):
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("HTTP audit limit must be 1–1000 pages")
    findings = []
    checked = 0
    byte_count = 0
    complete = True
    with store.ingestion._tx(False) as db:
        source, config, stream, stream_def = store._source(db, name)
        offset = source["initial_offset"]
        generation = source["initial_generation"]
        cursor = source["initial_cursor"]
        round_number = 0
        chain = fingerprint([])
        pending = None
        rows = db.execute(
            "SELECT id FROM http_pages WHERE source=? ORDER BY seq LIMIT ?",
            (name, limit + 1),
        ).fetchall()
        for reference in rows:
            if checked >= limit:
                complete = False
                break
            try:
                page = store._page(db, name, reference["id"])
                byte_count += len(page["raw"])
                if byte_count > 64 * 1024 * 1024:
                    complete = False
                    break
                checked += 1
                if pending is not None:
                    raise ValueError("another page follows an unapplied HTTP page")
                if (
                    page["offset"],
                    page["generation"],
                    page["cursor"],
                    page["round"],
                    page["previous_chain"],
                ) != (offset, generation, cursor, round_number, chain):
                    raise ValueError("HTTP page checkpoint chain mismatch")
                if page["state"] == "discarded":
                    if (
                        not isinstance(page["reason"], str)
                        or not page["reason"].strip()
                    ):
                        raise ValueError("discarded HTTP page has no review reason")
                    chain = fingerprint(
                        [chain, page["id"], "discarded", page["reason"]]
                    )
                    continue
                if page["state"] in ("fetched", "blocked"):
                    pending = page["id"]
                    continue
                if page["state"] != "applied":
                    raise ValueError("unknown HTTP page state")
                meta = parse_json(page["metadata"].encode())
                adapted = adapt_page(
                    config,
                    stream_def,
                    page["raw"],
                    name=name,
                    after=offset,
                    request_cursor=parse_json(cursor.encode()),
                    content_type=meta["content_type"],
                    status=meta["status"],
                )
                saved = parse_json(page["receipt"].encode())
                ingestion = saved["ingestion"]
                if adapted["batch"] is not None:
                    adapted["batch"]["source_sha256"].append(chain)
                    raw = json_bytes(adapted["batch"], 4 * 1024 * 1024)
                    expected_id = fingerprint(
                        [
                            store.ingestion.id,
                            source["stream"],
                            source["stream_sha"],
                            hashlib.sha256(raw).hexdigest(),
                        ]
                    )
                    row = db.execute(
                        "SELECT raw,receipt,generation FROM batches WHERE stream=? AND id=?",
                        (source["stream"], expected_id),
                    ).fetchone()
                    if (
                        row is None
                        or bytes(row["raw"]) != raw
                        or row["generation"] != generation + 1
                        or ingestion is None
                        or {
                            key: value
                            for key, value in ingestion.items()
                            if key != "reused"
                        }
                        != parse_json(row["receipt"].encode())
                    ):
                        raise ValueError(
                            "HTTP page differs from its committed ingestion batch"
                        )
                    generation += 1
                elif ingestion is not None:
                    raise ValueError(
                        "empty HTTP page unexpectedly allocated an ingestion batch"
                    )
                else:
                    if generation:
                        historical = db.execute(
                            "SELECT bundle FROM batches WHERE stream=? AND generation=?",
                            (source["stream"], generation),
                        ).fetchone()
                        if historical is None:
                            raise ValueError(
                                "empty HTTP page references a missing materialized generation"
                            )
                        table = table_bundle(bytes(historical["bundle"]))[0]
                    else:
                        table = Table(Contract.compile(stream_def["contract"]), ())
                    checks = [
                        gate.evaluate(table)
                        for gate in compile_gates(stream_def["gates"], table.contract)
                    ]
                    if saved["unchanged_quality_checks"] != checks or not all(
                        check["passed"] for check in checks
                    ):
                        raise ValueError(
                            "empty HTTP page quality checks differ from historical materialized state"
                        )

                expected = {
                    "source": name,
                    "page_id": page["id"],
                    "offset_before": offset,
                    "offset_after": adapted["through"],
                    "generation": generation,
                    "items": adapted["items"],
                    "more": adapted["more"],
                    "cursor_sha256": fingerprint(adapted["next_cursor"]),
                    "ingestion": ingestion,
                    "unchanged_quality_checks": saved["unchanged_quality_checks"],
                }
                if saved != expected:
                    raise ValueError(
                        "HTTP page receipt differs from deterministic adaptation"
                    )
                offset = adapted["through"]
                cursor = json_bytes(adapted["next_cursor"]).decode()
                round_number += int(not adapted["more"])
                chain = fingerprint([chain, page["id"], "applied", fingerprint(saved)])
            except ValueError as error:
                findings.append({"page": reference["id"], "error": str(error)})
                break
        if complete and not findings:
            if (offset, generation, cursor, round_number, chain, pending) != (
                source["offset"],
                source["generation"],
                source["cursor"],
                source["round"],
                source["chain"],
                source["pending"],
            ):
                findings.append(
                    {"error": "HTTP source checkpoint differs from its page journal"}
                )
            if stream["offset"] != offset or stream["generation"] != generation:
                findings.append(
                    {"error": "ingestion stream was advanced outside the HTTP journal"}
                )
        if (
            complete
            and not findings
            and (source["pages"] != checked or source["journal_bytes"] != byte_count)
        ):
            findings.append(
                {"error": "HTTP journal counters differ from retained entities"}
            )
        ingestion = audit_in_view(store.ingestion, db, source["stream"], limit=1000)
    return {
        "passed": complete and not findings and ingestion["passed"],
        "complete": complete and ingestion["complete"],
        "checked_pages": checked,
        "checked_entity_bytes": byte_count,
        "pending_page": pending,
        "findings": findings,
        "ingestion_audit": ingestion,
        "scope": "Local raw-page/checkpoint/ingestion replay consistency; not source authenticity or remote completeness.",
    }
