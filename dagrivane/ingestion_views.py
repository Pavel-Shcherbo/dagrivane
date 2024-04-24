"""Coherent ingestion projections over source, stream, outbox and review state."""

from decimal import Decimal, localcontext

from .catalog_contracts import fingerprint, parse_json

PROFILES = {
    "healthy": "Incremental orders",
    "quality": "Quality rejection",
    "retry": "Transient HTTP failure",
}


def require_profile(value):
    if not isinstance(value, str) or value not in PROFILES:
        raise ValueError("choose a reviewed ingestion source")
    return value


def provider_state(db, profile):
    row = db.execute(
        "SELECT corrected,retry_sent,requests FROM desk_providers WHERE profile=?",
        (profile,),
    ).fetchone()
    if row is None:
        raise ValueError("reviewed provider has not been initialized")
    return {
        "corrected": bool(row["corrected"]),
        "retry_sent": bool(row["retry_sent"]),
        "requests": row["requests"],
    }


def revision_in_view(db, sources, profile):
    source = sources._public(db, profile)
    provider = provider_state(db, profile)
    publications = [
        dict(row)
        for row in db.execute(
            "SELECT b.generation,b.id,p.status,p.attempts,p.artifact FROM batches b JOIN publications p ON p.stream=b.stream AND p.batch_id=b.id WHERE b.stream=? ORDER BY b.generation",
            (profile,),
        )
    ]
    # Lease wall-clock changes and request counters do not identify a data revision.
    revision = fingerprint(
        {
            "chain": source["chain_sha256"],
            "configuration_sha256": source["configuration_sha256"],
            "stream_definition_sha256": source["stream_definition_sha256"],
            "offset": source["offset"],
            "generation": source["generation"],
            "pending": source["pending"],
            "publications": publications,
            "corrected": provider["corrected"],
        }
    )
    return revision, source, provider, publications


def project_view(
    db, store, sources, profile, *, after_page=0, after_generation=0, row_offset=0
):
    revision, source, provider, publications = revision_in_view(db, sources, profile)
    stream = store._status(db, profile)
    data = store._rows(db, profile)
    if row_offset > len(data["rows"]):
        raise ValueError("row offset exceeds retained live rows")
    with localcontext() as context:
        context.prec = 80
        total = format(
            sum((Decimal(row["amount"]) for row in data["rows"]), Decimal(0)), "f"
        )
    data = {
        **data,
        "rows": data["rows"][row_offset : row_offset + 25],
        "row_offset": row_offset,
        "total_rows": len(data["rows"]),
        "next_offset": row_offset + 25 if row_offset + 25 < len(data["rows"]) else None,
        "amount_total": total,
    }
    pages = [
        dict(row)
        for row in db.execute(
            "SELECT id,seq,state,offset,generation,raw_sha,error,reason,received_at FROM http_pages WHERE source=? AND seq>? ORDER BY seq LIMIT 26",
            (profile, after_page),
        )
    ]
    outbox = []
    for row in db.execute(
        "SELECT b.id,b.generation,b.bundle_sha256,p.status,p.attempts,p.last_error,p.artifact FROM batches b JOIN publications p ON p.stream=b.stream AND p.batch_id=b.id WHERE b.stream=? AND b.generation>? ORDER BY b.generation LIMIT 26",
        (profile, after_generation),
    ):
        value = dict(row)
        value["artifact"] = (
            parse_json(value["artifact"].encode()) if value["artifact"] else None
        )
        outbox.append(value)
    operations = [
        dict(row)
        for row in db.execute(
            "SELECT seq,request_key,action,status,error,started_at,finished_at FROM desk_operations WHERE profile=? ORDER BY seq DESC LIMIT 10",
            (profile,),
        )
    ]
    head = next(
        (row["id"] for row in publications if row["status"] != "published"), None
    )
    return {
        "profile": profile,
        "label": PROFILES[profile],
        "revision": revision,
        "source": source,
        "provider": provider,
        "stream": stream,
        "data": data,
        "pages": {
            "items": pages[:25],
            "next_cursor": pages[24]["seq"] if len(pages) > 25 else None,
        },
        "publications": {
            "items": outbox[:25],
            "next_generation": outbox[24]["generation"] if len(outbox) > 25 else None,
            "head_batch_id": head,
        },
        "operations": operations,
        "snapshot_scope": "Source, stream, records, outbox and provider control in one SQLite read snapshot; catalog objects verified separately",
    }
