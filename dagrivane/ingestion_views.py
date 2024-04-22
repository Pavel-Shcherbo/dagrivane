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


