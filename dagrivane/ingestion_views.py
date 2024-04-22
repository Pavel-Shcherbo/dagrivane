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


