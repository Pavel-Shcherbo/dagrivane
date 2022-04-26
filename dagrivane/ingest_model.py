"""Immutable incremental-stream contracts and deterministic event reduction."""

import copy
from datetime import timedelta
from decimal import Context, Decimal

from .catalog_contracts import fingerprint, json_bytes, parse_json, token
from .contracts import Contract, convert, encode
from .quality import compile_gates
from .tables import Table

MAX_BATCH_BYTES = 4 * 1024 * 1024
MAX_EVENTS = 10000
MAX_KEYS = 100000


def instant(value):
    return (
        convert(value, "timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


