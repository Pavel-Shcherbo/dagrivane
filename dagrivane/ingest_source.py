"""Convert a pinned local CSV revision into an explicit ordered upsert batch."""

import os
import tempfile
from pathlib import Path

from .catalog_contracts import fingerprint, json_bytes
from .contracts import Contract
from .ingest_model import MAX_BATCH_BYTES, batch_definition, normalized_key, offset
from .tables import read_csv


def csv_batch(definition, path, *, after, expected_sha256=None):
    offset(after)
    contract = Contract.compile(definition["contract"])
    table = read_csv(path, contract, expected_sha256=expected_sha256)
    if not 1 <= len(table.records) <= 10000:
        raise ValueError("CSV batch conversion requires 1–10,000 rows")
    if after + len(table.records) >= 2**63:
        raise ValueError("CSV batch would exceed the signed offset range")
    source_sha = table.sources[0]
    events = []
    for index, row in enumerate(table.json_rows(), 1):
        key = {name: row[name] for name in definition["keys"]}
        normalized_key(contract, definition["keys"], key)
        events.append(
            {
                "id": fingerprint([source_sha, index]),
                "offset": after + index,
                "operation": "upsert",
                "key": key,
                "at": row[definition["event_time_column"]],
                "row": row,
            }
        )
    result = {
        "version": 1,
        "after": after,
        "through": after + len(events),
        "source_sha256": [source_sha],
        "events": events,
    }
    batch_definition(json_bytes(result, MAX_BATCH_BYTES), definition)
    return result


