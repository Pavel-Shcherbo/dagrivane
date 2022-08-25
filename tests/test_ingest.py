"""Incremental recovery, temporal ordering and independent row-state controls."""

import copy
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import json_bytes
from dagrivane.ingest_model import batch_definition, stream_definition
from dagrivane.ingest_store import IngestStore
from dagrivane.ingest_audit import audit
from dagrivane.ingest_publish import configure_target, publish_next, publish_pending
from dagrivane.ingest_source import csv_batch, write_batch


def definition():
    return {
        "version": 1,
        "contract": {
            "name": "orders",
            "version": 1,
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "amount", "type": "decimal", "min": "0"},
                {"name": "updated_at", "type": "timestamp"},
            ],
        },
        "keys": ["id"],
        "event_time_column": "updated_at",
        "allowed_lateness_seconds": 86400 * 10,
        "gates": [
            {"id": "ids", "kind": "unique", "columns": ["id"]},
            {"id": "budget", "kind": "sum", "columns": ["amount"], "max": "1000"},
        ],
        "target": {"workspace": "research", "dataset": "orders", "policy_version": 1},
    }


def event(number, key=1, amount="10", day=1, operation="upsert", event_id=None):
    at = f"2020-01-{day:02d}T12:00:00Z"
    return {
        "id": event_id or "event-" + str(number),
        "offset": number,
        "operation": operation,
        "key": {"id": key},
        "at": at,
        "row": (
            {"id": key, "amount": amount, "updated_at": at}
            if operation == "upsert"
            else None
        ),
    }


def batch(after, events, through=None):
    return json_bytes(
        {
            "version": 1,
            "after": after,
            "through": through if through is not None else events[-1]["offset"],
            "events": events,
        }
    )


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = IngestStore(self.root / "ingest.sqlite", create=True)
        self.store.register("orders", definition())
        self.catalog = Catalog(self.root / "catalog")

