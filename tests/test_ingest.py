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

    def ingest(self, after, events, key="load", through=None):
        return self.store.ingest(
            "orders", batch(after, events, through), request_key=key
        )

    def test_upsert_delete_tombstone_and_replay(self):
        first = self.ingest(0, [event(1), event(2, key=2, amount="20")])
        second = self.ingest(
            2,
            [
                event(3, amount="15", day=2),
                event(4, key=2, day=3, operation="delete"),
                event(5, key=3, amount="30", day=3),
            ],
            key="next",
        )
        rows = self.store.rows("orders")
        self.assertEqual(
            {r["id"]: r["amount"] for r in rows["rows"]}, {1: "15", 3: "30"}
        )
        self.assertEqual(rows["tombstones"], 1)
        self.assertEqual(second["previous_state_sha256"], first["state_sha256"])
        self.assertTrue(audit(self.store, "orders")["passed"])
        configure_target(self.store, "orders", self.catalog)
        published = publish_pending(self.store, "orders", self.catalog)
        self.assertEqual(len(published["published"]), 2)
        self.assertEqual(
            published["published"][1]["artifact"]["parents"],
            [published["published"][0]["artifact"]["id"]],
        )
        self.assertTrue(audit(self.store, "orders", catalog=self.catalog)["passed"])

    def test_exact_batch_retry_and_request_conflict(self):
        raw = batch(0, [event(1)])
        first = self.store.ingest("orders", raw, request_key="same")
        retry = self.store.ingest("orders", raw, request_key="same")
        self.assertTrue(retry["reused"])
        self.assertEqual(retry["batch_id"], first["batch_id"])
        self.assertTrue(self.store.ingest("orders", raw, request_key="other")["reused"])
        with self.assertRaisesRegex(ValueError, "conflicts"):
            self.store.ingest("orders", raw + b" ", request_key="same")
        self.assertEqual(self.store.status("orders")["generation"], 1)

    def test_duplicate_event_at_new_offset_does_not_reapply(self):
        self.ingest(0, [event(1)])
        self.ingest(1, [event(2, day=2, operation="delete")], key="delete")
        duplicate = event(1)
        duplicate["offset"] = 3
        result = self.ingest(2, [duplicate], key="retry-delivery")
        self.assertEqual(result["counts"]["duplicate"], 1)
        self.assertEqual(self.store.rows("orders")["rows"], [])
        self.assertTrue(audit(self.store, "orders")["passed"])

    def test_conflicting_event_identity_is_atomic(self):
        self.ingest(0, [event(1)])
        with self.assertRaisesRegex(ValueError, "different content"):
            self.ingest(
                1,
                [event(2, key=2), event(3, amount="12", event_id="event-1")],
                key="bad",
            )
        self.assertEqual(self.store.status("orders")["offset"], 1)
        self.assertEqual(len(self.store.rows("orders")["rows"]), 1)

