"""Independent state, temporal, deduplication and catalog controls over schema forks."""

import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import fingerprint, json_bytes, parse_json
from dagrivane.ingest_store import IngestStore
from dagrivane.ingest_publish import configure_target, publish_pending
from dagrivane.ingest_audit import audit
from dagrivane.http_store import HTTPSourceStore
from dagrivane.http_client import HTTPPage
from dagrivane.stream_migration import (
    prepare_migration,
    configure_migration_policy,
    apply_migration,
    write_migration,
)
from dagrivane.stream_migration_seed import verify_base
from dagrivane.stream_migration_model import compile_projection, lossless_cast
from decimal import Decimal
from tests.test_ingest import definition, event, batch

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def proposal(source="orders", successor="orders-v2", old=None):
    old = old or definition()
    target = copy.deepcopy(old)
    target["contract"]["version"] += 1
    target["target"]["policy_version"] += 1
    target["contract"]["columns"].append(
        {"name": "currency", "type": "string", "enum": ["USD"]}
    )
    mappings = {
        column["name"]: {"operation": "copy", "source": column["name"]}
        for column in old["contract"]["columns"]
    }
    mappings["currency"] = {"operation": "constant", "value": "USD"}
    return {
        "version": 1,
        "source": source,
        "successor": successor,
        "definition": target,
        "columns": mappings,
        "drop_columns": [],
        "reason": "Expand the reviewed orders schema with explicit currency",
    }


def v2event(number, key=1, amount="10", day=4, operation="upsert", event_id=None):
    value = event(
        number, key=key, amount=amount, day=day, operation=operation, event_id=event_id
    )
    if value["row"] is not None:
        value["row"]["currency"] = "USD"
    return value


def resign(document):
    document["sha256"] = fingerprint(
        {k: v for k, v in document.items() if k != "sha256"}
    )
    return document


class StreamMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = IngestStore(self.root / "ingest.sqlite", create=True)
        self.store.register("orders", definition())
        self.catalog = Catalog(self.root / "catalog")
        configure_target(self.store, "orders", self.catalog)
        self.original = self.store.ingest(
            "orders",
            batch(
                0,
                [
                    event(1, day=2),
                    event(2, key=2, day=2),
                    event(3, key=2, day=3, operation="delete"),
                ],
            ),
            request_key="initial",
        )
        self.original_artifact = publish_pending(self.store, "orders", self.catalog)[
            "published"
        ][0]["artifact"]

    def prepare(self, value=None):
        return prepare_migration(self.store, value or proposal(), catalog=self.catalog)

    def apply(self, document=None, key="schema-two"):
        document = document or self.prepare()
        configure_migration_policy(document, self.catalog)
        return apply_migration(
            self.store, document, catalog=self.catalog, request_key=key
        )

    def test_exact_state_offset_tombstones_and_publication_lineage(self):
        old = self.store.rows("orders")
        document = self.prepare()
        receipt = self.apply(document)
        self.assertEqual(self.store.rows("orders"), old)
        current = self.store.rows("orders-v2")
        self.assertEqual(current["offset"], 3)
        self.assertEqual(current["generation"], 1)
        self.assertEqual(
            current["rows"],
            [
                {
                    "id": 1,
                    "amount": "10",
                    "updated_at": "2020-01-02T12:00:00Z",
                    "currency": "USD",
                }
            ],
        )
        self.assertEqual(current["tombstones"], 1)
        self.assertEqual(receipt["bootstrap"]["after"], receipt["bootstrap"]["through"])
        artifact = publish_pending(self.store, "orders-v2", self.catalog)["published"][
            0
        ]["artifact"]
        self.assertEqual(artifact["parents"], [self.original_artifact["id"]])
        self.assertEqual(artifact["policy_version"], 2)
        self.assertTrue(audit(self.store, "orders-v2", catalog=self.catalog)["passed"])
        self.assertTrue(
            self.catalog.verify_artifact("research", self.original_artifact["id"])[
                "passed"
            ]
        )

    def test_retired_source_blocks_new_writes_but_allows_receipt_recovery(self):
        self.apply()
        with self.assertRaisesRegex(ValueError, "retired"):
            self.store.ingest("orders", batch(3, [event(4, day=4)]), request_key="late")
        repeated = self.store.ingest(
            "orders",
            batch(
                0,
                [
                    event(1, day=2),
                    event(2, key=2, day=2),
                    event(3, key=2, day=3, operation="delete"),
                ],
            ),
            request_key="initial",
        )
        self.assertTrue(repeated["reused"])
        self.assertEqual(
            self.store.status("orders")["retired_by_migration"]["successor"],
            "orders-v2",
        )
        self.assertTrue(audit(self.store, "orders", catalog=self.catalog)["passed"])

    def test_preserved_tombstone_and_watermark_prevent_stale_resurrection(self):
        self.apply()
        result = self.store.ingest(
            "orders-v2",
            batch(3, [v2event(4, key=2, day=2), v2event(5, key=3, amount="30", day=4)]),
            request_key="continue",
        )
        self.assertEqual(result["counts"], {"applied": 1, "duplicate": 0, "stale": 1})
        self.assertEqual(
            [r["id"] for r in self.store.rows("orders-v2")["rows"]], [1, 3]
        )
        self.assertEqual(self.store.rows("orders-v2")["tombstones"], 1)
        self.assertEqual(self.store.status("orders-v2")["seen_events"], 5)
        publish_pending(self.store, "orders-v2", self.catalog)
        self.assertTrue(audit(self.store, "orders-v2", catalog=self.catalog)["passed"])

    def test_inherited_event_ids_cannot_be_rewritten_under_the_new_contract(self):
        self.apply()
        with self.assertRaisesRegex(ValueError, "event ID"):
            self.store.ingest(
                "orders-v2",
                batch(3, [v2event(4, event_id="event-1")]),
                request_key="duplicate-id",
            )
        self.assertEqual(self.store.status("orders-v2")["offset"], 3)

    def test_apply_retry_after_restart_is_identical(self):
        document = self.prepare()
        first = self.apply(document)
        self.store = IngestStore(self.store.path)
        self.assertEqual(
            apply_migration(
                self.store, document, catalog=self.catalog, request_key="schema-two"
            ),
            first,
        )
        self.assertEqual(self.store.status("orders-v2")["generation"], 1)
        with self.assertRaises(ValueError):
            self.apply(document, key="another-key")

    def test_stale_review_rejects_checkpoint_advance_even_without_row_changes(self):
        document = self.prepare()
        configure_migration_policy(document, self.catalog)
        self.store.ingest(
            "orders", batch(3, [], through=4), request_key="empty-advance"
        )
        publish_pending(self.store, "orders", self.catalog)
        with self.assertRaisesRegex(ValueError, "changed"):
            apply_migration(
                self.store, document, catalog=self.catalog, request_key="stale"
            )
        with self.assertRaises(ValueError):
            self.store.status("orders-v2")
        self.assertNotIn("retired_by_migration", self.store.status("orders"))

    def test_pending_publications_block_review_and_cutover(self):
        self.store.ingest("orders", batch(3, [event(4, day=4)]), request_key="pending")
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertNotIn("retired_by_migration", self.store.status("orders"))

