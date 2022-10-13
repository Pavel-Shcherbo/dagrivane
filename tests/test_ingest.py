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

    def test_out_of_order_events_do_not_resurrect_deleted_state(self):
        self.ingest(0, [event(1, day=3, operation="delete")])
        result = self.ingest(1, [event(2, day=2)], key="stale")
        self.assertEqual(result["counts"]["stale"], 1)
        self.assertEqual(self.store.rows("orders")["rows"], [])
        result = self.ingest(
            2, [event(3, day=3, amount="15")], key="same-time-new-offset"
        )
        self.assertEqual(result["counts"]["applied"], 1)
        self.assertEqual(self.store.rows("orders")["rows"][0]["amount"], "15")

    def test_watermark_boundary_and_failed_quality_leave_no_checkpoint(self):
        custom = definition()
        custom["allowed_lateness_seconds"] = 86400
        self.store.register("strict", custom)
        self.store.ingest("strict", batch(0, [event(1, day=3)]), request_key="first")
        self.store.ingest(
            "strict", batch(1, [event(2, key=2, day=2)]), request_key="boundary"
        )
        for raw in [
            batch(2, [event(3, key=3, day=1)]),
            batch(2, [event(3, key=3, day=3, amount="999")]),
        ]:
            with self.assertRaises(ValueError):
                self.store.ingest("strict", raw, request_key="rejected")
        status = self.store.status("strict")
        self.assertEqual(status["offset"], 2)
        self.assertEqual(status["generation"], 2)
        self.assertTrue(audit(self.store, "strict")["passed"])

    def test_offset_gap_and_empty_source_window_are_explicit(self):
        self.ingest(0, [], through=50)
        self.ingest(50, [event(100)], key="gapped")
        self.assertEqual(self.store.status("orders")["offset"], 100)
        with self.assertRaisesRegex(ValueError, "checkpoint"):
            self.ingest(50, [event(101)], key="overlap")
        self.assertTrue(audit(self.store, "orders")["passed"])

    def test_invalid_contract_rows_keys_clocks_and_batch_order(self):
        bad = [
            {**event(1), "key": {"id": 2}},
            {**event(1), "at": "2020-01-02T12:00:00Z"},
            {**event(1), "operation": "drop"},
            {**event(1), "offset": True},
            {
                **event(1),
                "row": {"id": 1, "amount": 1.5, "updated_at": "2020-01-01T12:00:00Z"},
            },
        ]
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.ingest(0, [value])
        with self.assertRaises(ValueError):
            self.ingest(0, [event(2), event(1)])
        with self.assertRaises(ValueError):
            self.ingest(0, [event(1), event(2, event_id="event-1")])
        self.assertEqual(self.store.status("orders")["generation"], 0)

    def test_decimal_key_normalization_and_private_context(self):
        from decimal import localcontext

        custom = definition()
        custom["contract"]["columns"][0]["type"] = "decimal"
        self.store.register("decimal", custom)
        first = event(1, key="1.0")
        second = event(2, key="1.00", day=2)
        with localcontext() as context:
            context.prec = 2
            self.store.ingest(
                "decimal", batch(0, [first, second]), request_key="decimal"
            )
        self.assertEqual(len(self.store.rows("decimal")["rows"]), 1)

    def test_stream_immutability_missing_store_and_unrelated_database(self):
        self.assertEqual(self.store.register("orders", definition())["generation"], 0)
        changed = definition()
        changed["allowed_lateness_seconds"] = 0
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.store.register("orders", changed)
        missing = self.root / "missing.sqlite"
        with self.assertRaises(ValueError):
            IngestStore(missing)
        self.assertFalse(missing.exists())
        unrelated = self.root / "other.sqlite"
        with closing(sqlite3.connect(unrelated)) as db, db:
            db.execute("CREATE TABLE unrelated(id INTEGER)")
        with self.assertRaisesRegex(ValueError, "unrelated"):
            IngestStore(unrelated, create=True)

    def test_concurrent_exact_retries_and_conflicting_offsets(self):
        raw = batch(0, [event(1)])

        def retry(_):
            return IngestStore(self.store.path).ingest(
                "orders", raw, request_key="same"
            )

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(retry, range(8)))
        self.assertEqual(sum(not row["reused"] for row in results), 1)

        def next_batch(number):
            try:
                return self.ingest(1, [event(2, key=number)], key="race-" + str(number))
            except ValueError:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(next_batch, [3, 4]))
        self.assertEqual(sum(row is not None for row in results), 1)
        self.assertTrue(audit(self.store, "orders")["passed"])

    def test_publisher_recovers_after_catalog_commit_before_ack(self):
        self.ingest(0, [event(1)])
        configure_target(self.store, "orders", self.catalog)

        class Crash(BaseException):
            pass

        def crash(_):
            raise Crash()

        with self.assertRaises(Crash):
            publish_next(self.store, "orders", self.catalog, after_publish=crash)
        self.assertEqual(
            self.store.status("orders")["publication_counts"], {"leased": 1}
        )
        self.assertEqual(len(self.catalog.list_artifacts("research")["artifacts"]), 1)
        future = datetime.now(timezone.utc) + timedelta(seconds=60)
        with patch("dagrivane.ingest_store.utc", return_value=future):
            result = publish_next(IngestStore(self.store.path), "orders", self.catalog)
        self.assertTrue(result["published"])
        self.assertEqual(len(self.catalog.list_artifacts("research")["artifacts"]), 1)
        self.assertEqual(
            self.store.batches("orders")["items"][0]["publication"]["attempts"], 2
        )
        self.assertTrue(audit(self.store, "orders", catalog=self.catalog)["passed"])

    def test_expired_worker_ack_is_fenced(self):
        self.ingest(0, [event(1)])
        configure_target(self.store, "orders", self.catalog)
        old = self.store.claim("orders", self.catalog.root, lease_seconds=1)
        path = self.root / "bundle.json"
        path.write_bytes(old["bundle"])
        target = old["definition"]["target"]
        artifact = self.catalog.publish_table(
            target["workspace"],
            target["dataset"],
            1,
            path,
            run_id=old["request_key"],
            request_key=old["request_key"],
        )
        future = datetime.now(timezone.utc) + timedelta(seconds=10)
        with patch("dagrivane.ingest_store.utc", return_value=future):
            with self.assertRaisesRegex(ValueError, "fenced"):
                self.store.acknowledge(old, artifact)
            newer = self.store.claim("orders", self.catalog.root)
            with self.assertRaisesRegex(ValueError, "fenced"):
                self.store.acknowledge(old, artifact)
            self.store.acknowledge(newer, artifact)
        self.assertTrue(audit(self.store, "orders", catalog=self.catalog)["passed"])

    def test_concurrent_publishers_create_one_artifact(self):
        self.ingest(0, [event(1)])
        configure_target(self.store, "orders", self.catalog)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(
                pool.map(
                    lambda _: publish_next(self.store, "orders", self.catalog), range(4)
                )
            )
        self.assertEqual(sum(row["published"] for row in results), 1)
        self.assertEqual(len(self.catalog.list_artifacts("research")["artifacts"]), 1)

    def test_destination_binding_is_immutable(self):
        self.ingest(0, [event(1)])
        configure_target(self.store, "orders", self.catalog)
        publish_next(self.store, "orders", self.catalog)
        other = Catalog(self.root / "other-catalog")
        configure_target(self.store, "orders", other)
        with self.assertRaisesRegex(ValueError, "bound"):
            publish_next(self.store, "orders", other)
        self.assertEqual(len(other.list_artifacts("research")["artifacts"]), 0)

    def test_audit_detects_materialized_row_corruption(self):
        self.ingest(0, [event(1)])
        self.ingest(1, [event(2, key=2)], key="next")
        self.assertFalse(audit(self.store, "orders", limit=1)["passed"])
        with closing(sqlite3.connect(self.store.path)) as db, db:
            db.execute(
                "UPDATE records SET payload=replace(payload,'10','11') WHERE stream='orders'"
            )
        self.assertFalse(audit(self.store, "orders")["passed"])
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            self.store.rows("orders")

    def test_seeded_event_timeline_matches_independent_key_oracle(self):
        import random

        rng = random.Random(108)
        events = []
        oracle = {}
        for number in range(1, 101):
            key = rng.randint(1, 20)
            day = rng.randint(1, 10)
            op = "delete" if rng.random() < 0.2 else "upsert"
            value = event(
                number, key=key, amount=str(rng.randint(0, 20)), day=day, operation=op
            )
            events.append(value)
            if key not in oracle or (day, number) >= oracle[key][:2]:
                oracle[key] = (day, number, value["row"])
        self.ingest(0, events)
        expected = {
            key: row["amount"]
            for key, (_, __, row) in oracle.items()
            if row is not None
        }
        actual = {row["id"]: row["amount"] for row in self.store.rows("orders")["rows"]}
        self.assertEqual(actual, expected)
        self.assertTrue(audit(self.store, "orders")["passed"])

    def test_csv_conversion_pins_duplicates_and_create_only_output(self):
        path = self.root / "orders.csv"
        path.write_text(
            "id,amount,updated_at\n1,10,2020-01-01T12:00:00Z\n2,20,2020-01-01T12:00:00Z\n"
        )
        normalized = self.store.status("orders")["definition"]
        value = csv_batch(normalized, path, after=0)
        output = self.root / "batch.json"
        write_batch(value, output)
        self.store.ingest_file("orders", output, request_key="csv")
        repeated = csv_batch(normalized, path, after=2)
        result = self.store.ingest(
            "orders", json_bytes(repeated), request_key="csv-repeat"
        )
        self.assertEqual(result["counts"]["duplicate"], 2)
        with self.assertRaises(FileExistsError):
            write_batch(value, output)
        with self.assertRaisesRegex(ValueError, "pinned"):
            csv_batch(normalized, path, after=4, expected_sha256="0" * 64)
        self.assertTrue(audit(self.store, "orders")["passed"])

