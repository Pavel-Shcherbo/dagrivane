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

    def test_projection_failure_does_not_create_policy_or_successor(self):
        value = proposal()
        value["definition"]["contract"]["columns"][-1]["enum"] = ["EUR"]
        with self.assertRaises(ValueError):
            self.prepare(value)
        with self.assertRaises(ValueError):
            self.store.status("orders-v2")
        with self.assertRaises(ValueError):
            self.catalog.policy("research", "orders", 2)

    def test_quality_gate_failure_cannot_be_previewed_as_valid(self):
        value = proposal()
        value["definition"]["gates"][1]["max"] = "9"
        with self.assertRaisesRegex(ValueError, "quality gates"):
            self.prepare(value)

    def test_missing_target_policy_blocks_apply_without_freezing_source(self):
        document = self.prepare()
        with self.assertRaises(ValueError):
            apply_migration(
                self.store, document, catalog=self.catalog, request_key="missing-policy"
            )
        self.assertNotIn("retired_by_migration", self.store.status("orders"))

    def test_wrong_catalog_and_wrong_store_are_rejected(self):
        document = self.prepare()
        wrong = Catalog(self.root / "wrong")
        with self.assertRaises(ValueError):
            configure_migration_policy(document, wrong)
        another = IngestStore(self.root / "another.sqlite", create=True)
        configure_migration_policy(document, self.catalog)
        with self.assertRaisesRegex(ValueError, "another ingestion"):
            apply_migration(
                another, document, catalog=self.catalog, request_key="wrong-store"
            )

    def test_atomic_failure_rolls_back_new_base_records_retirement_and_outbox(self):
        document = self.prepare()
        configure_migration_policy(document, self.catalog)
        with (
            patch.object(
                self.store, "_ingest", side_effect=RuntimeError("interrupted bootstrap")
            ),
            self.assertRaises(RuntimeError),
        ):
            apply_migration(
                self.store, document, catalog=self.catalog, request_key="atomic"
            )
        with self.assertRaises(ValueError):
            self.store.status("orders-v2")
        self.assertNotIn("retired_by_migration", self.store.status("orders"))
        self.assertTrue(
            apply_migration(
                self.store, document, catalog=self.catalog, request_key="atomic"
            )["source_frozen"]
        )

    def test_concurrent_cutovers_share_one_idempotent_receipt(self):
        document = self.prepare()
        configure_migration_policy(document, self.catalog)

        def apply(_):
            return apply_migration(
                IngestStore(self.store.path),
                document,
                catalog=self.catalog,
                request_key="concurrent",
            )

        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(apply, range(4)))
        self.assertTrue(all(value == values[0] for value in values))
        self.assertEqual(self.store.status("orders-v2")["generation"], 1)

    def test_rehashed_projection_or_dedup_tampering_is_rejected(self):
        for mutate in [
            lambda d: d["records"]['{"id":1}']["row"].update(amount="999"),
            lambda d: d["seen"].pop("event-1"),
        ]:
            document = self.prepare()
            mutate(document)
            resign(document)
            with self.assertRaises(ValueError):
                verify_base(document)

    def test_corrupt_base_or_seen_cache_cannot_pass_replay(self):
        document = self.prepare()
        self.apply(document)
        with self.store._tx() as db:
            db.execute(
                "DELETE FROM migration_seen WHERE stream='orders-v2' AND event_id='event-1'"
            )
        self.assertFalse(audit(self.store, "orders-v2")["passed"])
        with self.store._tx() as db:
            db.execute(
                "UPDATE migration_bases SET sha256=? WHERE stream='orders-v2'",
                ("0" * 64,),
            )
        with self.assertRaises(ValueError):
            audit(self.store, "orders-v2")

    def test_keys_and_event_time_cannot_be_synthesized(self):
        for column in ["id", "updated_at"]:
            value = proposal()
            value["columns"][column] = {
                "operation": "constant",
                "value": 1 if column == "id" else "2020-01-01T00:00:00Z",
            }
            value["drop_columns"] = [column]
            with self.assertRaisesRegex(ValueError, "copy mappings"):
                compile_projection(definition(), value)

    def test_explicit_drop_and_exact_integral_cast(self):
        value = proposal()
        value["definition"]["contract"]["columns"][1]["type"] = "integer"
        value["columns"]["amount"] = {"operation": "cast", "source": "amount"}
        # Numeric constraints must use the declared new type as well.
        value["definition"]["contract"]["columns"][1]["min"] = 0
        value["definition"]["gates"][1]["max"] = 1000000
        doc = self.prepare(value)
        self.apply(doc)
        self.assertEqual(self.store.rows("orders-v2")["rows"][0]["amount"], 10)
        self.assertTrue(audit(self.store, "orders-v2")["passed"])
        with self.assertRaises(ValueError):
            lossless_cast(Decimal("1.5"), "decimal", "integer")
        with self.assertRaises(ValueError):
            lossless_cast("001", "string", "integer")
        self.assertEqual(lossless_cast("12", "string", "integer"), 12)
        self.assertEqual(lossless_cast(False, "boolean", "string"), "false")

    def test_drop_requires_exact_unused_column_list(self):
        value = proposal()
        value["definition"]["contract"]["columns"] = [
            c
            for c in value["definition"]["contract"]["columns"]
            if c["name"] != "amount"
        ]
        value["definition"]["gates"] = value["definition"]["gates"][:1]
        value["columns"].pop("amount")
        with self.assertRaisesRegex(ValueError, "drop_columns"):
            self.prepare(value)
        value["drop_columns"] = ["amount"]
        document = self.prepare(value)
        self.assertEqual(document["projection"]["dropped_columns"], ["amount"])
        self.apply(document)
        self.assertNotIn("amount", self.store.rows("orders-v2")["rows"][0])

    def test_empty_source_bootstrap_keeps_zero_checkpoint(self):
        self.store.register("empty", definition())
        value = proposal("empty", "empty-v2")
        document = self.prepare(value)
        self.apply(document, key="empty-migration")
        status = self.store.status("empty-v2")
        self.assertEqual(status["offset"], 0)
        self.assertEqual(status["generation"], 1)
        self.assertTrue(audit(self.store, "empty-v2")["passed"])

    def test_nested_migration_renames_keys_and_replays_full_ancestry(self):
        self.apply()
        publish_pending(self.store, "orders-v2", self.catalog)
        old = self.store.status("orders-v2")["definition"]
        target = copy.deepcopy(old)
        target["contract"]["version"] = 3
        target["target"]["policy_version"] = 3
        target["contract"]["columns"][0]["name"] = "order_id"
        target["keys"] = ["order_id"]
        target["gates"][0]["columns"] = ["order_id"]
        value = {
            "version": 1,
            "source": "orders-v2",
            "successor": "orders-v3",
            "definition": target,
            "columns": {
                ("order_id" if col["name"] == "id" else col["name"]): {
                    "operation": "copy",
                    "source": col["name"],
                }
                for col in old["contract"]["columns"]
            },
            "drop_columns": [],
            "reason": "Rename the key while preserving logical identity",
        }
        document = self.prepare(value)
        self.apply(document, key="schema-three")
        self.assertEqual(self.store.rows("orders-v3")["rows"][0]["order_id"], 1)
        self.assertEqual(self.store.rows("orders-v3")["tombstones"], 1)
        artifact = publish_pending(self.store, "orders-v3", self.catalog)["published"][
            0
        ]["artifact"]
        self.assertTrue(
            self.catalog.verify_artifact("research", artifact["id"])["passed"]
        )
        self.assertTrue(audit(self.store, "orders-v3", catalog=self.catalog)["passed"])

    def http_source(self):
        source = HTTPSourceStore(self.store)
        config = json.loads((EXAMPLES / "http-row-source.json").read_text())
        config["url"] = "http://127.0.0.1:1/orders"
        config["initial_cursor"] = "checkpoint-three"
        source.register("api", "orders", config)
        return source

    def test_active_http_claim_and_staged_page_block_migration(self):
        source = self.http_source()
        claim = source.claim("api", "worker")
        with self.assertRaisesRegex(ValueError, "HTTP source"):
            self.prepare()
        raw = json_bytes({"data": [], "next_cursor": "next", "has_more": False})
        page = HTTPPage(
            raw,
            {
                "status": 200,
                "entity_bytes": len(raw),
                "entity_sha256": hashlib.sha256(raw).hexdigest(),
            },
        )
        source.stage(claim, source.checkpoint(claim), page)
        source.release(claim)
        with self.assertRaisesRegex(ValueError, "HTTP source"):
            self.prepare()

    def test_expired_http_owner_cannot_fetch_or_heartbeat_after_cutover(self):
        source = self.http_source()
        claim = source.claim("api", "worker", now=1)
        document = self.prepare()
        self.apply(document)
        self.assertEqual(
            document["origin"]["connector"]["cursor"], '"checkpoint-three"'
        )
        with self.assertRaisesRegex(ValueError, "retired"):
            source.claim("api", "new-worker")
        with self.assertRaisesRegex(ValueError, "retired"):
            source.heartbeat(claim, now=2)

    def test_successor_http_mapping_resumes_retained_cursor_without_reset(self):
        source = self.http_source()
        document = self.prepare()
        self.apply(document)
        config = json.loads((EXAMPLES / "http-row-source.json").read_bytes())
        config["initial_cursor"] = json.loads(document["origin"]["connector"]["cursor"])
        config["response"]["columns"]["currency"] = "/currency"
        source.register("api-v2", "orders-v2", config)
        claim = source.claim("api-v2", "worker-v2")
        checkpoint = source.checkpoint(claim)
        self.assertEqual(checkpoint["cursor"], "checkpoint-three")
        self.assertEqual((checkpoint["offset"], checkpoint["generation"]), (3, 1))
        raw = json_bytes(
            {
                "data": [
                    {
                        "id": 3,
                        "amount": "30",
                        "updated_at": "2020-01-04T12:00:00Z",
                        "currency": "USD",
                        "revision": "http-four",
                    }
                ],
                "next_cursor": "checkpoint-four",
                "has_more": False,
            }
        )
        page = HTTPPage(
            raw,
            {
                "status": 200,
                "content_type": "application/json",
                "entity_bytes": len(raw),
                "entity_sha256": hashlib.sha256(raw).hexdigest(),
            },
        )
        page_id = source.stage(claim, checkpoint, page)
        result = source.apply(claim, page_id)
        self.assertEqual((result["offset_after"], result["generation"]), (4, 2))
        self.assertTrue(source.apply(claim, page_id)["reused"])
        source.release(claim)
        rows = self.store.rows("orders-v2")
        self.assertEqual([row["id"] for row in rows["rows"]], [1, 3])
        self.assertEqual(rows["tombstones"], 1)
        publish_pending(self.store, "orders-v2", self.catalog)
        from dagrivane.http_audit import audit_source

        self.assertTrue(audit_source(source, "api-v2")["passed"])
        self.assertTrue(audit(self.store, "orders-v2", catalog=self.catalog)["passed"])

    def test_bootstrap_cannot_be_submitted_to_an_unmigrated_stream(self):
        raw = json_bytes(
            {
                "version": 2,
                "kind": "migration_bootstrap",
                "base_sha256": "0" * 64,
                "after": 3,
                "through": 3,
                "events": [],
            }
        )
        with self.assertRaisesRegex(ValueError, "bootstrap"):
            self.store.ingest("orders", raw, request_key="forged-bootstrap")
        self.assertEqual(self.store.status("orders")["generation"], 1)

