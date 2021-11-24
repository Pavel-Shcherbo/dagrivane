import copy
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import CatalogError
from catalog_fixtures import contract, gates, prepare, write_bundle


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.catalog = Catalog(self.root / "catalog")
        self.catalog.create_workspace("research")
        self.catalog.register_policy("research", "orders", 1, contract(), gates())

    def publish(self, key="table-1", **options):
        path = options.pop("path") if "path" in options else write_bundle(self.root)
        return self.catalog.publish_table(
            "research", "orders", 1, path, run_id="data-run", request_key=key, **options
        )

    def test_table_publication_rechecks_contents_and_catalog_quality(self):
        record = self.publish()
        self.assertEqual(record["metadata"]["table"]["rows"], 2)
        self.assertEqual(record["metadata"]["quality_checks"][1]["observed"], "30.00")
        self.assertTrue(
            self.catalog.verify_artifact("research", record["id"])["passed"]
        )
        self.assertEqual(
            self.catalog.dataset_version("research", "orders", 1)["id"], record["id"]
        )
        self.assertTrue(self.catalog.audit("research")["passed"])

    def test_false_producer_success_cannot_bypass_independent_quality_gate(self):
        path = write_bundle(
            self.root, rows=[{"id": 1, "amount": "10"}, {"id": 1, "amount": "20"}]
        )
        with self.assertRaisesRegex(CatalogError, "independently"):
            self.publish(path=path)
        self.assertEqual(self.catalog.list_artifacts("research")["artifacts"], [])
        self.assertEqual(self.catalog.objects.inventory()["objects"], [])

    def test_tampered_rows_and_boolean_dimensions_are_rejected(self):
        for change in [
            lambda data: data["rows"][0].update(amount="999"),
            lambda data: data["report"]["output"].update(rows=True),
            lambda data: data["report"].update(passed=1),
        ]:
            path = write_bundle(self.root)
            data = json.loads(path.read_text())
            change(data)
            path.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                self.publish(path=path)

    def test_publication_idempotency_and_conflicts_are_explicit(self):
        first = self.publish()
        second = self.publish()
        self.assertEqual(first, second)
        changed = write_bundle(self.root, rows=[{"id": 1, "amount": "40"}])
        with self.assertRaisesRegex(CatalogError, "request key conflicts"):
            self.publish(path=changed)
        self.assertEqual(len(self.catalog.list_artifacts("research")["artifacts"]), 1)

    def test_concurrent_publishers_allocate_unique_dataset_versions(self):
        path = write_bundle(self.root)

        def publish(number):
            return Catalog(self.root / "catalog").publish_table(
                "research",
                "orders",
                1,
                path,
                run_id="data-run",
                request_key="publish-" + str(number),
            )

        with ThreadPoolExecutor(max_workers=4) as pool:
            records = list(pool.map(publish, range(8)))
        self.assertEqual(
            {record["metadata"]["dataset_version"] for record in records},
            set(range(1, 9)),
        )
        self.assertEqual(len({record["sha256"] for record in records}), 1)
        self.assertEqual(len(self.catalog.objects.inventory()["objects"]), 1)

    def test_policy_versions_are_immutable_and_breaks_require_explicit_choice(self):
        self.assertEqual(
            self.catalog.register_policy("research", "orders", 1, contract(), gates())[
                "version"
            ],
            1,
        )
        changed_gates = gates()
        changed_gates[1]["max"] = "999"
        with self.assertRaisesRegex(CatalogError, "immutable"):
            self.catalog.register_policy(
                "research", "orders", 1, contract(), changed_gates
            )
        changed = contract(2)
        changed["columns"].append({"name": "note", "type": "string", "nullable": True})
        with self.assertRaisesRegex(CatalogError, "breaking"):
            self.catalog.register_policy("research", "orders", 2, changed, gates())
        accepted = self.catalog.register_policy(
            "research", "orders", 2, changed, gates(), allow_breaking=True
        )
        self.assertFalse(accepted["compatibility"]["compatible"])

    def test_namespace_scope_applies_to_lookup_parent_links_and_release_refs(self):
        record = self.publish()
        self.catalog.create_workspace("other")
        with self.assertRaisesRegex(CatalogError, "unknown artifact"):
            self.catalog.artifact("other", record["id"])
        path = self.root / "app.txt"
        path.write_text("application")
        with self.assertRaisesRegex(CatalogError, "unknown artifact"):
            self.catalog.record_build(
                "other",
                "app",
                path,
                revision="a" * 40,
                run_id="build",
                request_key="record",
                parents=[record["id"]],
            )
        with self.assertRaises(CatalogError):
            self.catalog.create_release(
                "other",
                "release",
                {"orders": {"artifact": record["id"]}},
                request_key="release",
            )
        self.assertEqual(self.catalog.list_artifacts("other")["artifacts"], [])

    def test_lineage_limits_report_partial_scope_and_cursor_is_stable(self):
        first = self.publish()
        second = self.publish("table-2", parents=[first["id"]])
        third = self.publish("table-3", parents=[second["id"]])
        graph = self.catalog.lineage("research", third["id"])
        self.assertTrue(graph["complete"])
        self.assertEqual(len(graph["nodes"]), 3)
        self.assertFalse(
            self.catalog.lineage("research", third["id"], max_depth=1)["complete"]
        )
        descendants = self.catalog.lineage(
            "research", first["id"], direction="descendants"
        )
        self.assertEqual(
            {item["id"] for item in descendants["nodes"]},
            {first["id"], second["id"], third["id"]},
        )
        page = self.catalog.list_artifacts("research", limit=1)
        remainder = Catalog(self.root / "catalog").list_artifacts(
            "research", after=page["next_cursor"]
        )
        self.assertEqual(
            [item["id"] for item in remainder["artifacts"]], [second["id"], third["id"]]
        )

    def test_audit_detects_policy_and_blob_corruption_without_deleting(self):
        record = self.publish()
        self.catalog.objects.path(record["sha256"]).write_bytes(b"broken")
        report = self.catalog.audit("research")
        self.assertFalse(report["passed"])
        self.assertFalse(report["deletion_performed"])
        with closing(sqlite3.connect(self.catalog.path)) as db, db:
            db.execute("UPDATE policies SET payload='{}'")
        with self.assertRaisesRegex(CatalogError, "fingerprint"):
            self.catalog.policy("research", "orders", 1)

    def test_orphans_are_reported_and_partial_audits_are_not_full_success(self):
        self.publish()
        self.publish("table-2")
        orphan = self.catalog.objects.put(b"unreferenced")["sha256"]
        report = self.catalog.audit("research", limit=1)
        self.assertFalse(report["passed"])
        self.assertFalse(report["complete"])
        self.assertIn(orphan, report["unreferenced_objects"])
        self.assertEqual(self.catalog.objects.get(orphan), b"unreferenced")

    def test_concurrent_cold_initialization_keeps_one_schema(self):
        target = self.root / "cold-catalog"

        def create(_):
            return Catalog(target).create_workspace("shared")

        with ThreadPoolExecutor(max_workers=6) as pool:
            records = list(pool.map(create, range(8)))
        self.assertTrue(all(record == records[0] for record in records))
        self.assertTrue(Catalog(target).audit("shared")["passed"])

