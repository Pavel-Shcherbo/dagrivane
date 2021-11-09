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

