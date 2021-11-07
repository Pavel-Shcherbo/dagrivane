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

