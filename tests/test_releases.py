import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import CatalogError
from catalog_fixtures import prepare, write_bundle


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.catalog = Catalog(self.root / "catalog")
        self.table, self.build, self.release = prepare(self.catalog, self.root)

    def test_release_pins_alias_and_request_retry_does_not_resolve_it_again(self):
        alias = self.catalog.move_alias(
            "research", "orders-current", self.table["id"], expected_generation=0
        )
        pinned = self.catalog.create_release(
            "research",
            "alias-release",
            {"orders": {"alias": "orders-current"}},
            request_key="alias-release",
        )
        second = self.catalog.publish_table(
            "research",
            "orders",
            1,
            write_bundle(self.root),
            run_id="data-run-2",
            request_key="second",
        )
        self.catalog.move_alias(
            "research",
            "orders-current",
            second["id"],
            expected_generation=alias["generation"],
        )
        repeated = self.catalog.create_release(
            "research",
            "alias-release",
            {"orders": {"alias": "orders-current"}},
            request_key="alias-release",
        )
        self.assertEqual(pinned, repeated)
        self.assertEqual(
            pinned["manifest"]["bindings"]["orders"]["id"], self.table["id"]
        )

