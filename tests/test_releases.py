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

    def test_concurrent_alias_updates_have_one_generation_winner(self):
        self.catalog.move_alias(
            "research", "orders-current", self.table["id"], expected_generation=0
        )

        def update(_):
            try:
                Catalog(self.root / "catalog").move_alias(
                    "research",
                    "orders-current",
                    self.table["id"],
                    expected_generation=1,
                )
                return True
            except CatalogError:
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(update, range(2))), 1)
        self.assertEqual(
            self.catalog.alias("research", "orders-current")["generation"], 2
        )

    def test_prerequisite_and_generation_gate_local_promotion(self):
        self.catalog.create_environment("research", "staging")
        self.catalog.create_environment("research", "production", requires="staging")
        with self.assertRaisesRegex(CatalogError, "prerequisite"):
            self.catalog.promote(
                "research",
                "production",
                self.release["id"],
                expected_generation=0,
                request_key="too-early",
            )
        self.catalog.promote(
            "research",
            "staging",
            self.release["id"],
            expected_generation=0,
            request_key="stage",
        )
        result = self.catalog.promote(
            "research",
            "production",
            self.release["id"],
            expected_generation=0,
            request_key="promote",
        )
        self.assertEqual(result["generation"], 1)
        self.assertTrue(result["verification"]["passed"])
        self.assertEqual(
            result,
            self.catalog.promote(
                "research",
                "production",
                self.release["id"],
                expected_generation=0,
                request_key="promote",
            ),
        )
        with self.assertRaisesRegex(CatalogError, "generation"):
            self.catalog.promote(
                "research",
                "production",
                self.release["id"],
                expected_generation=0,
                request_key="another",
            )

    def test_corrupt_ancestor_blocks_build_only_release_verification(self):
        release = self.catalog.create_release(
            "research",
            "build-only",
            {"app": {"artifact": self.build["id"]}},
            request_key="build-only",
        )
        self.catalog.objects.path(self.table["sha256"]).write_bytes(b"corrupt parent")
        with self.assertRaises(ValueError):
            self.catalog.verify_release("research", release["id"])
        self.catalog.create_environment("research", "staging")
        with self.assertRaises(ValueError):
            self.catalog.promote(
                "research",
                "staging",
                release["id"],
                expected_generation=0,
                request_key="stage",
            )
        self.assertIsNone(self.catalog.environment("research", "staging")["release_id"])

