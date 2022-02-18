import copy
import json
import tempfile
import unittest
from pathlib import Path

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import CatalogError, fingerprint
from dagrivane.release_bundle import export_release, verify_export
from catalog_fixtures import prepare


class ReleaseBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.catalog = Catalog(self.root / "catalog")
        self.table, self.build, self.release = prepare(self.catalog, self.root)

    def export(self, release=None):
        target = self.root / "export"
        export_release(
            self.catalog, "research", (release or self.release)["id"], target
        )
        return target

