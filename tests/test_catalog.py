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

