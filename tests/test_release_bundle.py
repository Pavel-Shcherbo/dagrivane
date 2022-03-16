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

    def test_independent_export_includes_and_verifies_parent_closure(self):
        release = self.catalog.create_release(
            "research",
            "build-only",
            {"app": {"artifact": self.build["id"]}},
            request_key="build-only",
        )
        target = self.export(release)
        descriptor = json.loads((target / "release.json").read_text())
        self.assertEqual(len(descriptor["artifacts"]), 2)
        self.assertEqual(
            {item["artifact"]["id"] for item in descriptor["artifacts"]},
            {self.table["id"], self.build["id"]},
        )
        self.assertTrue(verify_export(target)["passed"])
        # The verifier requires only the export, not catalog files or handles.
        self.catalog.path.rename(self.root / "hidden.sqlite")
        self.assertTrue(verify_export(target)["passed"])

    def test_export_never_overwrites_an_existing_directory(self):
        target = self.root / "export"
        target.mkdir()
        (target / "previous").write_text("keep")
        with self.assertRaises(FileExistsError):
            export_release(self.catalog, "research", self.release["id"], target)
        self.assertEqual((target / "previous").read_text(), "keep")
        self.assertFalse(list(self.root.glob(".dagrivane-release-*")))

    def test_tampered_object_is_detected_by_offline_verifier(self):
        target = self.export()
        (target / "objects" / self.table["sha256"]).write_bytes(b"tampered")
        with self.assertRaisesRegex(CatalogError, "hash or byte"):
            verify_export(target)

    def test_manifest_requirements_are_rechecked_even_if_descriptor_hash_changes(self):
        target = self.export()
        path = target / "release.json"
        data = json.loads(path.read_text())
        data["release"]["requirements"]["app"]["kind"] = "table"
        data["release_sha256"] = fingerprint(data["release"])
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(CatalogError, "requirement"):
            verify_export(target)

