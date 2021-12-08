import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from catalog_fixtures import contract, gates, write_bundle

ROOT = Path(__file__).resolve().parents[1]


class CatalogCliTests(unittest.TestCase):
    def command(self, store, *arguments):
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "dagrivane.catalog_cli",
                "--store",
                str(store),
                *map(str, arguments),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertEqual(process.returncode, 0, process.stderr)
        return json.loads(process.stdout)

    def test_operator_lifecycle_exports_and_verifies_without_a_live_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = root / "catalog"
            policy = root / "policy.json"
            policy.write_text(
                json.dumps({"contract": contract(), "version": 1, "gates": gates()})
            )
            self.command(store, "workspace-create", "research")
            self.command(store, "policy-register", "research", "orders", policy)
            artifact = self.command(
                store,
                "publish-table",
                "research",
                "orders",
                write_bundle(root),
                "--policy-version",
                "1",
                "--run-id",
                "data",
                "--request-key",
                "publish",
            )
            release_definition = root / "release.json"
            release_definition.write_text(
                json.dumps({"bindings": {"orders": {"artifact": artifact["id"]}}})
            )
            release = self.command(
                store,
                "release-create",
                "research",
                "release-1",
                release_definition,
                "--request-key",
                "release",
            )
            self.assertTrue(
                self.command(store, "verify-release", "research", release["id"])[
                    "passed"
                ]
            )
            self.command(
                store, "export-release", "research", release["id"], root / "export"
            )
            self.assertTrue(
                self.command(
                    root / "nonexistent-catalog", "verify-export", root / "export"
                )["passed"]
            )
            self.assertFalse((root / "nonexistent-catalog").exists())

    def test_read_command_does_not_create_an_empty_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "missing"
            process = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "dagrivane.catalog_cli",
                    "--store",
                    str(root),
                    "artifacts",
                    "research",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(process.returncode, 2)
            self.assertFalse(root.exists())
