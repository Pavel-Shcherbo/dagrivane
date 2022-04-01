import hashlib
import tempfile
import unittest
from pathlib import Path

from dagrivane.release_demo import build_application, run_demo
from dagrivane.release_bundle import verify_export
from dagrivane.resources import examples_directory


class ReleaseControlTests(unittest.TestCase):
    def test_healthy_data_to_executable_package_and_local_pointers(self):
        with tempfile.TemporaryDirectory() as directory:
            report = run_demo(Path(directory) / "run")
            self.assertTrue(report["passed"])
            self.assertTrue(report["audit"]["passed"])
            self.assertEqual(
                report["application"]["result"],
                {"currencies": {"USD": "112.00"}, "orders": 4},
            )
            self.assertTrue(
                all(
                    environment["release_id"] == report["release"]["id"]
                    for environment in report["environments"]
                )
            )
            self.assertTrue(
                verify_export(Path(directory) / "run/release-export")["passed"]
            )
            self.assertEqual(len(report["lineage"]["nodes"]), 2)

    def test_corruption_and_missing_prerequisite_block_local_production_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            for scenario in ["corrupt-data", "missing-staging"]:
                with self.subTest(scenario=scenario):
                    report = run_demo(Path(directory) / scenario, scenario)
                    self.assertFalse(report["passed"])
                    self.assertIsNone(report["environments"][1]["release_id"])
                    self.assertIsNone(report["export"])
                    self.assertTrue(report["blocked_reason"])

    def test_application_archive_is_reproducible_and_tracks_source_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = examples_directory() / "summary_app.py"
            first = build_application(source, root / "first.pyz")
            second = build_application(source, root / "second.pyz")
            self.assertEqual(first, second)
            self.assertEqual(first, hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(
                (root / "first.pyz").read_bytes(), (root / "second.pyz").read_bytes()
            )
