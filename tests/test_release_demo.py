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

