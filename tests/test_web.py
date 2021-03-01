import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagrivane.web import evaluate


class WorkbenchTests(unittest.TestCase):
    def test_typed_pipeline_success_and_quality_failure_use_separate_workspaces(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict("os.environ", {"DAGRIVANE_STATE_DIR": folder}),
        ):
            successful = evaluate({"mode": "dataflow"})
            self.assertTrue(successful["succeeded"])
            self.assertEqual(
                [task["id"] for task in successful["tasks"]],
                ["validate", "aggregate", "release-check"],
            )
            blocked = evaluate({"mode": "dataflow", "fail_quality": True})
            self.assertFalse(blocked["succeeded"])
            self.assertEqual(
                [task["status"] for task in blocked["tasks"]],
                ["failed", "blocked", "blocked"],
            )
            self.assertNotEqual(successful["run_id"], blocked["run_id"])
            success_dir = Path(folder) / "demo-inputs" / successful["run_id"]
            failure_dir = Path(folder) / "demo-inputs" / blocked["run_id"]
            self.assertTrue((success_dir / ".local/typed-summary.json").exists())
            self.assertFalse((failure_dir / ".local/typed-summary.json").exists())

