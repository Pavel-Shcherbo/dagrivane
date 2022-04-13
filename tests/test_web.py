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

    def test_invalid_mode_is_rejected_before_creating_runtime_state(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict(
                "os.environ", {"DAGRIVANE_STATE_DIR": str(Path(folder) / "state")}
            ),
        ):
            with self.assertRaises(ValueError):
                evaluate({"mode": "unknown"})
            self.assertFalse((Path(folder) / "state").exists())

    def test_release_control_retains_isolated_catalogs_and_blocks_changed_data(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict("os.environ", {"DAGRIVANE_STATE_DIR": folder}),
        ):
            healthy = evaluate({"mode": "release"})
            self.assertTrue(healthy["passed"])
            self.assertTrue(Path(healthy["catalog_root"]).is_dir())
            changed = evaluate({"mode": "release", "scenario": "corrupt-data"})
            self.assertFalse(changed["passed"])
            self.assertNotEqual(healthy["run_id"], changed["run_id"])
            self.assertIsNone(changed["environments"][1]["release_id"])

    def test_release_controls_reject_unknown_parameters_before_state_creation(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict(
                "os.environ", {"DAGRIVANE_STATE_DIR": str(Path(folder) / "state")}
            ),
        ):
            for params in [
                {"mode": "release", "scenario": "unknown"},
                {"mode": "release", "command": ["arbitrary"]},
            ]:
                with self.assertRaises(ValueError):
                    evaluate(params)
            self.assertFalse((Path(folder) / "state").exists())
