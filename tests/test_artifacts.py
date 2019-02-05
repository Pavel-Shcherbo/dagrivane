import sys
import tempfile
import unittest
from pathlib import Path

from dagrivane.artifacts import file_digest, verify_inputs
from dagrivane.engine import run


class ArtifactTests(unittest.TestCase):
    def test_changed_input_cannot_reuse_a_successful_checkpoint(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            data = root / "input.txt"
            data.write_text("original")
            spec = {
                "inputs": [{"path": "input.txt", "sha256": file_digest(data)}],
                "tasks": [
                    {
                        "id": "read",
                        "command": [
                            sys.executable,
                            "-c",
                            "from pathlib import Path; print(Path('input.txt').read_text())",
                        ],
                    }
                ],
            }
            run(spec, root / "runs.db", "one", root)
            data.write_text("changed")
            with self.assertRaisesRegex(ValueError, "content changed"):
                run(spec, root / "runs.db", "one", root)
            spec["inputs"][0]["sha256"] = file_digest(data)
            with self.assertRaisesRegex(ValueError, "different specification"):
                run(spec, root / "runs.db", "one", root)
            result = run(spec, root / "runs.db", "two", root)
            self.assertEqual(result[0]["output"].strip(), "changed")

    def test_missing_duplicate_and_escaping_inputs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "input").write_text("value")
            declaration = {"path": "input", "sha256": file_digest(root / "input")}
            for declarations in [
                [declaration, declaration],
                [{"path": "../outside", "sha256": "a" * 64}],
                [{"path": "missing", "sha256": "a" * 64}],
                [{"path": "input", "sha256": "bad"}],
            ]:
                with self.assertRaises(ValueError):
                    verify_inputs({"inputs": declarations}, root)
