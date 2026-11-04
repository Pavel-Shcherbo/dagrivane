import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]


class WorkbenchPortTests(unittest.TestCase):
    def run_invalid_port(self, value):
        with tempfile.TemporaryDirectory() as state:
            env = os.environ.copy()
            env["DAGRIVANE_STATE_DIR"] = state
            result = subprocess.run(
                [sys.executable, "-m", "dagrivane.web", "--port", value],
                cwd=ROOT, env=env, capture_output=True, text=True, timeout=10,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("port must be an integer from 0 to 65535", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_out_of_range_ports_are_usage_errors(self):
        for value in ("-1", "65536", "999999999999999999999999999999"):
            with self.subTest(port=value):
                self.run_invalid_port(value)

    def test_non_numeric_port_is_a_usage_error(self):
        self.run_invalid_port("not-a-port")

    def test_port_zero_serves_the_workbench(self):
        with tempfile.TemporaryDirectory() as state:
            env = os.environ.copy()
            env["DAGRIVANE_STATE_DIR"] = state
            process = subprocess.Popen(
                [sys.executable, "-m", "dagrivane.web", "--port", "0"],
                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
            )
            lines = queue.Queue()
            reader = threading.Thread(
                target=lambda: lines.put(process.stdout.readline()), daemon=True
            )
            reader.start()
            try:
                line = lines.get(timeout=10)
                match = re.fullmatch(r"Workbench: (http://127\.0\.0\.1:([0-9]+))\n", line)
                self.assertIsNotNone(match, line)
                self.assertGreater(int(match.group(2)), 0)
                with urlopen(match.group(1), timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn("Dagrivane workbench", response.read().decode())
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)
                reader.join(timeout=1)


if __name__ == "__main__":
    unittest.main()
