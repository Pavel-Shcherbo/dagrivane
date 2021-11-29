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

