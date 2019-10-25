import concurrent.futures
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from dagrivane.control import Conflict, ControlStore, LostLease, NotFound


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / "control.db"
        self.store = ControlStore(self.path)
        self.workspace = self.store.create_workspace("Research", concurrency=2, now=1)[
            "id"
        ]
        self.spec = {
            "tasks": [{"id": "check", "command": [sys.executable, "-c", "print('ok')"]}]
        }
        self.workflow = self.store.register_workflow(
            self.workspace, "Control", self.spec, self.root, now=2
        )

