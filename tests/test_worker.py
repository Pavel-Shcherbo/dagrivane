import concurrent.futures
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from dagrivane.control import ControlStore
from dagrivane.worker import Worker


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = ControlStore(self.root / "control.sqlite")
        self.workspace = self.store.create_workspace("Local")["id"]
        self.worker = Worker(
            self.store, self.root / "tasks", "test-worker", lease_seconds=5
        )

