"""Durable workbench operations, scoped workers and isolated input provenance."""

import concurrent.futures
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from dagrivane.control import ControlStore, Conflict, NotFound
from dagrivane.operations import Operations
from dagrivane.run_workspace import create_run_workspace
from dagrivane.worker import Worker
from dagrivane.web import evaluate


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "ops"
        self.operations = Operations(self.root)

    def initialize(self):
        state = self.operations.initialize()
        return state, ControlStore(self.operations.database)

    def test_overview_does_not_initialize_and_bootstrap_is_atomic(self):
        self.assertFalse(self.operations.overview()["initialized"])
        self.assertFalse(self.root.exists())
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            states = list(pool.map(lambda _: self.operations.initialize(), range(4)))
        self.assertEqual(len({state["workspace"]["id"] for state in states}), 1)
        store = ControlStore(self.operations.database)
        self.assertEqual(len(store.list_workspaces()), 1)
        self.assertEqual(len(states[0]["versions"]), 1)

