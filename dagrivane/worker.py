"""Claim a durable run and execute it while retaining fenced ownership."""

import threading
from pathlib import Path

from .control import Conflict, LostLease, text
from .engine import run


class Worker:
    def __init__(self, store, state_directory, identity, lease_seconds=30):
        text(identity, "worker identity")
        store.lease_duration(lease_seconds)
        self.store = store
        self.identity = identity
        self.lease_seconds = lease_seconds
        self.state_directory = Path(state_directory)
        self.state_directory.mkdir(parents=True, exist_ok=True)

