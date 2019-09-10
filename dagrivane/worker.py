"""Claim a durable run and execute it while retaining fenced ownership."""

import threading
from pathlib import Path

from .control import Conflict, LostLease, text
from .engine import run


class Worker:
    pass
