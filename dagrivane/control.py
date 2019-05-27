"""Versioned workflows, durable run submission and fenced worker ownership."""

import hashlib
import json
import math
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .engine import validate


class ControlError(Exception):
    """A rejected control-plane operation."""


class NotFound(ControlError):
    pass


class Conflict(ControlError):
    pass


class LostLease(ControlError):
    pass


STATES = {
    "queued",
    "running",
    "cancel_requested",
    "cancelled",
    "succeeded",
    "failed",
    "orphaned",
}
TERMINAL = {"cancelled", "succeeded", "failed", "orphaned"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


