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


def timestamp(now):
    value = time.time() if now is None else now
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("timestamp must be a finite nonnegative number")
    return value


def text(value, field, maximum=200):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{field} must contain 1 to {maximum} characters")
    return value


