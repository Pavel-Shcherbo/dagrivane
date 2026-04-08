"""Coherent calendar views and input-bound local operator response receipts."""

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
import uuid

from .control import ControlError, ControlStore, canonical, text
from .operations import Operations
from .schedule_clock import instant, iso
from .schedule_evidence import export_schedule, verify_schedule_evidence
from .schedule_model import compile_schedule, digest, preview_schedule
from .schedule_store import ScheduleStore, record

MAX_RESPONSE = 16 * 1024 * 1024


def fields(value, required, optional=()):
    if (
        not isinstance(value, dict)
        or not set(required) <= set(value)
        or set(value) - set(required) - set(optional)
    ):
        raise ValueError("calendar request has missing or unsupported fields")


