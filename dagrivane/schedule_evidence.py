"""Coherent bounded schedule exports and independently checked decision chains."""

import json
from pathlib import Path

from .control import Conflict, canonical, STATES
from .schedule_clock import instant, MAX_TIME
from .schedule_model import compile_schedule, digest
from .schedule_store import record

FORMAT = "dagrivane.schedule-evidence.v1"
MAX_EVENTS = 5000
MAX_OCCURRENCES = 10000
MAX_BYTES = 16 * 1024 * 1024


