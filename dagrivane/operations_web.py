"""Validate loopback operation requests before accessing durable local state."""

import sqlite3
import uuid

from .control import ControlError, STATES, text
from .operations import Operations, PROFILES
from .resources import state_directory

FIELDS = {
    "overview": {"before", "status"},
    "initialize": set(),
    "publish": {"profile", "expected_version"},
    "submit": {"version", "event_key", "priority"},
    "inspect": {"run_id", "after"},
    "work": {"run_id"},
    "cancel": {"run_id"},
    "retry": {"run_id", "event_key"},
}


