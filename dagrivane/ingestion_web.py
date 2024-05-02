"""Strict browser controls over reviewed local source profiles and retained evidence."""

import sqlite3
from urllib.parse import parse_qs, urlsplit

from .catalog_contracts import token
from .ingestion_desk import IngestionDesk, digest, loopback_origin
from .ingestion_views import require_profile
from .resources import state_directory

ORIGIN = None
FIELDS = {
    "overview": set(),
    "initialize": set(),
    "inspect": {"profile", "after_page", "after_generation", "row_offset"},
    "poll": {"profile", "request_key", "expected_revision"},
    "publish": {"profile", "request_key", "expected_revision"},
    "repair": {"profile", "request_key", "expected_revision", "page_id", "reason"},
    "discard": {"profile", "request_key", "expected_revision", "page_id", "reason"},
    "page": {"profile", "page_id"},
    "artifact": {"profile", "generation"},
    "audit": {"profile"},
    "operation": {"profile", "request_key"},
}


def start_origin(origin):
    global ORIGIN
    ORIGIN = loopback_origin(origin)


def nonnegative(value, label):
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError(label + " must be a bounded nonnegative integer")
    return value


