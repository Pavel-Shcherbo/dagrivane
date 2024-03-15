"""Durable reviewed ingestion controls without browser-defined URLs or code."""

import base64
import copy
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
import time

from .catalog import Catalog
from .catalog_contracts import fingerprint, json_bytes, parse_json, table_bundle, token
from .http_audit import audit_source
from .http_contract import endpoint
from .http_runner import poll_source
from .http_store import HTTPSourceStore
from .ingest_audit import audit as audit_ingestion
from .ingest_publish import configure_target, publish_next
from .ingest_store import IngestStore
from .ingestion_views import (
    PROFILES,
    require_profile,
    provider_state,
    project_view,
    revision_in_view,
)
from .resources import examples_directory

LOCK = threading.Lock()
OPERATION_RESULT_BUDGET = 32 * 1024 * 1024
SCHEMA = [
    "CREATE TABLE IF NOT EXISTS desk_meta(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL,origin TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS desk_providers(profile TEXT PRIMARY KEY,corrected INTEGER NOT NULL,retry_sent INTEGER NOT NULL,requests INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS desk_operations(seq INTEGER PRIMARY KEY AUTOINCREMENT,profile TEXT NOT NULL,request_key TEXT NOT NULL,action TEXT NOT NULL,request_sha256 TEXT NOT NULL,status TEXT NOT NULL,result BLOB,error TEXT,receipt_sha256 TEXT NOT NULL DEFAULT '',started_at REAL NOT NULL,finished_at REAL,UNIQUE(profile,request_key))",
]


def loopback_origin(value):
    normalized = endpoint(value)
    if not re.fullmatch(r"http://127\.0\.0\.1:[1-9][0-9]{0,4}/?", normalized):
        raise ValueError("ingestion workbench needs its actual loopback HTTP origin")
    return normalized.rstrip("/")


def digest(value, label="identity"):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(label + " requires lowercase SHA-256")
    return value


class IngestionDesk:
    def __init__(self, root, origin=None):
        self.root = Path(root)
        self.path = self.root / "ingestion.sqlite"
        self.origin = loopback_origin(origin) if origin is not None else None

    def _stores(self):
        store = IngestStore(self.path)
        sources = HTTPSourceStore(store)
        with store._tx(False) as db:
            marker = db.execute(
                "SELECT version,origin FROM desk_meta WHERE singleton=1"
            ).fetchone()
            if marker is None or marker["version"] != 1:
                raise ValueError("unsupported ingestion workbench schema")
        return store, sources

