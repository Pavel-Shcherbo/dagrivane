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


