"""Fenced HTTP page journals sharing ingestion's atomic checkpoint transaction."""

import hashlib
import math
import time
import uuid

from .catalog_contracts import fingerprint, json_bytes, parse_json, token
from .contracts import Contract
from .http_client import HTTPPage
from .http_contract import adapt_page, definition
from .quality import compile_gates
from .tables import Table

SCHEMA = [
    "CREATE TABLE IF NOT EXISTS http_meta(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL)",
    """CREATE TABLE IF NOT EXISTS http_sources(name TEXT PRIMARY KEY,stream TEXT UNIQUE NOT NULL REFERENCES streams(name),
       configuration TEXT NOT NULL,configuration_sha TEXT NOT NULL,stream_sha TEXT NOT NULL,
       initial_offset INTEGER NOT NULL,initial_generation INTEGER NOT NULL,initial_cursor TEXT NOT NULL,
       offset INTEGER NOT NULL,generation INTEGER NOT NULL,cursor TEXT NOT NULL,round INTEGER NOT NULL,
       chain TEXT NOT NULL,pending TEXT,claim TEXT,owner TEXT,lease_until REAL,not_before REAL,
       last_error TEXT,pages INTEGER NOT NULL,journal_bytes INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS http_pages(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,
       source TEXT NOT NULL REFERENCES http_sources(name),offset INTEGER NOT NULL,generation INTEGER NOT NULL,
       round INTEGER NOT NULL,cursor TEXT NOT NULL,previous_chain TEXT NOT NULL,raw BLOB NOT NULL,
       raw_sha TEXT NOT NULL,metadata TEXT NOT NULL,state TEXT NOT NULL,receipt TEXT,error TEXT,
       reason TEXT,received_at REAL NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS http_page_history ON http_pages(source,seq)",
]


class SourceLeaseLost(ValueError):
    pass


def moment(value=None):
    value = time.time() if value is None else value
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("HTTP lease time must be finite and nonnegative")
    return value


class HTTPSourceStore:
    def __init__(self, ingestion):
        self.ingestion = ingestion

    def _source(self, db, name):
        token(name, "HTTP source")
        if (
            db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='http_meta'"
            ).fetchone()
            is None
        ):
            raise ValueError(
                "HTTP sources are not initialized; register one explicitly"
            )
        version = db.execute(
            "SELECT version FROM http_meta WHERE singleton=1"
        ).fetchone()
        if version is None or version[0] != 1:
            raise ValueError("unsupported HTTP journal schema")
        row = db.execute("SELECT * FROM http_sources WHERE name=?", (name,)).fetchone()
        if row is None:
            raise ValueError("unknown HTTP source")
        stream, stream_def = self.ingestion._stream(db, row["stream"])
        config = definition(parse_json(row["configuration"].encode()), stream_def)
        if (
            fingerprint(config) != row["configuration_sha"]
            or stream["definition_sha256"] != row["stream_sha"]
        ):
            raise ValueError("HTTP source or stream definition identity mismatch")
        return dict(row), config, stream, stream_def

