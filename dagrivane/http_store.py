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


