"""Real loopback feeds, crash boundaries, source fencing and exact decimal mapping."""

import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import fingerprint, json_bytes
from dagrivane.http_audit import audit_source
from dagrivane.http_client import HTTPPage, fetch_page
from dagrivane.http_contract import (
    adapt_page,
    definition as compile_http,
    endpoint,
    extract,
    origin,
)
from dagrivane.http_runner import poll_source
from dagrivane.http_store import HTTPSourceStore, SourceLeaseLost
from dagrivane.ingest_audit import audit_in_view
from dagrivane.ingest_publish import configure_target, publish_pending
from dagrivane.ingest_store import IngestStore
from test_ingest import definition, batch, event


def row(number=1, amount="10", day=1):
    return {
        "id": number,
        "amount": amount,
        "updated_at": f"2020-01-{day:02d}T12:00:00Z",
    }


def payload(rows=None, next_cursor=None, more=False):
    return {
        "data": rows if rows is not None else [row()],
        "next": next_cursor,
        "more": more,
    }


def configuration(url):
    return {
        "version": 1,
        "url": url,
        "allow_http": True,
        "cursor_parameter": "cursor",
        "initial_cursor": None,
        "response": {
            "items": "/data",
            "next_cursor": "/next",
            "more": "/more",
            "mode": "rows",
            "columns": {"id": "/id", "amount": "/amount", "updated_at": "/updated_at"},
            "event_id": None,
        },
        "request": {
            "timeout_seconds": 1,
            "attempts": 2,
            "retry_base_seconds": 0,
            "max_inline_wait_seconds": 0,
            "redirects": 2,
        },
        "authentication": None,
        "limits": {"page_bytes": 1048576, "journal_bytes": 8388608},
    }


