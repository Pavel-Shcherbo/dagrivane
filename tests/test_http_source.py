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


@contextmanager
def provider(handler):
    requests = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlsplit(self.path)
            with lock:
                requests.append(
                    {
                        "path": parsed.path,
                        "query": parse_qs(parsed.query, keep_blank_values=True),
                        "headers": dict(self.headers),
                    }
                )
                number = len(requests)
            status, headers, body = handler(requests[-1], number)
            raw = json_bytes(body) if isinstance(body, (dict, list)) else body
            self.send_response(status)
            for name, value in {
                "Content-Type": "application/json",
                "Content-Length": str(len(raw)),
                **headers,
            }.items():
                if value is not None:
                    self.send_header(name, value)
            self.end_headers()
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.02), daemon=True
    )
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port) + "/feed", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class HTTPSourceTests(unittest.TestCase):
    pass
