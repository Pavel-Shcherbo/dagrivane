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
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ingest = IngestStore(self.root / "ingest.sqlite", create=True)
        self.ingest.register("orders", definition())
        self.sources = HTTPSourceStore(self.ingest)

    def register(self, url, mutate=None):
        config = configuration(url)
        if mutate:
            mutate(config)
        return self.sources.register("api", "orders", config)

    def test_two_real_pages_exact_rows_and_idempotent_catalog_publication(self):
        def respond(request, _):
            return (
                (200, {}, payload([row(1, "10"), row(2, "20")], "page /2 +é", True))
                if not request["query"]
                else (
                    200,
                    {},
                    payload([row(1, "15", 2), row(3, "30", 2)], "tail", False),
                )
            )

        with provider(respond) as (url, requests):
            self.register(url)
            result = poll_source(self.sources, "api")
        self.assertEqual(result["status"], "drained")
        self.assertEqual(len(result["pages"]), 2)
        self.assertEqual(requests[1]["query"], {"cursor": ["page /2 +é"]})
        values = {
            item["id"]: item["amount"] for item in self.ingest.rows("orders")["rows"]
        }
        self.assertEqual(values, {1: "15", 2: "20", 3: "30"})
        catalog = Catalog(self.root / "catalog")
        configure_target(self.ingest, "orders", catalog)
        self.assertEqual(
            len(publish_pending(self.ingest, "orders", catalog)["published"]), 2
        )
        self.assertEqual(
            publish_pending(self.ingest, "orders", catalog)["published"], []
        )
        checked = audit_source(self.sources, "api")
        self.assertTrue(checked["passed"])
        self.assertEqual(checked["checked_pages"], 2)
        self.assertNotIn("token", json.dumps(self.sources.status("api")))

    def test_native_json_decimals_are_not_converted_through_binary_float(self):
        raw = b'{"data":[{"id":1,"amount":0.100000000000000001,"updated_at":"2020-01-01T12:00:00Z"}],"next":null,"more":false}'
        with provider(lambda *_: (200, {}, raw)) as (url, _):
            self.register(url)
            self.assertTrue(poll_source(self.sources, "api")["drained"])
        self.assertEqual(
            self.ingest.rows("orders")["rows"][0]["amount"], "0.100000000000000001"
        )

    def test_event_feed_upserts_deletes_and_immutable_revision_deduplication(self):
        def change(number, key, amount="10", operation="upsert"):
            value = event(
                number, key=key, amount=amount, day=number, operation=operation
            )
            value.pop("offset")
            return value

        replies = [
            {
                "events": [change(1, 1), change(2, 2, "20")],
                "next": "second",
                "more": True,
            },
            {
                "events": [
                    change(3, 1, "15"),
                    change(4, 2, operation="delete"),
                    change(5, 3, "30"),
                ],
                "next": "tail",
                "more": False,
            },
        ]
        with provider(lambda _, number: (200, {}, replies[number - 1])) as (url, _):

            def events(config):
                config["response"].update(
                    mode="events", items="/events", columns=None, event_id=None
                )

            self.register(url, events)
            self.assertTrue(poll_source(self.sources, "api")["drained"])
        state = self.ingest.rows("orders")
        self.assertEqual(state["offset"], 5)
        self.assertEqual(state["tombstones"], 1)
        self.assertEqual(
            {item["id"]: item["amount"] for item in state["rows"]}, {1: "15", 3: "30"}
        )
        self.assertTrue(audit_source(self.sources, "api")["passed"])

    def test_repeated_rows_use_content_identity_and_do_not_create_new_events(self):
        with provider(lambda *_: (200, {}, payload())) as (url, _):
            self.register(url)
            poll_source(self.sources, "api")
            again = poll_source(self.sources, "api")
        self.assertEqual(
            again["pages"][0]["ingestion"]["counts"],
            {"applied": 0, "duplicate": 1, "stale": 0},
        )
        self.assertEqual(self.ingest.status("orders")["seen_events"], 1)
        self.assertTrue(audit_source(self.sources, "api")["passed"])

    def test_quality_rejection_retains_bytes_blocks_refetch_and_review_does_not_skip(
        self,
    ):
        body = payload([row(amount="2000")])
        with provider(lambda *_: (200, {}, body)) as (url, requests):
            self.register(url)
            blocked = poll_source(self.sources, "api")
            self.assertEqual(blocked["status"], "blocked")
            self.assertEqual(self.ingest.status("orders")["offset"], 0)
            self.assertEqual(poll_source(self.sources, "api")["status"], "blocked")
            self.assertEqual(len(requests), 1)
            page_id = blocked["source"]["pending"]["id"]
            saved = self.sources.page("api", page_id, include_raw=True)
            self.assertEqual(saved["raw"], json_bytes(body))
            self.assertEqual(saved["state"], "blocked")
            with self.assertRaises(ValueError):
                self.sources.discard("api", page_id, " ")
            self.sources.discard("api", page_id, "Upstream corrected invalid amount")
            body["data"][0]["amount"] = "25"
            resumed = poll_source(self.sources, "api")
            self.assertTrue(resumed["drained"])
            self.assertEqual(resumed["source"]["offset"], 1)
        self.assertEqual(self.sources.pages("api")["items"][0]["state"], "discarded")
        self.assertTrue(audit_source(self.sources, "api")["passed"])

