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

    def test_recovery_after_raw_stage_does_not_request_the_page_again(self):
        def crash(_):
            raise SystemExit(99)

        with provider(lambda *_: (200, {}, payload())) as (url, requests):
            self.register(url)
            with self.assertRaises(SystemExit):
                poll_source(self.sources, "api", after_stage=crash)
            self.assertEqual(self.sources.status("api")["pending"]["state"], "fetched")
            self.assertEqual(self.ingest.status("orders")["generation"], 0)
            reopened = HTTPSourceStore(IngestStore(self.ingest.path))
            self.assertTrue(poll_source(reopened, "api")["drained"])
            self.assertEqual(len(requests), 1)
        self.assertTrue(audit_source(self.sources, "api")["passed"])

    def test_recovery_after_atomic_apply_uses_the_saved_next_cursor(self):
        def respond(request, _):
            return (
                (200, {}, payload([row(2)], None, False))
                if request["query"]
                else (200, {}, payload([row(1)], "second", True))
            )

        def crash(_):
            raise SystemExit(98)

        with provider(respond) as (url, requests):
            self.register(url)
            with self.assertRaises(SystemExit):
                poll_source(self.sources, "api", after_apply=crash)
            self.assertEqual(self.sources.status("api")["offset"], 1)
            self.assertTrue(poll_source(self.sources, "api")["drained"])
            self.assertEqual(requests[1]["query"], {"cursor": ["second"]})
            self.assertEqual(len(requests), 2)
        self.assertEqual(self.ingest.status("orders")["generation"], 2)

    def test_live_heartbeat_preserves_ownership_during_a_slow_response(self):
        started = threading.Event()
        release = threading.Event()

        def slow(*_):
            started.set()
            release.wait(5)
            return 200, {}, payload()

        with provider(slow) as (url, _):
            self.register(url, lambda c: c["request"].update(timeout_seconds=5))
            with ThreadPoolExecutor(max_workers=1) as pool:
                running = pool.submit(poll_source, self.sources, "api", lease_seconds=1)
                self.assertTrue(started.wait(2))
                time.sleep(1.02)
                self.assertIsNone(
                    self.sources.claim("api", "competing-worker", lease_seconds=1)
                )
                release.set()
                self.assertTrue(running.result(timeout=6)["drained"])

    def test_stale_owner_cannot_apply_or_release_a_replaced_claim(self):
        self.register("http://127.0.0.1:1/feed")
        now = time.time()
        old = self.sources.claim("api", "old", lease_seconds=1, now=now)
        point = self.sources.checkpoint(old)
        raw = json_bytes(payload())
        page = HTTPPage(
            raw,
            {
                "status": 200,
                "content_type": "application/json",
                "entity_bytes": len(raw),
                "entity_sha256": hashlib.sha256(raw).hexdigest(),
            },
        )
        page_id = self.sources.stage(old, point, page)
        new = self.sources.claim("api", "new", lease_seconds=10, now=now + 2)
        with self.assertRaises(SourceLeaseLost):
            self.sources.apply(old, page_id)
        with self.assertRaises(SourceLeaseLost):
            self.sources.release(old)
        self.assertEqual(self.sources.apply(new, page_id)["offset_after"], 1)
        self.assertTrue(self.sources.apply(new, page_id)["reused"])
        self.sources.release(new)

    def test_lease_expiring_during_ingestion_rolls_back_data_and_page_checkpoint(self):
        self.register("http://127.0.0.1:1/feed")
        now = time.time()
        claim = self.sources.claim("api", "worker", lease_seconds=10, now=now)
        raw = json_bytes(payload())
        page = HTTPPage(
            raw,
            {
                "status": 200,
                "content_type": "application/json",
                "entity_bytes": len(raw),
                "entity_sha256": hashlib.sha256(raw).hexdigest(),
            },
        )
        page_id = self.sources.stage(claim, self.sources.checkpoint(claim), page)
        with patch("dagrivane.http_store.time.time", side_effect=[now, now + 20]):
            with self.assertRaisesRegex(SourceLeaseLost, "expired during processing"):
                self.sources.apply(claim, page_id)
        self.assertEqual(self.ingest.status("orders")["generation"], 0)
        self.assertEqual(self.ingest.status("orders")["offset"], 0)
        self.assertEqual(self.sources.page("api", page_id)["state"], "fetched")
        with self.ingest._tx(False) as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM publications").fetchone()[0], 0
            )
            self.assertEqual(db.execute("SELECT count(*) FROM events").fetchone()[0], 0)
        self.assertEqual(self.sources.apply(claim, page_id)["offset_after"], 1)
        self.sources.release(claim)

    def test_concurrent_poll_returns_busy_instead_of_fetching_twice(self):
        started = threading.Event()
        release = threading.Event()

        def delayed(*_):
            started.set()
            release.wait(2)
            return 200, {}, payload()

        with provider(delayed) as (url, requests):
            self.register(url)
            with ThreadPoolExecutor(max_workers=1) as pool:
                running = pool.submit(poll_source, self.sources, "api")
                self.assertTrue(started.wait(2))
                self.assertEqual(poll_source(self.sources, "api")["status"], "busy")
                release.set()
                self.assertTrue(running.result(timeout=3)["drained"])
            self.assertEqual(len(requests), 1)

    def test_retryable_error_retries_but_rate_limit_persists_cooldown(self):
        with provider(
            lambda _, number: (
                (503, {"Retry-After": "0"}, b"busy")
                if number == 1
                else (200, {}, payload())
            )
        ) as (url, requests):
            self.register(url)
            self.assertTrue(poll_source(self.sources, "api")["drained"])
            self.assertEqual(len(requests), 2)
            page = self.sources.pages("api")["items"][0]
            self.assertEqual(
                self.sources.page("api", page["id"])["metadata"]["attempts"], 2
            )
        # A second source/stream isolates a different immutable endpoint policy.
        self.ingest.register("limited", definition())
        with provider(lambda *_: (429, {"Retry-After": "3600"}, b"busy")) as (
            url,
            requests,
        ):
            self.sources.register("limited-api", "limited", configuration(url))
            deferred = poll_source(self.sources, "limited-api")
            self.assertEqual(deferred["status"], "deferred")
            self.assertGreater(deferred["source"]["not_before"], time.time() + 3500)
            self.assertEqual(
                poll_source(self.sources, "limited-api")["status"], "cooldown"
            )
            self.assertEqual(len(requests), 1)

    def test_404_and_timeout_leave_no_accepted_page_or_position(self):
        with provider(lambda *_: (404, {}, b"missing")) as (url, requests):
            self.register(url)
            result = poll_source(self.sources, "api")
            self.assertEqual(result["status"], "error")
            self.assertEqual(len(requests), 1)
            self.assertEqual(result["source"]["retained_pages"], 0)
            self.assertEqual(result["source"]["offset"], 0)
        self.ingest.register("slow", definition())

        def delayed(*_):
            time.sleep(0.15)
            return 200, {}, payload()

        with provider(delayed) as (url, _):
            config = configuration(url)
            config["request"].update(timeout_seconds=0.05, attempts=1)
            self.sources.register("slow-api", "slow", config)
            self.assertEqual(poll_source(self.sources, "slow-api")["status"], "error")
            self.assertEqual(self.ingest.status("slow")["offset"], 0)

    def test_same_origin_redirects_work_but_credentials_never_cross_origins(self):
        with provider(
            lambda request, _: (
                (302, {"Location": "/final"}, b"")
                if request["path"] == "/feed"
                else (200, {}, payload())
            )
        ) as (url, requests):
            self.register(url)
            self.assertTrue(poll_source(self.sources, "api")["drained"])
            self.assertEqual(len(requests), 2)
        self.ingest.register("auth", definition())
        with provider(lambda *_: (200, {}, payload())) as (foreign, foreign_requests):
            with provider(lambda *_: (302, {"Location": foreign}, b"")) as (
                url,
                requests,
            ):
                config = configuration(url)
                config["authentication"] = {
                    "header": "Authorization",
                    "environment": "DAGRIVANE_TEST_SECRET",
                    "prefix": "Bearer ",
                }
                self.sources.register("auth-api", "auth", config)
                secret = "only-for-loopback-test-385921"
                with patch.dict(os.environ, {"DAGRIVANE_TEST_SECRET": secret}):
                    result = poll_source(self.sources, "auth-api")
                self.assertEqual(result["status"], "error")
                self.assertEqual(foreign_requests, [])
                self.assertEqual(
                    requests[0]["headers"]["Authorization"], "Bearer " + secret
                )
                for path in self.root.glob("ingest.sqlite*"):
                    self.assertNotIn(secret.encode(), path.read_bytes())

    def test_redirect_cannot_drop_the_configured_cursor_or_query_filter(self):
        with provider(lambda *_: (302, {"Location": "/final"}, b"")) as (url, requests):
            self.register(
                url + "?account=chosen",
                lambda c: c.update(initial_cursor="resume-token"),
            )
            result = poll_source(self.sources, "api")
            self.assertEqual(result["status"], "error")
            self.assertIn("redirect_changed_query_scope", result["error"])
            self.assertEqual(len(requests), 1)
            self.assertEqual(self.ingest.status("orders")["offset"], 0)

    def test_real_cli_bounded_resume_audit_and_create_only_raw_export(self):
        def respond(request, _):
            return (
                (200, {}, payload([row(2)]))
                if request["query"]
                else (200, {}, payload([row(1)], "second", True))
            )

        with provider(respond) as (url, _):
            config = self.root / "source.json"
            config.write_bytes(json_bytes(configuration(url)))

            def command(*args, expected=0):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "dagrivane.http_source",
                        "--database",
                        str(self.ingest.path),
                        *map(str, args),
                    ],
                    cwd=Path(__file__).resolve().parents[1],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, expected, result.stderr)
                return json.loads(result.stdout) if result.stdout else None

            command("register", "api", "orders", config)
            self.assertEqual(
                command("poll", "api", "--max-pages", "1", expected=3)["status"],
                "limited",
            )
            self.assertTrue(command("poll", "api")["drained"])
            page = command("pages", "api")["items"][0]
            output = self.root / "entity.json"
            command("export-page", "api", page["id"], "--output", output)
            original = output.read_bytes()
            command("export-page", "api", page["id"], "--output", output, expected=2)
            self.assertEqual(output.read_bytes(), original)
            self.assertEqual(hashlib.sha256(original).hexdigest(), page["raw_sha"])
            self.assertTrue(command("audit", "api")["passed"])

    def test_body_limits_and_bad_content_type_cannot_advance_the_checkpoint(self):
        with provider(lambda *_: (200, {}, b"x" * 101)) as (url, _):
            self.register(
                url, lambda c: c["limits"].update(page_bytes=100, journal_bytes=1000)
            )
            result = poll_source(self.sources, "api")
            self.assertEqual(result["status"], "error")
            self.assertEqual(result["source"]["retained_pages"], 0)
        self.ingest.register("html", definition())
        with provider(
            lambda *_: (
                200,
                {"Content-Type": "text/html"},
                b"<html>wrong response</html>",
            )
        ) as (url, _):
            self.sources.register("html-api", "html", configuration(url))
            result = poll_source(self.sources, "html-api")
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["source"]["offset"], 0)
            self.assertEqual(
                self.sources.page(
                    "html-api", result["source"]["pending"]["id"], include_raw=True
                )["raw"],
                b"<html>wrong response</html>",
            )

    def test_chunked_entities_are_bounded_and_ambiguous_framing_is_rejected(self):
        raw = json_bytes(payload([row(amount="12.34")]))
        framed = format(len(raw), "x").encode() + b"\r\n" + raw + b"\r\n0\r\n\r\n"
        with provider(
            lambda *_: (
                200,
                {"Transfer-Encoding": "chunked", "Content-Length": None},
                framed,
            )
        ) as (url, _):
            self.register(url)
            self.assertTrue(poll_source(self.sources, "api")["drained"])
        self.assertEqual(self.ingest.rows("orders")["rows"][0]["amount"], "12.34")
        self.ingest.register("streamed-limit", definition())
        with provider(
            lambda *_: (
                200,
                {"Transfer-Encoding": "chunked", "Content-Length": None},
                framed,
            )
        ) as (url, _):
            config = configuration(url)
            config["limits"]["page_bytes"] = len(raw) - 1
            self.sources.register("streamed-api", "streamed-limit", config)
            self.assertEqual(
                poll_source(self.sources, "streamed-api")["status"], "error"
            )
            self.assertEqual(self.ingest.status("streamed-limit")["offset"], 0)
        self.ingest.register("ambiguous", definition())
        with provider(lambda *_: (200, {"Transfer-Encoding": "chunked"}, framed)) as (
            url,
            _,
        ):
            self.sources.register("ambiguous-api", "ambiguous", configuration(url))
            problem = poll_source(self.sources, "ambiguous-api")
            self.assertEqual(problem["status"], "error")
            self.assertIn("ambiguous_or_unsupported", problem["error"])

    def test_pagination_cycle_is_blocked_across_bounded_resume_calls(self):
        def respond(request, _):
            cursor = request["query"].get("cursor", [None])[0]
            following = {None: "A", "A": "B", "B": "A"}[cursor]
            return (
                200,
                {},
                payload([row({None: 1, "A": 2, "B": 3}[cursor])], following, True),
            )

        with provider(respond) as (url, _):
            self.register(url)
            self.assertEqual(
                poll_source(self.sources, "api", max_pages=1)["status"], "limited"
            )
            self.assertEqual(poll_source(self.sources, "api")["status"], "blocked")
        self.assertEqual(self.ingest.status("orders")["offset"], 2)
        self.assertTrue(audit_source(self.sources, "api")["passed"])

    def test_empty_json_and_no_content_check_quality_without_empty_versions(self):
        with provider(
            lambda _, number: (200, {}, payload()) if number == 1 else (204, {}, b"")
        ) as (url, _):
            self.register(url)
            poll_source(self.sources, "api")
            idle = poll_source(self.sources, "api")
            self.assertTrue(idle["drained"])
            self.assertIsNone(idle["pages"][0]["ingestion"])
        self.assertEqual(self.ingest.status("orders")["generation"], 1)
        self.assertTrue(audit_source(self.sources, "api")["passed"])
        self.ingest.register("empty", definition())
        with provider(lambda *_: (200, {}, payload([]))) as (url, _):
            self.sources.register("empty-api", "empty", configuration(url))
            self.assertTrue(poll_source(self.sources, "empty-api")["drained"])
            self.assertEqual(self.ingest.status("empty")["generation"], 0)
            self.assertTrue(audit_source(self.sources, "empty-api")["passed"])

    def test_null_native_revision_and_bad_row_keys_are_retained_not_invented(self):
        with provider(lambda *_: (200, {}, payload([{**row(), "revision": None}]))) as (
            url,
            _,
        ):
            self.register(url, lambda c: c["response"].update(event_id="/revision"))
            result = poll_source(self.sources, "api")
            self.assertEqual(result["status"], "blocked")
            self.assertIn("cannot be null", result["error"])

    def test_raw_or_response_metadata_damage_is_detected_before_recovery(self):
        self.register("http://127.0.0.1:1/feed")
        claim = self.sources.claim("api", "worker")
        point = self.sources.checkpoint(claim)
        raw = json_bytes(payload())
        page_id = self.sources.stage(
            claim,
            point,
            HTTPPage(
                raw,
                {
                    "status": 200,
                    "content_type": "application/json",
                    "entity_bytes": len(raw),
                    "entity_sha256": hashlib.sha256(raw).hexdigest(),
                },
            ),
        )
        self.sources.release(claim)
        with self.ingest._tx() as db:
            db.execute(
                "UPDATE http_pages SET metadata=? WHERE id=?",
                (json.dumps({"status": 204}), page_id),
            )
        self.assertFalse(audit_source(self.sources, "api")["passed"])
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.sources.page("api", page_id)

    def test_out_of_band_ingestion_and_invalid_registration_cannot_be_silently_rebased(
        self,
    ):
        self.register("http://127.0.0.1:1/feed")
        with self.assertRaisesRegex(ValueError, "managed by an HTTP source"):
            self.ingest.ingest("orders", batch(0, [event(1)]), request_key="outside")
        # Simulate an older writer bypassing the current public ownership guard.
        with self.ingest._tx() as db:
            self.ingest._ingest(
                db, "orders", batch(0, [event(1)]), request_key="legacy-outside"
            )
        self.assertFalse(self.sources.status("api")["stream_synchronized"])
        with self.assertRaisesRegex(ValueError, "outside"):
            poll_source(self.sources, "api")
        with self.assertRaises(ValueError):
            self.sources.register(
                "another", "orders", configuration("http://127.0.0.1:1/other")
            )
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.sources.register(
                "api", "orders", configuration("http://127.0.0.1:1/other")
            )

    def test_pages_audit_and_stored_quality_replay_are_bounded(self):
        with provider(lambda *_: (200, {}, payload([]))) as (url, _):
            self.register(url)
            for _ in range(3):
                self.assertTrue(poll_source(self.sources, "api")["drained"])
        first = self.sources.pages("api", limit=2)
        second = self.sources.pages("api", after=first["next_cursor"], limit=2)
        self.assertEqual(len(first["items"]), 2)
        self.assertEqual(len(second["items"]), 1)
        limited = audit_source(self.sources, "api", limit=2)
        self.assertFalse(limited["complete"])
        self.assertFalse(limited["passed"])
        self.assertTrue(audit_source(self.sources, "api")["passed"])

    def test_audit_uses_one_read_snapshot_when_a_writer_commits_mid_audit(self):
        def respond(_, number):
            return 200, {}, payload([row(number)])

        with provider(respond) as (url, _):
            self.register(url)
            poll_source(self.sources, "api")

            def interleave(ingestion, db, name, **kwargs):
                self.assertTrue(db.in_transaction)
                self.assertTrue(poll_source(self.sources, "api")["drained"])
                return audit_in_view(ingestion, db, name, **kwargs)

            with patch("dagrivane.http_audit.audit_in_view", side_effect=interleave):
                checked = audit_source(self.sources, "api")
        self.assertTrue(checked["passed"])
        self.assertEqual(checked["checked_pages"], 1)
        self.assertEqual(checked["ingestion_audit"]["checked_batches"], 1)
        self.assertEqual(self.ingest.status("orders")["generation"], 2)

    def test_cancellation_after_staging_preserves_the_unapplied_entity(self):
        stopped = threading.Event()
        with provider(lambda *_: (200, {}, payload())) as (url, requests):
            self.register(url)
            cancelled = poll_source(
                self.sources,
                "api",
                should_cancel=stopped.is_set,
                after_stage=lambda _: stopped.set(),
            )
            self.assertEqual(cancelled["status"], "cancelled")
            self.assertEqual(cancelled["source"]["pending"]["state"], "fetched")
            self.assertEqual(self.ingest.status("orders")["offset"], 0)
            self.assertTrue(poll_source(self.sources, "api")["drained"])
            self.assertEqual(len(requests), 1)

