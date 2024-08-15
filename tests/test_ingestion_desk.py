"""Real self-hosted source controls, durable review receipts and race fences."""

import base64
from contextlib import closing
import hashlib
import http.client
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from dagrivane.catalog_contracts import fingerprint, json_bytes
from dagrivane.httpapp import serve
from dagrivane.http_runner import poll_source
from dagrivane.http_store import HTTPSourceStore
from dagrivane.ingest_store import IngestStore
from dagrivane.ingest_publish import publish_next
from dagrivane.ingestion_desk import IngestionDesk, LOCK
from dagrivane.ingestion_web import evaluate_ingestion, start_origin, source_route
from dagrivane.web import evaluate, example


class IngestionDeskTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ready = threading.Event()
        holder = {}

        def factory(*args, **kwargs):
            server = ThreadingHTTPServer(*args, **kwargs)
            holder["server"] = server
            return server

        def started(origin):
            start_origin(origin)
            holder["origin"] = origin
            ready.set()

        def worker():
            with (
                patch("dagrivane.httpapp.ThreadingHTTPServer", side_effect=factory),
                patch.object(sys, "argv", ["workbench", "--port", "0"]),
            ):
                serve(evaluate, example, 0, on_start=started, source_route=source_route)

        cls.thread = threading.Thread(target=worker, daemon=True)
        cls.thread.start()
        if not ready.wait(5):
            raise RuntimeError("test workbench failed to start")
        cls.server = holder["server"]
        cls.origin = holder["origin"]
        cls.port = cls.server.server_port

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join(5)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.guard = patch(
            "dagrivane.ingestion_web.state_directory", return_value=self.root
        )
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.instance = IngestionDesk(self.root / "ingestion-workbench", self.origin)
        self.sequence = 0

    def api(self, action, **params):
        return evaluate_ingestion({"mode": "ingestion", "action": action, **params})

    def initialize(self):
        return self.api("initialize")

    def view(self, profile="healthy", **params):
        return self.api("inspect", profile=profile, **params)

    def action(self, action, profile="healthy", **params):
        self.sequence += 1
        return self.api(
            action,
            profile=profile,
            request_key="test-" + str(self.sequence),
            expected_revision=self.view(profile)["revision"],
            **params,
        )

    def stores(self):
        store = IngestStore(self.instance.path)
        return store, HTTPSourceStore(store)

    def http(self, method, path, params=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            connection.request(
                method,
                path,
                json.dumps(params) if params is not None else None,
                {
                    "Content-Type": "application/json",
                    "X-Local-Request": "1",
                    **(headers or {}),
                },
            )
            response = connection.getresponse()
            body = response.read()
            return response.status, json.loads(body) if body else None
        finally:
            connection.close()

    def test_empty_and_invalid_controls_do_not_initialize_state(self):
        self.assertFalse(self.api("overview")["initialized"])
        invalid = [
            {
                "action": "poll",
                "profile": "outside",
                "request_key": "x",
                "expected_revision": "a" * 64,
            },
            {
                "action": "poll",
                "profile": "healthy",
                "url": "http://external",
                "request_key": "x",
                "expected_revision": "a" * 64,
            },
            {"action": "inspect", "profile": "healthy", "after_page": True},
            {"action": "page", "profile": "healthy", "page_id": "../file"},
            {
                "action": "repair",
                "profile": "quality",
                "page_id": "a" * 64,
                "request_key": "x",
                "expected_revision": "a" * 64,
                "reason": "",
            },
            {"action": "artifact", "profile": "healthy", "generation": 0},
        ]
        for fields in invalid:
            with self.assertRaises(ValueError):
                evaluate_ingestion({"mode": "ingestion", **fields})
        self.assertFalse(self.instance.path.exists())

    def test_initialize_is_idempotent_and_has_three_empty_coherent_sources(self):
        first = self.initialize()
        self.assertEqual(first, self.initialize())
        self.assertEqual(len(first["items"]), 3)
        view = self.view()
        self.assertEqual(view["data"]["rows"], [])
        self.assertEqual(view["data"]["amount_total"], "0")
        self.assertEqual(view["source"]["offset"], view["data"]["offset"])
        self.assertEqual(view["source"]["generation"], view["stream"]["generation"])

    def test_two_real_pages_one_at_a_time_then_idle_without_new_generations(self):
        self.initialize()
        first = self.action("poll")
        self.assertEqual(first["outcome"]["status"], "limited")
        self.assertEqual(self.view()["data"]["amount_total"], "30")
        second = self.action("poll")
        self.assertEqual(second["outcome"]["status"], "drained")
        view = self.view()
        self.assertEqual(
            view["data"]["rows"],
            [
                {"id": 1, "amount": "15", "updated_at": "2020-01-02T12:00:00Z"},
                {"id": 3, "amount": "30", "updated_at": "2020-01-02T12:00:00Z"},
            ],
        )
        self.assertEqual(view["data"]["amount_total"], "45")
        self.assertEqual(view["data"]["tombstones"], 1)
        self.action("poll")
        self.assertEqual(self.view()["source"]["generation"], 2)
        self.assertEqual(self.view()["provider"]["requests"], 3)

    def test_poll_retry_receipt_does_not_refetch_or_advance_another_page(self):
        self.initialize()
        revision = self.view()["revision"]
        fields = {
            "profile": "healthy",
            "request_key": "retry",
            "expected_revision": revision,
        }
        first = self.api("poll", **fields)
        again = self.api("poll", **fields)
        self.assertEqual(first, again)
        self.assertEqual(self.view()["provider"]["requests"], 1)
        self.assertEqual(
            self.api("operation", profile="healthy", request_key="retry"), first
        )
        with self.assertRaisesRegex(ValueError, "conflicts"):
            self.api("publish", **fields)
        self.assertEqual(self.view()["source"]["generation"], 1)

    def test_quality_gate_pins_raw_entity_and_stops_further_fetches(self):
        self.initialize()
        result = self.action("poll", "quality")
        self.assertEqual(result["outcome"]["status"], "blocked")
        before = self.view("quality")
        page = before["source"]["pending"]["id"]
        raw = self.api("page", profile="quality", page_id=page)
        entity = base64.b64decode(raw["raw_base64"])
        self.assertEqual(hashlib.sha256(entity).hexdigest(), raw["raw_sha256"])
        self.assertEqual(json.loads(entity)["events"][0]["row"]["amount"], "2000")
        self.action("poll", "quality")
        after = self.view("quality")
        self.assertEqual(after["provider"]["requests"], 1)
        self.assertEqual(after["data"]["total_rows"], 0)
        self.assertEqual(after["source"]["offset"], 0)

    def test_review_correction_preserves_original_then_refetches_same_position(self):
        self.initialize()
        self.action("poll", "quality")
        blocked = self.view("quality")
        page = blocked["source"]["pending"]["id"]
        raw = self.api("page", profile="quality", page_id=page)["raw_base64"]
        self.action(
            "repair",
            "quality",
            page_id=page,
            reason="Correct synthetic provider amount",
        )
        corrected = self.view("quality")
        self.assertTrue(corrected["provider"]["corrected"])
        self.assertEqual(corrected["source"]["pending"]["id"], page)
        self.assertEqual(corrected["source"]["offset"], 0)
        self.action(
            "discard",
            "quality",
            page_id=page,
            reason="Reviewed invalid entity; keep original bytes",
        )
        self.assertEqual(self.view("quality")["source"]["offset"], 0)
        self.action("poll", "quality")
        self.action("poll", "quality")
        self.assertEqual(self.view("quality")["data"]["amount_total"], "45")
        retained = self.api("page", profile="quality", page_id=page)
        self.assertEqual(retained["raw_base64"], raw)
        self.assertEqual(retained["state"], "discarded")
        self.assertTrue(self.api("audit", profile="quality")["passed"])

    def test_transient_retry_happens_over_real_http_and_is_visible_in_provider_count(
        self,
    ):
        self.initialize()
        result = self.action("poll", "retry")
        self.assertEqual(result["outcome"]["status"], "limited")
        provider = self.view("retry")["provider"]
        self.assertTrue(provider["retry_sent"])
        self.assertEqual(provider["requests"], 2)

