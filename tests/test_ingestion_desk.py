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
    pass
