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


