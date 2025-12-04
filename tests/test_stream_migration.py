"""Independent state, temporal, deduplication and catalog controls over schema forks."""

import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import fingerprint, json_bytes, parse_json
from dagrivane.ingest_store import IngestStore
from dagrivane.ingest_publish import configure_target, publish_pending
from dagrivane.ingest_audit import audit
from dagrivane.http_store import HTTPSourceStore
from dagrivane.http_client import HTTPPage
from dagrivane.stream_migration import (
    prepare_migration,
    configure_migration_policy,
    apply_migration,
    write_migration,
)
from dagrivane.stream_migration_seed import verify_base
from dagrivane.stream_migration_model import compile_projection, lossless_cast
from decimal import Decimal
from tests.test_ingest import definition, event, batch

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


