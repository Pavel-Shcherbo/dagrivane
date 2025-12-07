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


def proposal(source="orders", successor="orders-v2", old=None):
    old = old or definition()
    target = copy.deepcopy(old)
    target["contract"]["version"] += 1
    target["target"]["policy_version"] += 1
    target["contract"]["columns"].append(
        {"name": "currency", "type": "string", "enum": ["USD"]}
    )
    mappings = {
        column["name"]: {"operation": "copy", "source": column["name"]}
        for column in old["contract"]["columns"]
    }
    mappings["currency"] = {"operation": "constant", "value": "USD"}
    return {
        "version": 1,
        "source": source,
        "successor": successor,
        "definition": target,
        "columns": mappings,
        "drop_columns": [],
        "reason": "Expand the reviewed orders schema with explicit currency",
    }


def v2event(number, key=1, amount="10", day=4, operation="upsert", event_id=None):
    value = event(
        number, key=key, amount=amount, day=day, operation=operation, event_id=event_id
    )
    if value["row"] is not None:
        value["row"]["currency"] = "USD"
    return value


