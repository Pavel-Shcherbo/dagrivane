"""Register, poll and review explicit HTTP feeds into typed incremental streams."""

import argparse
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from .catalog_contracts import parse_json
from .http_audit import audit_source
from .http_runner import poll_source
from .http_store import HTTPSourceStore
from .ingest_store import IngestStore


def write_raw(raw, destination):
    destination = Path(destination)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=".http-entity-", dir=destination.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


