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


