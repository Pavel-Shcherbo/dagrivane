"""Operate local incremental streams and recover their artifact publications."""

import argparse
import json
import sqlite3
from pathlib import Path

from .catalog import Catalog
from .catalog_contracts import parse_json
from .ingest_audit import audit
from .ingest_publish import configure_target, publish_pending
from .ingest_source import csv_batch, write_batch
from .ingest_store import IngestStore


