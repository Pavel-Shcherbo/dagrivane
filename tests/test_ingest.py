"""Incremental recovery, temporal ordering and independent row-state controls."""

import copy
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

from dagrivane.catalog import Catalog
from dagrivane.catalog_contracts import json_bytes
from dagrivane.ingest_model import batch_definition, stream_definition
from dagrivane.ingest_store import IngestStore
from dagrivane.ingest_audit import audit
from dagrivane.ingest_publish import configure_target, publish_next, publish_pending
from dagrivane.ingest_source import csv_batch, write_batch


