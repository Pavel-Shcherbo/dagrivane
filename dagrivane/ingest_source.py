"""Convert a pinned local CSV revision into an explicit ordered upsert batch."""

import os
import tempfile
from pathlib import Path

from .catalog_contracts import fingerprint, json_bytes
from .contracts import Contract
from .ingest_model import MAX_BATCH_BYTES, batch_definition, normalized_key, offset
from .tables import read_csv


