"""Replay bounded raw ingestion history against checkpoints, outbox and row caches."""

import hashlib
import json

from .catalog_contracts import fingerprint, json_bytes
from .ingest_model import batch_definition, reduce_events
from .tables import Table


