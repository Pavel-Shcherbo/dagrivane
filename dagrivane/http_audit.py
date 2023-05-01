"""Recompute retained HTTP identities and their atomic ingestion checkpoints."""

import hashlib

from .catalog_contracts import fingerprint, json_bytes, parse_json, table_bundle
from .contracts import Contract
from .tables import Table
from .quality import compile_gates
from .http_contract import adapt_page
from .ingest_audit import audit_in_view


