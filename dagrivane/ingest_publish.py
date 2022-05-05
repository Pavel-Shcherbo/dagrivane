"""Recoverable publication from ingestion's ordered outbox to the artifact catalog."""

import tempfile
from pathlib import Path

from .catalog_contracts import fingerprint, policy_definition


