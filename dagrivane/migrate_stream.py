"""Prepare, explicitly configure and apply a replayable stream-schema successor."""

import argparse
import json
from pathlib import Path
import sqlite3

from .catalog import Catalog
from .catalog_contracts import parse_json
from .ingest_store import IngestStore
from .stream_migration import (
    prepare_migration,
    configure_migration_policy,
    apply_migration,
    write_migration,
)
from .stream_migration_seed import verify_base


