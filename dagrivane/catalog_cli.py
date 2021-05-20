"""Operator CLI for local data artifacts, pinned releases and export verification."""

import argparse
import json
import sqlite3
from pathlib import Path

from .catalog import Catalog
from .catalog_contracts import parse_json
from .object_store import bounded_file
from .release_bundle import export_release, verify_export


