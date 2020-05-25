"""Immutable bounded tables, canonical identities and safe local CSV ingestion."""

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from pathlib import Path

from .contracts import Contract, encode

MAX_ROWS = 100000
MAX_CELLS = 2000000
MAX_BYTES = 16 * 1024 * 1024


