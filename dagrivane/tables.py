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


def local_path(root, relative):
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("data paths must be non-empty relative paths")
    base = Path(root).resolve()
    target = (base / relative).resolve()
    if not target.is_relative_to(base) or target == base:
        raise ValueError("data path escapes the workflow directory")
    return target


@dataclass(frozen=True)
class Table:
    contract: Contract
    records: tuple
    sources: tuple = ()

