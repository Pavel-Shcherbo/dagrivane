"""Catalog input contracts and independent verification of typed table bundles."""

import hashlib
import copy
import json
import re
from datetime import datetime, timezone

from .contracts import Contract
from .object_store import digest_name, MAX_OBJECT_BYTES
from .quality import compile_gates
from .tables import Table

TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}$")
REVISION = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class CatalogError(ValueError):
    pass


def token(value, label):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        raise CatalogError(f"{label} must be a bounded ASCII identifier")
    return value


def positive_version(value):
    if type(value) is not int or not 1 <= value <= 1000000:
        raise CatalogError("version must be an integer between 1 and one million")
    return value


