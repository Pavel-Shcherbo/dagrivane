"""Explicit HTTP pagination, typed row mapping and stable change identities."""

import copy
import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from .catalog_contracts import fingerprint, json_bytes
from .contracts import Contract, encode
from .ingest_model import MAX_BATCH_BYTES, instant, normalized_key, offset


def pointer(value):
    if (
        not isinstance(value, str)
        or len(value) > 512
        or value
        and not value.startswith("/")
    ):
        raise ValueError(
            "JSON pointer requires bounded text beginning with / or an empty root"
        )
    parts = []
    for part in value.split("/")[1:]:
        if re.search(r"~(?![01])", part):
            raise ValueError("invalid JSON pointer escape")
        parts.append(part.replace("~1", "/").replace("~0", "~"))
    return tuple(parts)


