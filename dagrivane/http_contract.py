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


def extract(value, path):
    for part in pointer(path):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif (
            isinstance(value, list)
            and re.fullmatch(r"0|[1-9][0-9]{0,8}", part)
            and int(part) < len(value)
        ):
            value = value[int(part)]
        else:
            raise ValueError("required HTTP JSON pointer is missing")
    return value


def opaque(value):
    if value is None:
        return None
    if type(value) is int and 0 <= value < 2**63:
        return value
    if (
        isinstance(value, str)
        and 1 <= len(value.encode("utf-8")) <= 2048
        and not any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        return value
    raise ValueError(
        "pagination cursor must be null, bounded text or a nonnegative 64-bit integer"
    )


def endpoint(value):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 4096
        or any(ord(c) <= 32 or ord(c) == 127 for c in value)
        or "\\" in value
    ):
        raise ValueError("HTTP endpoint requires a bounded absolute URL")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("endpoint must be HTTP(S), without credentials or a fragment")
    host = parsed.hostname.encode("idna").decode().lower()
    port = parsed.port
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("invalid HTTP endpoint port")
    authority = ("[" + host + "]" if ":" in host else host) + (
        ":" + str(port) if port is not None else ""
    )
    return urlunsplit(
        (
            parsed.scheme,
            authority,
            quote(parsed.path or "/", safe="/%:@!$&'()*+,;=-._~"),
            urlencode(parse_qsl(parsed.query, keep_blank_values=True)),
            "",
        )
    )


def origin(url):
    parsed = urlsplit(endpoint(url))
    return (
        parsed.scheme,
        parsed.hostname,
        parsed.port or (443 if parsed.scheme == "https" else 80),
    )


