"""Bounded direct GET requests with same-origin redirects and durable retry hints."""

import hashlib
import http.client
import math
import os
import re
import socket
import ssl
import time
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs, urljoin, urlsplit

from .http_contract import endpoint, origin, request_url

RETRYABLE = frozenset({408, 429, 500, 502, 503, 504})
REDIRECTS = frozenset({301, 302, 303, 307, 308})


class HTTPSourceError(ValueError):
    def __init__(
        self, code, *, status=None, retryable=False, retry_at=None, attempts=0
    ):
        self.code = code
        self.status = status
        self.retryable = retryable
        self.retry_at = retry_at
        self.attempts = attempts
        super().__init__(
            "HTTP source "
            + code
            + ((" (status " + str(status) + ")") if status is not None else "")
        )


class FetchCancelled(HTTPSourceError):
    def __init__(self):
        super().__init__("cancelled")


@dataclass(frozen=True)
class HTTPPage:
    raw: bytes
    metadata: dict


def retry_delay(value, wall):
    if value is None:
        return None
    value = value.strip()
    if len(value) > 128:
        return None
    if re.fullmatch(r"[0-9]{1,12}", value):
        return float(int(value))
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            return None
        return max(0.0, parsed.astimezone(timezone.utc).timestamp() - wall)
    except (ValueError, TypeError, OverflowError):
        return None


def headers(config):
    result = {
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "User-Agent": "Dagrivane/0.2",
    }
    auth = config["authentication"]
    if auth is not None:
        secret = os.environ.get(auth["environment"])
        if (
            not isinstance(secret, str)
            or not secret
            or len(secret) > 8192
            or any(ord(c) < 32 or ord(c) > 126 for c in secret)
        ):
            raise HTTPSourceError("authentication_environment_unavailable_or_invalid")
        result[auth["header"]] = auth["prefix"] + secret
    return result


def _check(cancel, deadline):
    if cancel and cancel():
        raise FetchCancelled()
    if time.monotonic() >= deadline:
        raise HTTPSourceError("deadline_exceeded", retryable=True)


