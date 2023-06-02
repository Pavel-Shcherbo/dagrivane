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


def _request(config, url, request_headers, cancel, deadline):
    start = time.monotonic()
    original_origin = origin(url)
    redirects = 0
    while True:
        _check(cancel, deadline)
        parsed = urlsplit(url)
        remaining = max(
            0.001,
            min(config["request"]["timeout_seconds"], deadline - time.monotonic()),
        )
        connection = (
            http.client.HTTPSConnection(
                parsed.hostname,
                parsed.port,
                timeout=remaining,
                context=ssl.create_default_context(),
            )
            if parsed.scheme == "https"
            else http.client.HTTPConnection(
                parsed.hostname, parsed.port, timeout=remaining
            )
        )
        response = None
        try:
            connection.request(
                "GET",
                parsed.path + ("?" + parsed.query if parsed.query else ""),
                headers=request_headers,
            )
            response = connection.getresponse()
            status = response.status
            if status in REDIRECTS:
                location = response.getheader("Location")
                if (
                    redirects >= config["request"]["redirects"]
                    or not location
                    or len(location) > 4096
                ):
                    raise HTTPSourceError(
                        "redirect_limit_or_missing_location", status=status
                    )
                try:
                    target = endpoint(urljoin(url, location))
                except (ValueError, UnicodeError):
                    raise HTTPSourceError("invalid_redirect", status=status) from None
                if origin(target) != original_origin:
                    raise HTTPSourceError(
                        "cross_origin_redirect_rejected", status=status
                    )
                required = parse_qs(urlsplit(url).query, keep_blank_values=True)
                redirected = parse_qs(urlsplit(target).query, keep_blank_values=True)
                if any(
                    redirected.get(key) != values for key, values in required.items()
                ):
                    raise HTTPSourceError("redirect_changed_query_scope", status=status)
                url = target
                redirects += 1
                continue
            if status not in (200, 204):
                delay = (
                    retry_delay(response.getheader("Retry-After"), time.time())
                    if status in RETRYABLE
                    else None
                )
                raise HTTPSourceError(
                    "response_rejected",
                    status=status,
                    retryable=status in RETRYABLE,
                    retry_at=time.time() + delay if delay is not None else None,
                )
            encoding = (
                response.getheader("Content-Encoding", "identity").strip().lower()
            )
            if encoding not in ("", "identity"):
                raise HTTPSourceError("content_encoding_not_supported", status=status)
            declared = response.getheader("Content-Length")
            transfer = response.getheader("Transfer-Encoding")
            if transfer is not None and (
                transfer.strip().lower() != "chunked" or declared is not None
            ):
                raise HTTPSourceError(
                    "ambiguous_or_unsupported_transfer_encoding", status=status
                )
            if declared is not None:
                if (
                    not re.fullmatch(r"[0-9]{1,12}", declared)
                    or int(declared) > config["limits"]["page_bytes"]
                ):
                    raise HTTPSourceError(
                        "declared_body_exceeds_limit_or_invalid", status=status
                    )
            chunks = []
            size = 0
            while True:
                _check(cancel, deadline)
                chunk = response.read1(
                    min(65536, config["limits"]["page_bytes"] + 1 - size)
                )
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
                if size > config["limits"]["page_bytes"]:
                    raise HTTPSourceError("body_exceeds_limit", status=status)
            _check(cancel, deadline)
            if declared is not None and size != int(declared):
                raise HTTPSourceError(
                    "incomplete_response_body", status=status, retryable=True
                )
            if status == 204 and size:
                raise HTTPSourceError("no_content_response_has_body", status=status)
            raw = b"".join(chunks)
            return HTTPPage(
                raw,
                {
                    "status": status,
                    "content_type": response.getheader("Content-Type", "")[:256],
                    "entity_bytes": len(raw),
                    "entity_sha256": hashlib.sha256(raw).hexdigest(),
                    "request_url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "redirects": redirects,
                    "elapsed_seconds": time.monotonic() - start,
                },
            )
        finally:
            if response is not None:
                response.close()
            connection.close()


