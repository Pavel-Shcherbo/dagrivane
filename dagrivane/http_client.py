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
    pass
