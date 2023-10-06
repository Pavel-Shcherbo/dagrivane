"""Synthetic loopback-only HTTP source for the documented ingestion control."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


def row(number, amount, day=1):
    return {
        "id": number,
        "amount": amount,
        "updated_at": f"2020-01-{day:02d}T12:00:00Z",
    }


