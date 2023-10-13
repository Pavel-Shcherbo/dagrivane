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


def change(identifier, number, amount=None, day=1):
    return {
        "id": identifier,
        "operation": "delete" if amount is None else "upsert",
        "key": {"id": number},
        "at": f"2020-01-{day:02d}T12:00:00Z",
        "row": row(number, amount, day) if amount is not None else None,
    }


