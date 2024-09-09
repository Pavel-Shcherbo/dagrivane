"""Retained timezone clocks and bounded minute-resolution recurrence expansion."""

import base64
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
import hashlib
from importlib import resources
import io
from pathlib import Path
import re
import struct
from zoneinfo import TZPATH, ZoneInfo

UTC = timezone.utc
MIN_TIME = 0
MAX_TIME = 4133980800  # 2101-01-01T00:00:00Z, exclusive.


def instant(value, field="time"):
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,10}", value):
        value = int(value)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.microsecond:
                raise ValueError("an explicit timezone and whole seconds are required")
            value = parsed.timestamp()
        except (ValueError, OverflowError, OSError) as error:
            raise ValueError(f"invalid {field}: {error}") from error
    if (
        type(value) not in (int, float)
        or not MIN_TIME <= value < MAX_TIME
        or int(value) != value
    ):
        raise ValueError(f"{field} must be whole Unix seconds from 1970 through 2100")
    return int(value)


def iso(value):
    return datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z")


