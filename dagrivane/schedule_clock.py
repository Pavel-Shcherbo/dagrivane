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


def retained_zone(value):
    if isinstance(value, str):
        if not re.fullmatch(r"[A-Za-z0-9_+.-]+(?:/[A-Za-z0-9_+.-]+)*", value) or any(
            part in (".", "..") for part in value.split("/")
        ):
            raise ValueError("timezone must be an IANA key")
        raw = None
        for directory in TZPATH:
            candidate = Path(directory).joinpath(*value.split("/"))
            if candidate.is_file():
                with candidate.open("rb") as stream:
                    raw = stream.read(262145)
                break
        if raw is None:
            try:
                node = resources.files("tzdata.zoneinfo").joinpath(*value.split("/"))
                with node.open("rb") as stream:
                    raw = stream.read(262145)
            except (ModuleNotFoundError, FileNotFoundError, IsADirectoryError) as error:
                raise ValueError(
                    "timezone data unavailable; supply retained TZif data or install tzdata"
                ) from error
        value = {
            "name": value,
            "tzif": base64.b64encode(raw).decode(),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    if (
        not isinstance(value, dict)
        or set(value) != {"name", "tzif", "sha256"}
        or not isinstance(value["name"], str)
        or not 1 <= len(value["name"]) <= 200
        or not isinstance(value["tzif"], str)
        or len(value["tzif"]) > 350000
    ):
        raise ValueError("invalid retained timezone declaration")
    try:
        raw = base64.b64decode(value["tzif"], validate=True)
        if (
            not raw.startswith(b"TZif")
            or len(raw) > 262144
            or hashlib.sha256(raw).hexdigest() != value["sha256"]
        ):
            raise ValueError("retained timezone bytes or hash mismatch")
        zone = ZoneInfo.from_file(io.BytesIO(raw), key=value["name"])
    except (ValueError, TypeError, OverflowError, EOFError, struct.error) as error:
        raise ValueError("invalid retained timezone data") from error
    return dict(value), zone


def cron_field(value, low, high):
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        raise ValueError("invalid cron field")
    selected = set()
    for component in value.split(","):
        if not re.fullmatch(r"(?:\*|\d{1,2}(?:-\d{1,2})?)(?:/\d{1,2})?", component):
            raise ValueError(
                "cron supports numeric values, lists, ranges and explicit range/* steps"
            )
        parts = component.split("/")
        step = int(parts[1]) if len(parts) == 2 else 1
        if not 1 <= step <= high - low + 1:
            raise ValueError("cron step is outside the field range")
        if parts[0] == "*":
            start, stop = low, high
        elif "-" in parts[0]:
            start, stop = map(int, parts[0].split("-"))
        elif len(parts) == 1:
            start = stop = int(parts[0])
        else:
            raise ValueError("cron steps require a wildcard or range")
        if not low <= start <= stop <= high:
            raise ValueError("cron value/range outside bounds")
        selected.update(range(start, stop + 1, step))
    return tuple(sorted(selected))


class Recurrence:
    pass
