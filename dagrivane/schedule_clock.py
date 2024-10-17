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
    def __init__(self, trigger, start, end):
        self.trigger = trigger
        self.start = start
        self.end = end
        self.zone = None
        self.fields = None
        self._day_cache = {}
        if trigger["kind"] == "cron":
            _, self.zone = retained_zone(trigger["timezone"])
            expression = trigger["expression"]
            if (
                not isinstance(expression, str)
                or len(expression) > 512
                or len(expression.split()) != 5
            ):
                raise ValueError("cron expression must have five numeric fields")
            self.fields = tuple(
                cron_field(part, *bounds)
                for part, bounds in zip(
                    expression.split(), ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))
                )
            )

    def _date_matches(self, day):
        _, _, dom, months, dow = self.fields
        if day.month not in months:
            return False
        left, right = day.day in dom, (day.weekday() + 1) % 7 in dow
        if self.trigger["day_match"] == "and" or len(dom) == 31 or len(dow) == 7:
            return left and right
        return left or right

    def _local_instants(self, wall):
        found = set()
        for fold in (0, 1):
            aware = wall.replace(tzinfo=self.zone, fold=fold)
            stamp = int(aware.timestamp())
            back = datetime.fromtimestamp(stamp, self.zone)
            if back.replace(tzinfo=None) == wall and back.fold == fold:
                found.add(stamp)
        ordered = sorted(found)
        policy = self.trigger["fold"]
        return (
            ordered
            if policy == "both"
            else ordered[:1] if policy == "first" else ordered[-1:]
        )

    def _day_instants(self, day):
        if day not in self._day_cache:
            values = set()
            for hour in self.fields[1]:
                for minute in self.fields[0]:
                    wall = datetime(day.year, day.month, day.day, hour, minute)
                    values.update(self._local_instants(wall))
            if len(self._day_cache) >= 8:
                self._day_cache.pop(next(iter(self._day_cache)))
            self._day_cache[day] = tuple(sorted(values))
        return self._day_cache[day]

    def occurrence(self, boundary, *, reverse=False):
        boundary = instant(boundary, "recurrence boundary")
        lower = self.start
        upper = self.end if self.end is not None else MAX_TIME
        if reverse:
            boundary = min(boundary, upper - 1)
            if boundary < lower:
                return None
        else:
            boundary = max(boundary, lower)
            if boundary >= upper:
                return None
        if self.trigger["kind"] == "interval":
            interval = self.trigger["seconds"]
            offset = (
                (boundary - lower) // interval
                if reverse
                else (boundary - lower + interval - 1) // interval
            )
            result = lower + offset * interval
            return result if lower <= result < upper else None
        origin = datetime.fromtimestamp(boundary, self.zone).date()
        direction = -1 if reverse else 1
        day = origin - timedelta(days=2 * direction)
        best = None
        checked = 0
        for _ in range(3664):
            # A timezone offset has absolute magnitude <24 hours. This bound makes
            # ordering safe even around date-line changes and unusual transitions.
            extreme = datetime.combine(day, datetime.min.time(), UTC).timestamp()
            if best is not None and (
                extreme - 86400 > best if not reverse else extreme + 172800 < best
            ):
                break
            if (extreme - 86400 >= upper and not reverse) or (
                extreme + 172800 < lower and reverse
            ):
                break
            if self._date_matches(day):
                checked += len(self.fields[1]) * len(self.fields[0])
                if checked > 200000:
                    raise ValueError(
                        "cron search exceeds 200,000 local-minute candidates"
                    )
                values = self._day_instants(day)
                position = (
                    bisect_right(values, boundary) - 1
                    if reverse
                    else bisect_left(values, boundary)
                )
                if 0 <= position < len(values):
                    candidate = values[position]
                    if lower <= candidate < upper and (
                        best is None
                        or (candidate > best if reverse else candidate < best)
                    ):
                        best = candidate
            day += timedelta(days=direction)
        if best is None and lower <= boundary < upper:
            edge = datetime.combine(day, datetime.min.time(), UTC).timestamp()
            if lower < edge < upper:
                raise ValueError(
                    "cron has no occurrence within the ten-year search horizon"
                )
        return best

    def next(self, boundary):
        return self.occurrence(boundary)

    def previous(self, boundary):
        return self.occurrence(boundary, reverse=True)

    def describe(self, due):
        value = {"due": due, "utc": iso(due)}
        if self.zone is not None:
            local = datetime.fromtimestamp(due, self.zone)
            value.update(
                local=local.isoformat(),
                fold=local.fold,
                timezone=self.trigger["timezone"]["name"],
                timezone_sha256=self.trigger["timezone"]["sha256"],
            )
        return value
