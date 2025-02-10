"""Register, preview, dispatch and inspect retained local workflow schedules."""

import argparse
import json
from pathlib import Path
import sqlite3
import time

from .control import ControlError, ControlStore
from .schedule_clock import instant
from .schedule_evidence import export_schedule, save_evidence, verify_schedule_evidence
from .schedule_model import preview_schedule
from .schedule_store import ScheduleStore


def load(path, maximum=16 * 1024 * 1024):
    with Path(path).open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("schedule input exceeds byte limit")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    return json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(
            ValueError("nonfinite JSON number")
        ),
    )


