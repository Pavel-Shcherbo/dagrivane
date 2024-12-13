"""Strict pinned schedule definitions and side-effect-free recurrence previews."""

import copy
from dataclasses import dataclass
import hashlib

from .control import canonical, text
from .schedule_clock import Recurrence, instant, retained_zone, MAX_TIME


@dataclass(frozen=True)
class SchedulePlan:
    definition: dict
    sha256: str

    def clock(self):
        return Recurrence(
            self.definition["trigger"],
            self.definition["start_at"],
            self.definition["end_at"],
        )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


