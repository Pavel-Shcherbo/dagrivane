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


def compile_schedule(value):
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "version",
            "name",
            "trigger",
            "start_at",
            "end_at",
            "misfire",
            "overlap",
            "priority",
        }
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError(
            "schedule requires version 1, name, trigger, start_at, end_at, misfire, overlap and priority"
        )
    text(value["name"], "schedule name", 160)
    start = instant(value["start_at"], "schedule start")
    end = None if value["end_at"] is None else instant(value["end_at"], "schedule end")
    if end is not None and end <= start:
        raise ValueError("schedule end must follow its start")
    trigger = value["trigger"]
    if not isinstance(trigger, dict):
        raise ValueError("schedule trigger must be an object")
    if trigger.get("kind") == "interval":
        if (
            set(trigger) != {"kind", "seconds"}
            or type(trigger["seconds"]) is not int
            or not 60 <= trigger["seconds"] <= 31 * 86400
        ):
            raise ValueError("interval trigger requires 60 seconds through 31 days")
        trigger = dict(trigger)
    elif trigger.get("kind") == "cron":
        if (
            set(trigger)
            != {"kind", "expression", "timezone", "fold", "gap", "day_match"}
            or trigger["fold"] not in ("first", "second", "both")
            or trigger["gap"] != "skip"
            or trigger["day_match"] not in ("and", "or")
        ):
            raise ValueError(
                "cron requires expression, retained timezone, first/second/both fold, skip gap and and/or day_match"
            )
        trigger = copy.deepcopy(trigger)
        trigger["timezone"], _ = retained_zone(trigger["timezone"])
        if start % 60:
            raise ValueError("cron start must lie on a whole UTC minute")
    else:
        raise ValueError("trigger kind must be interval or cron")
    misfire = value["misfire"]
    if (
        not isinstance(misfire, dict)
        or set(misfire) != {"policy", "max_lateness_seconds"}
        or misfire["policy"] not in ("catch_up", "coalesce", "skip")
    ):
        raise ValueError(
            "misfire requires catch_up/coalesce/skip and max_lateness_seconds"
        )
    late = misfire["max_lateness_seconds"]
    if late is not None and (type(late) is not int or not 0 <= late <= 31 * 86400):
        raise ValueError("maximum lateness must be null or 0 through 31 days")
    if misfire["policy"] == "skip" and late is None:
        raise ValueError("skip policy requires an explicit lateness bound")
    if (
        value["overlap"] not in ("allow", "skip", "defer")
        or type(value["priority"]) is not int
        or not -100 <= value["priority"] <= 100
    ):
        raise ValueError(
            "overlap must be allow/skip/defer and priority must be -100 through 100"
        )
    definition = {
        "version": 1,
        "name": value["name"],
        "trigger": trigger,
        "start_at": start,
        "end_at": end,
        "misfire": dict(misfire),
        "overlap": value["overlap"],
        "priority": value["priority"],
    }
    plan = SchedulePlan(definition, digest(definition))
    plan.clock()  # Parse every cron field before state is written.
    if len(canonical(definition).encode()) > 400000:
        raise ValueError("schedule definition exceeds 400 KiB")
    return plan


def preview_schedule(definition, *, after=None, limit=20):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("preview limit must be between 1 and 100")
    plan = compile_schedule(definition)
    clock = plan.clock()
    cursor = plan.definition["start_at"] if after is None else instant(after)
    items = []
    for _ in range(limit + 1):
        due = clock.next(cursor)
        if due is None:
            break
        items.append(clock.describe(due))
        if due + 1 >= MAX_TIME:
            break
        cursor = due + 1
    return {
        "definition": plan.definition,
        "plan_sha256": plan.sha256,
        "items": items[:limit],
        "has_more": len(items) > limit,
        "next_cursor": items[limit]["due"] if len(items) > limit else None,
    }
