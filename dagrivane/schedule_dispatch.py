"""Atomic due-cursor decisions and queue publication with replayable receipts."""

import json

from .control import Conflict, canonical, text
from .schedule_clock import instant, MAX_TIME
from .schedule_model import compile_schedule, digest
from .schedule_store import event


def advance(clock, due):
    return None if due + 1 >= MAX_TIME else clock.next(due + 1)


def omitted(clock, start, through, reason):
    count = None
    if clock.trigger["kind"] == "interval":
        count = (through - start) // clock.trigger["seconds"] + 1
    return {"from_due": start, "through_due": through, "count": count, "reason": reason}


def active(db, schedule):
    row = db.execute(
        "SELECT r.id,r.status FROM cp_timed_occurrences o JOIN cp_runs r ON r.id=o.run_id WHERE o.schedule_id=? AND r.status IN ('queued','running','cancel_requested') ORDER BY o.due LIMIT 1",
        (schedule,),
    ).fetchone()
    return dict(row) if row else None


