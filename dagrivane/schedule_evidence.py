"""Coherent bounded schedule exports and independently checked decision chains."""

import json
from pathlib import Path

from .control import Conflict, canonical, STATES
from .schedule_clock import instant, MAX_TIME
from .schedule_model import compile_schedule, digest
from .schedule_store import record

FORMAT = "dagrivane.schedule-evidence.v1"
MAX_EVENTS = 5000
MAX_OCCURRENCES = 10000
MAX_BYTES = 16 * 1024 * 1024


def next_after(clock, due):
    return None if due + 1 >= MAX_TIME else clock.next(due + 1)


def export_schedule(store, workspace, schedule):
    with store.control.reading() as db:
        schedule_record = record(store.row(db, workspace, schedule))
        events = []
        for row in db.execute(
            "SELECT * FROM cp_timed_events WHERE schedule_id=? ORDER BY sequence LIMIT ?",
            (schedule, MAX_EVENTS + 1),
        ):
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            events.append(item)
        occurrences = [
            dict(row)
            for row in db.execute(
                "SELECT * FROM cp_timed_occurrences WHERE schedule_id=? ORDER BY due LIMIT ?",
                (schedule, MAX_OCCURRENCES + 1),
            )
        ]
        if len(events) > MAX_EVENTS or len(occurrences) > MAX_OCCURRENCES:
            raise Conflict(
                "schedule export exceeds complete-audit bounds; no partial green export is available"
            )
        runs = []
        for row in db.execute(
            "SELECT r.id,r.workspace_id,r.workflow_id,r.version,r.event_key,r.priority,r.status,r.created,r.result FROM cp_runs r JOIN cp_timed_occurrences o ON o.run_id=r.id WHERE o.schedule_id=? ORDER BY o.due",
            (schedule,),
        ):
            value = dict(row)
            value["result"] = (
                json.loads(value["result"]) if value["result"] is not None else None
            )
            runs.append(value)
    document = {
        "format": FORMAT,
        "schedule": schedule_record,
        "events": events,
        "occurrences": occurrences,
        "runs": runs,
    }
    document["sha256"] = digest(document)
    verify_schedule_evidence(document)
    if len(canonical(document).encode()) > MAX_BYTES:
        raise Conflict("schedule evidence exceeds 16 MiB")
    return document


