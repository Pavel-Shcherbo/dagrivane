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


def process(store, db, row, now, budget):
    plan = compile_schedule(json.loads(row["definition"]))
    if plan.sha256 != row["plan_sha256"]:
        raise Conflict("retained schedule definition hash changed")
    clock = plan.clock()
    definition = plan.definition
    start = row["next_due"]
    cursor = start
    outcomes = []
    ranges = []
    deferred = None
    policy = definition["misfire"]["policy"]
    lateness = definition["misfire"]["max_lateness_seconds"]
    if lateness is not None and cursor < now - lateness:
        last = clock.previous(now - lateness - 1)
        if last is not None and last >= cursor:
            ranges.append(omitted(clock, cursor, last, "lateness_exceeded"))
            cursor = advance(clock, last)
    if cursor is not None and cursor <= now and policy in ("coalesce", "skip"):
        last = clock.previous(now)
        if last is not None and last > cursor:
            prior = clock.previous(last - 1)
            ranges.append(
                omitted(
                    clock,
                    cursor,
                    prior,
                    "coalesced" if policy == "coalesce" else "missed_older_slots",
                )
            )
            cursor = last
    while cursor is not None and cursor <= now and len(outcomes) < budget:
        blocking = active(db, row["id"]) if definition["overlap"] != "allow" else None
        if blocking and definition["overlap"] == "defer":
            deferred = blocking
            break
        outcome = {
            "due": cursor,
            "outcome": "skipped_overlap" if blocking else "submitted",
            "run_id": None,
        }
        if not blocking:
            key = f"timed:{row['id']}:{cursor}"
            queued = store.control._enqueue(
                db,
                row["workspace_id"],
                row["workflow_id"],
                row["version"],
                key,
                definition["priority"],
                now,
            )
            outcome["run_id"] = queued["id"]
            store.control.event(
                db,
                queued["id"],
                "scheduled",
                {"schedule_id": row["id"], "due": cursor, "plan_sha256": plan.sha256},
                now,
            )
        db.execute(
            "INSERT INTO cp_timed_occurrences VALUES(?,?,?,?,?)",
            (row["id"], cursor, outcome["outcome"], outcome["run_id"], now),
        )
        outcomes.append(outcome)
        cursor = advance(clock, cursor)
    payload = {
        "before_due": start,
        "after_due": cursor,
        "before_generation": row["generation"],
        "after_generation": row["generation"] + 1,
        "outcomes": outcomes,
        "omitted_ranges": ranges,
        "deferred_by": deferred,
    }
    event(db, row["id"], "dispatched", payload, now)
    db.execute(
        "UPDATE cp_timed_schedules SET next_due=?,generation=generation+1 WHERE id=?",
        (cursor, row["id"]),
    )
    return {
        "schedule_id": row["id"],
        "generation": row["generation"] + 1,
        "next_due": cursor,
        "outcomes": outcomes,
        "omitted_ranges": ranges,
        "deferred_by": deferred,
        "due_remaining": cursor is not None and cursor <= now,
    }


def dispatch(store, workspace, request_key, *, now, limit=100):
    now = instant(now, "dispatch clock")
    text(request_key, "dispatch request key", 200)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("dispatch limit must be 1 through 100 total slot outcomes")
    identity = digest({"now": now, "limit": limit})
    with store.control.transaction() as db:
        store.control.workspace(db, workspace)
        prior = db.execute(
            "SELECT * FROM cp_timed_receipts WHERE workspace_id=? AND request_key=?",
            (workspace, request_key),
        ).fetchone()
        if prior is not None:
            if prior["request_sha256"] != identity:
                raise Conflict(
                    "dispatch request key already identifies another clock or limit"
                )
            return json.loads(prior["result"])
        head = db.execute(
            "SELECT last_schedule FROM cp_timed_dispatch_heads WHERE workspace_id=?",
            (workspace,),
        ).fetchone()
        cursor = head[0] if head else ""
        rows = db.execute(
            "SELECT * FROM cp_timed_schedules WHERE workspace_id=? AND enabled=1 AND next_due<=? ORDER BY CASE WHEN id>? THEN 0 ELSE 1 END,id LIMIT 100",
            (workspace, now, cursor),
        ).fetchall()
        result = {
            "workspace_id": workspace,
            "request_key": request_key,
            "now": now,
            "limit": limit,
            "schedules": [],
            "outcome_count": 0,
            "due_remaining": False,
        }
        for row in rows[:100]:
            if result["outcome_count"] >= limit:
                break
            outcome = process(
                store, db, row, now, min(20, limit - result["outcome_count"])
            )
            result["schedules"].append(outcome)
            cursor = row["id"]
            result["outcome_count"] += len(outcome["outcomes"])
        if result["schedules"]:
            db.execute(
                "INSERT INTO cp_timed_dispatch_heads VALUES(?,?) ON CONFLICT(workspace_id) DO UPDATE SET last_schedule=excluded.last_schedule",
                (workspace, cursor),
            )
        result["due_remaining"] = bool(
            db.execute(
                "SELECT 1 FROM cp_timed_schedules WHERE workspace_id=? AND enabled=1 AND next_due<=? LIMIT 1",
                (workspace, now),
            ).fetchone()
        )
        db.execute(
            "INSERT INTO cp_timed_receipts VALUES(?,?,?,?,?)",
            (workspace, request_key, identity, canonical(result), now),
        )
        return result
