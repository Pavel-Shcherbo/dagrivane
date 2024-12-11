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


def invalid(message):
    raise ValueError("invalid schedule evidence: " + message)


def verify_schedule_evidence(document):
    if (
        not isinstance(document, dict)
        or set(document)
        != {"format", "schedule", "events", "occurrences", "runs", "sha256"}
        or document["format"] != FORMAT
    ):
        invalid("format")
    if len(canonical(document).encode()) > MAX_BYTES or document["sha256"] != digest(
        {key: value for key, value in document.items() if key != "sha256"}
    ):
        invalid("content identity or size")
    saved = document["schedule"]
    if not isinstance(saved, dict) or set(saved) != {
        "id",
        "workspace_id",
        "workflow_id",
        "version",
        "request_key",
        "submission_hash",
        "definition",
        "plan_sha256",
        "next_due",
        "enabled",
        "generation",
        "created",
    }:
        invalid("schedule fields")
    plan = compile_schedule(saved["definition"])
    clock = plan.clock()
    if plan.sha256 != saved["plan_sha256"] or saved["submission_hash"] != digest(
        {
            "workflow": saved["workflow_id"],
            "version": saved["version"],
            "plan": plan.sha256,
        }
    ):
        invalid("pinned inputs")
    if (
        type(saved["version"]) is not int
        or saved["version"] < 1
        or type(saved["enabled"]) is not bool
        or type(saved["generation"]) is not int
        or saved["generation"] < 1
    ):
        invalid("version/generation/enabled")
    instant(saved["created"])
    events = document["events"]
    occurrences = document["occurrences"]
    runs = document["runs"]
    if (
        not isinstance(events, list)
        or not 1 <= len(events) <= MAX_EVENTS
        or not isinstance(occurrences, list)
        or len(occurrences) > MAX_OCCURRENCES
        or not isinstance(runs, list)
        or len(runs) > MAX_OCCURRENCES
    ):
        invalid("complete event/occurrence bounds")
    cursor = clock.next(plan.definition["start_at"])
    generation = 1
    enabled = True
    previous = digest([])
    sequence = 0
    expected = []
    for index, event in enumerate(events):
        if (
            not isinstance(event, dict)
            or set(event)
            != {
                "sequence",
                "schedule_id",
                "kind",
                "payload",
                "previous_sha256",
                "sha256",
                "created",
            }
            or type(event["sequence"]) is not int
            or event["sequence"] <= sequence
            or event["schedule_id"] != saved["id"]
            or event["previous_sha256"] != previous
        ):
            invalid("event ordering or parent")
        if event["sha256"] != digest(
            {
                key: value
                for key, value in event.items()
                if key not in ("sequence", "sha256")
            }
        ):
            invalid("decision hash chain")
        now = instant(event["created"])
        payload = event["payload"]
        kind = event["kind"]
        if index == 0:
            if (
                kind != "created"
                or now != saved["created"]
                or payload
                != {
                    "next_due": cursor,
                    "generation": 1,
                    "enabled": True,
                    "plan_sha256": plan.sha256,
                    "workflow_id": saved["workflow_id"],
                    "version": saved["version"],
                }
            ):
                invalid("creation decision")
        else:
            if (
                not isinstance(payload, dict)
                or payload.get("before_due") != cursor
                or payload.get("before_generation") != generation
                or payload.get("after_generation") != generation + 1
            ):
                invalid("cursor/generation continuity")
            if kind == "enabled_changed":
                if (
                    set(payload)
                    != {
                        "before_due",
                        "after_due",
                        "before_generation",
                        "after_generation",
                        "enabled",
                        "reason",
                    }
                    or payload["after_due"] != cursor
                    or type(payload["enabled"]) is not bool
                    or payload["enabled"] == enabled
                    or not isinstance(payload["reason"], str)
                    or not 1 <= len(payload["reason"].strip()) <= 500
                ):
                    invalid("operator enable decision")
                enabled = payload["enabled"]
            elif kind == "dispatched":
                if (
                    set(payload)
                    != {
                        "before_due",
                        "after_due",
                        "before_generation",
                        "after_generation",
                        "outcomes",
                        "omitted_ranges",
                        "deferred_by",
                    }
                    or not enabled
                    or cursor is None
                    or cursor > now
                ):
                    invalid("dispatch eligibility")
                working = cursor
                required_ranges = []
                late = plan.definition["misfire"]["max_lateness_seconds"]
                policy = plan.definition["misfire"]["policy"]
                if late is not None and working < now - late:
                    last_expired = clock.previous(now - late - 1)
                    if last_expired is not None and last_expired >= working:
                        count = (
                            (last_expired - working) // clock.trigger["seconds"] + 1
                            if clock.trigger["kind"] == "interval"
                            else None
                        )
                        required_ranges.append(
                            {
                                "from_due": working,
                                "through_due": last_expired,
                                "count": count,
                                "reason": "lateness_exceeded",
                            }
                        )
                        working = next_after(clock, last_expired)
                if (
                    working is not None
                    and working <= now
                    and policy in ("coalesce", "skip")
                ):
                    last_due = clock.previous(now)
                    if last_due > working:
                        prior_due = clock.previous(last_due - 1)
                        count = (
                            (prior_due - working) // clock.trigger["seconds"] + 1
                            if clock.trigger["kind"] == "interval"
                            else None
                        )
                        required_ranges.append(
                            {
                                "from_due": working,
                                "through_due": prior_due,
                                "count": count,
                                "reason": (
                                    "coalesced"
                                    if policy == "coalesce"
                                    else "missed_older_slots"
                                ),
                            }
                        )
                if payload["omitted_ranges"] != required_ranges:
                    invalid("omitted ranges do not implement the declared policy")
                working = cursor
                for omitted in payload["omitted_ranges"]:
                    if (
                        not isinstance(omitted, dict)
                        or set(omitted)
                        != {"from_due", "through_due", "count", "reason"}
                        or omitted["from_due"] != working
                    ):
                        invalid("omitted range continuity")
                    through = instant(omitted["through_due"])
                    if (
                        through < working
                        or through > now
                        or clock.next(through) != through
                    ):
                        invalid("omitted range endpoint")
                    count = (
                        (through - working) // clock.trigger["seconds"] + 1
                        if clock.trigger["kind"] == "interval"
                        else None
                    )
                    if count != omitted["count"]:
                        invalid("omitted count")
                    reason = omitted["reason"]
                    late = plan.definition["misfire"]["max_lateness_seconds"]
                    policy = plan.definition["misfire"]["policy"]
                    if reason == "lateness_exceeded":
                        if late is None or through >= now - late:
                            invalid("lateness decision")
                    elif reason in ("coalesced", "missed_older_slots"):
                        required = "coalesce" if reason == "coalesced" else "skip"
                        last = clock.previous(now)
                        if (
                            policy != required
                            or last is None
                            or through != clock.previous(last - 1)
                        ):
                            invalid("missed-slot policy")
                    else:
                        invalid("unknown omission reason")
                    working = next_after(clock, through)
                for outcome in payload["outcomes"]:
                    if (
                        not isinstance(outcome, dict)
                        or set(outcome) != {"due", "outcome", "run_id"}
                        or outcome["due"] != working
                        or working is None
                        or working > now
                    ):
                        invalid("due-slot outcome continuity")
                    late = plan.definition["misfire"]["max_lateness_seconds"]
                    if late is not None and working < now - late:
                        invalid("outcome exceeds lateness")
                    if outcome["outcome"] == "submitted":
                        if (
                            not isinstance(outcome["run_id"], str)
                            or not outcome["run_id"]
                        ):
                            invalid("submitted run identity")
                    elif outcome["outcome"] == "skipped_overlap":
                        if (
                            plan.definition["overlap"] != "skip"
                            or outcome["run_id"] is not None
                        ):
                            invalid("overlap skip decision")
                    else:
                        invalid("outcome kind")
                    expected.append(
                        {"schedule_id": saved["id"], **outcome, "created": now}
                    )
                    working = next_after(clock, working)
                blocking = payload["deferred_by"]
                if blocking is not None:
                    if (
                        plan.definition["overlap"] != "defer"
                        or not isinstance(blocking, dict)
                        or set(blocking) != {"id", "status"}
                        or blocking["status"]
                        not in ("queued", "running", "cancel_requested")
                        or not isinstance(blocking["id"], str)
                        or working is None
                        or working > now
                    ):
                        invalid("deferral declaration")
                if payload["after_due"] != working:
                    invalid("advanced cursor")
                cursor = working
            else:
                invalid("decision kind")
            generation += 1
        previous = event["sha256"]
        sequence = event["sequence"]
    if (
        cursor != saved["next_due"]
        or generation != saved["generation"]
        or enabled != saved["enabled"]
    ):
        invalid("current schedule differs from history")
    if expected != occurrences:
        invalid("occurrences differ from decisions")
    by_id = {run["id"]: run for run in runs}
    submitted = [item for item in expected if item["outcome"] == "submitted"]
    if (
        len(by_id) != len(runs)
        or {item["run_id"] for item in submitted} != set(by_id)
        or len(submitted) != len(by_id)
    ):
        invalid("run references")
    for item in submitted:
        run = by_id[item["run_id"]]
        if (
            set(run)
            != {
                "id",
                "workspace_id",
                "workflow_id",
                "version",
                "event_key",
                "priority",
                "status",
                "created",
                "result",
            }
            or run["status"] not in STATES
        ):
            invalid("run fields or status")
        if (
            run["workspace_id"] != saved["workspace_id"]
            or run["workflow_id"] != saved["workflow_id"]
            or run["version"] != saved["version"]
            or run["priority"] != plan.definition["priority"]
            or run["event_key"] != f"timed:{saved['id']}:{item['due']}"
            or run["created"] != item["created"]
        ):
            invalid("queued run pinned inputs")
    return {
        "passed": True,
        "complete": True,
        "schedule_id": saved["id"],
        "plan_sha256": plan.sha256,
        "events_checked": len(events),
        "occurrences_checked": len(occurrences),
        "run_references_checked": len(runs),
        "execution_verified": False,
        "source_authenticated": False,
        "scope": "Retained clock, decision-chain and queue-reference consistency; not independent proof of worker effects or authenticated production history",
    }


def save_evidence(document, path):
    verify_schedule_evidence(document)
    path = Path(path)
    with path.open("xb") as stream:
        stream.write(canonical(document).encode() + b"\n")
    path.chmod(0o600)
    return {"path": str(path), "sha256": document["sha256"]}
