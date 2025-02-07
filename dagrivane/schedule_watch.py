"""Explicit foreground dispatch loop with bounded batches and restart-safe tick keys."""

import threading
import time

from .control import text
from .schedule_clock import instant


def watch_schedules(
    store,
    workspace,
    *,
    identity="local",
    every=5,
    limit=100,
    max_ticks=None,
    stop=None,
    clock=None,
    emit=None,
):
    text(identity, "watch identity", 80)
    if type(every) not in (int, float) or not 1 <= every <= 60:
        raise ValueError("watch period must be between 1 and 60 seconds")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("watch slot limit must be between 1 and 100")
    if max_ticks is not None and (
        type(max_ticks) is not int or not 1 <= max_ticks <= 10000
    ):
        raise ValueError("maximum ticks must be 1 through 10,000 or omitted")
    if (
        clock is not None
        and not callable(clock)
        or emit is not None
        and not callable(emit)
    ):
        raise ValueError("watch clock and emitter must be callable")
    stop = stop if stop is not None else threading.Event()
    clock = clock or time.time
    emit = emit or (lambda value: None)
    # Fail a missing workspace before reporting that a watcher has started.
    store.list(workspace, limit=1)
    ticks = 0
    outcomes = 0
    previous = None
    reason = "tick_limit"
    try:
        while not stop.is_set() and (max_ticks is None or ticks < max_ticks):
            now = instant(int(clock()), "watch clock")
            if previous is not None and now < previous:
                reason = "clock_regressed"
                break
            key = f"watch:{identity}:{limit}:{now}"
            result = store.dispatch(workspace, key, now=now, limit=limit)
            ticks += 1
            # Receipts may be returned after restart. This is observed outcome
            # volume, not a count of newly created runs or worker executions.
            outcomes += result["outcome_count"]
            emit({"kind": "dispatch", "tick": ticks, "receipt": result})
            previous = now
            if max_ticks is not None and ticks >= max_ticks:
                break
            if stop.wait(every):
                reason = "stopped"
                break
        if stop.is_set():
            reason = "stopped"
    except KeyboardInterrupt:
        reason = "interrupted"
    summary = {
        "kind": "watch_stopped",
        "workspace_id": workspace,
        "identity": identity,
        "ticks_observed": ticks,
        "receipt_outcomes_observed": outcomes,
        "reason": reason,
        "worker_execution_included": False,
    }
    emit(summary)
    return summary
