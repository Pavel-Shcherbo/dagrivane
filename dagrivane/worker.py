"""Claim a durable run and execute it while retaining fenced ownership."""

import threading
from pathlib import Path

from .control import Conflict, LostLease, text
from .engine import run


class Worker:
    def __init__(self, store, state_directory, identity, lease_seconds=30):
        text(identity, "worker identity")
        store.lease_duration(lease_seconds)
        self.store = store
        self.identity = identity
        self.lease_seconds = lease_seconds
        self.state_directory = Path(state_directory)
        self.state_directory.mkdir(parents=True, exist_ok=True)

    def run_once(self):
        claimed = self.store.claim(self.identity, self.lease_seconds)
        if claimed is None:
            return None
        run_id, token = claimed["id"], claimed["lease_token"]
        stop = threading.Event()
        cancel = threading.Event()
        lost = threading.Event()

        def heartbeat():
            while not stop.wait(min(1, self.lease_seconds / 3)):
                try:
                    state = self.store.heartbeat(
                        run_id, self.identity, token, self.lease_seconds
                    )
                    if state["cancel_requested"]:
                        cancel.set()
                except Exception:
                    lost.set()
                    cancel.set()
                    return

        def event(kind, payload):
            self.store.append_event(run_id, self.identity, token, kind, payload)

        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        result = {}
        status = "failed"
        try:
            try:
                tasks = run(
                    claimed["specification"],
                    self.state_directory / f"{run_id}.sqlite",
                    run_id,
                    claimed["directory"],
                    should_cancel=cancel.is_set,
                    on_event=event,
                )
                result = {
                    "tasks": [
                        {key: task[key] for key in ["id", "status", "attempts"]}
                        for task in tasks
                    ]
                }
                status = (
                    "cancelled"
                    if any(task["status"] == "cancelled" for task in tasks)
                    else (
                        "succeeded"
                        if all(task["status"] == "succeeded" for task in tasks)
                        else "failed"
                    )
                )
            except LostLease:
                lost.set()
            except Exception as error:
                result = {"error_type": type(error).__name__, "error": str(error)}
                status = "failed"
            if lost.is_set():
                return {
                    "id": run_id,
                    "ownership_retained": False,
                    "status": "reconciliation_required",
                }
            try:
                state = self.store.heartbeat(
                    run_id, self.identity, token, self.lease_seconds
                )
                if state["cancel_requested"]:
                    status = "cancelled"
                    result["cancellation_note"] = (
                        "Completed task effects are retained; cancellation does not undo them"
                    )
                try:
                    self.store.finish(run_id, self.identity, token, status, result)
                except Conflict:
                    # Cancellation may arrive between the final heartbeat and finish.
                    state = self.store.heartbeat(
                        run_id, self.identity, token, self.lease_seconds
                    )
                    if not state["cancel_requested"]:
                        raise
                    status = "cancelled"
                    result["cancellation_note"] = (
                        "Cancellation arrived during completion; inspect recorded task effects"
                    )
                    self.store.finish(run_id, self.identity, token, status, result)
            except LostLease:
                return {
                    "id": run_id,
                    "ownership_retained": False,
                    "status": "reconciliation_required",
                }
            return {
                "id": run_id,
                "ownership_retained": True,
                "status": status,
                "result": result,
            }
        finally:
            stop.set()
            thread.join(timeout=2)
