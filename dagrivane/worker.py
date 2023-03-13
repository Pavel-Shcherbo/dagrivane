"""Claim a durable run and execute it while retaining fenced ownership."""

import threading
import copy
from pathlib import Path

from .control import Conflict, LostLease, text
from .engine import run
from .run_workspace import create_run_workspace


class Worker:
    def __init__(self, store, state_directory, identity, lease_seconds=30):
        text(identity, "worker identity")
        store.lease_duration(lease_seconds)
        self.store = store
        self.identity = identity
        self.lease_seconds = lease_seconds
        self.state_directory = Path(state_directory)
        self.state_directory.mkdir(parents=True, exist_ok=True)

    def run_once(self, *, workspace_id=None, run_id=None, prepare_claim=None):
        if prepare_claim is not None and not callable(prepare_claim):
            raise ValueError("claim preparation must be callable")
        claimed = self.store.claim(
            self.identity, self.lease_seconds, workspace_id=workspace_id, run_id=run_id
        )
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
                directory = (
                    prepare_claim(copy.deepcopy(claimed))
                    if prepare_claim is not None
                    else claimed["directory"]
                )
                if (
                    not isinstance(directory, (str, Path))
                    or not Path(directory).is_dir()
                ):
                    raise ValueError(
                        "claim preparation must return an existing source directory"
                    )
                workspace = None
                policy = claimed["specification"].get("execution_policy", "in_place")
                if policy == "isolated_inputs":
                    parent = self.state_directory / "workspaces"
                    parent.mkdir(mode=0o700, exist_ok=True)
                    workspace = create_run_workspace(
                        claimed["specification"], directory, parent / run_id
                    )
                    directory = workspace["directory"]
                    event(
                        "workspace_prepared",
                        {
                            "policy": policy,
                            "inputs": workspace["inputs"],
                            "bytes": workspace["bytes"],
                        },
                    )
                tasks = run(
                    claimed["specification"],
                    self.state_directory / f"{run_id}.sqlite",
                    run_id,
                    directory,
                    should_cancel=cancel.is_set,
                    on_event=event,
                )
                remaining = 65536
                saved_tasks = []
                for task in tasks:
                    raw = task.get("output", "").encode("utf-8")
                    cap = min(8192, remaining)
                    output = raw[-cap:].decode("utf-8", errors="ignore") if cap else ""
                    remaining = max(0, remaining - len(output.encode("utf-8")))
                    saved_tasks.append(
                        {
                            **{key: task[key] for key in ("id", "status", "attempts")},
                            "output": output,
                            "output_truncated": len(raw) > cap,
                        }
                    )
                result = {"tasks": saved_tasks, "execution_policy": policy}
                if workspace is not None:
                    result["input_pins"] = workspace["inputs"]
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
