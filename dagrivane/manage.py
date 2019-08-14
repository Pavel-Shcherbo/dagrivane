"""Local operator interface for the durable workflow control plane."""

import argparse
import json
import sqlite3
from pathlib import Path

from .control import ControlError, ControlStore
from .worker import Worker


def parser():
    root = argparse.ArgumentParser(
        description="Manage trusted local workflow execution"
    )
    root.add_argument("--db", type=Path, default=Path(".local/control.sqlite"))
    commands = root.add_subparsers(dest="action", required=True)
    create = commands.add_parser(
        "workspace-create", help="Create an execution workspace"
    )
    create.add_argument("name")
    create.add_argument("--concurrency", type=int, default=2)
    commands.add_parser("workspaces", help="List local workspaces")
    register = commands.add_parser(
        "register", help="Register an immutable workflow version"
    )
    register.add_argument("workspace")
    register.add_argument("name")
    register.add_argument("spec", type=Path)
    workflows = commands.add_parser("workflows")
    workflows.add_argument("workspace")
    submit = commands.add_parser(
        "submit", help="Submit a pinned version using an idempotency key"
    )
    submit.add_argument("workspace")
    submit.add_argument("workflow")
    submit.add_argument("--version", type=int, required=True)
    submit.add_argument("--event-key", required=True)
    submit.add_argument("--priority", type=int, default=0)
    runs = commands.add_parser("runs")
    runs.add_argument("workspace")
    runs.add_argument("--status")
    runs.add_argument("--limit", type=int, default=100)
    for action in ["run", "cancel", "retry", "events"]:
        command = commands.add_parser(action)
        command.add_argument("workspace")
        command.add_argument("run")
        if action == "retry":
            command.add_argument("--event-key", required=True)
        if action == "events":
            command.add_argument("--after", type=int, default=0)
            command.add_argument("--limit", type=int, default=100)
    work = commands.add_parser("worker", help="Claim and execute one queued local run")
    work.add_argument("--identity", default="local-worker")
    work.add_argument("--state-directory", type=Path)
    work.add_argument("--lease-seconds", type=float, default=30)
    schedule = commands.add_parser(
        "schedule-create", help="Create a pinned UTC interval schedule"
    )
    schedule.add_argument("workspace")
    schedule.add_argument("workflow")
    schedule.add_argument("--version", type=int, required=True)
    schedule.add_argument("--interval", type=int, required=True)
    schedule.add_argument(
        "--first-due", type=float, required=True, help="Unix timestamp"
    )
    toggle = commands.add_parser("schedule-set")
    toggle.add_argument("workspace")
    toggle.add_argument("schedule")
    toggle.add_argument(
        "--enabled", action=argparse.BooleanOptionalAction, required=True
    )
    tick = commands.add_parser(
        "tick", help="Submit currently due schedules, coalescing missed intervals"
    )
    tick.add_argument("--limit", type=int, default=100)
    return root


def operate(args, store):
    action = args.action
    if action == "workspace-create":
        return store.create_workspace(args.name, args.concurrency)
    if action == "workspaces":
        return store.list_workspaces()
    if action == "register":
        spec = json.loads(args.spec.read_text())
        return store.register_workflow(
            args.workspace, args.name, spec, args.spec.resolve().parent
        )
    if action == "workflows":
        return store.list_workflows(args.workspace)
    if action == "submit":
        return store.enqueue(
            args.workspace, args.workflow, args.version, args.event_key, args.priority
        )
    if action == "runs":
        return store.list_runs(args.workspace, args.status, args.limit)
    if action == "run":
        return store.get_run(args.workspace, args.run)
    if action == "cancel":
        return store.cancel(args.workspace, args.run)
    if action == "retry":
        return store.retry(args.workspace, args.run, args.event_key)
    if action == "events":
        return store.events(args.workspace, args.run, args.after, args.limit)
    if action == "worker":
        directory = args.state_directory or args.db.resolve().parent / "task-state"
        return Worker(store, directory, args.identity, args.lease_seconds).run_once()
    if action == "schedule-create":
        return store.create_schedule(
            args.workspace, args.workflow, args.version, args.interval, args.first_due
        )
    if action == "schedule-set":
        store.set_schedule_enabled(args.workspace, args.schedule, args.enabled)
        return {"id": args.schedule, "enabled": args.enabled}
    if action == "tick":
        return store.tick(limit=args.limit)
    raise ValueError("unknown management action")


def main():
    cli = parser()
    args = cli.parse_args()
    try:
        store = ControlStore(args.db)
        result = operate(args, store)
    except (ValueError, ControlError, OSError, sqlite3.Error) as error:
        cli.exit(2, f"{error}\n")
    print(json.dumps(result, indent=2, allow_nan=False))
    if (
        args.action == "worker"
        and result
        and result["status"] in {"failed", "reconciliation_required"}
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
