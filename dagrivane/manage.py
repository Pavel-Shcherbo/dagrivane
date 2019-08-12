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


