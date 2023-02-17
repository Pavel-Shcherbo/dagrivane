"""Validate loopback operation requests before accessing durable local state."""

import sqlite3
import uuid

from .control import ControlError, STATES, text
from .operations import Operations, PROFILES
from .resources import state_directory

FIELDS = {
    "overview": {"before", "status"},
    "initialize": set(),
    "publish": {"profile", "expected_version"},
    "submit": {"version", "event_key", "priority"},
    "inspect": {"run_id", "after"},
    "work": {"run_id"},
    "cancel": {"run_id"},
    "retry": {"run_id", "event_key"},
}


def positive(value, label, *, zero=False):
    if type(value) is not int or not (0 if zero else 1) <= value <= 9223372036854775807:
        raise ValueError(
            label
            + " requires a "
            + ("nonnegative" if zero else "positive")
            + " integer"
        )
    return value


def evaluate_operations(params):
    if not isinstance(params, dict):
        raise ValueError("operations request requires an object")
    action = params.get("action", "overview")
    if not isinstance(action, str) or action not in FIELDS:
        raise ValueError("unknown operations action")
    if set(params) - ({"mode", "action"} | FIELDS[action]):
        raise ValueError("operations request contains unsupported fields")
    if "run_id" in FIELDS[action]:
        value = params.get("run_id")
        if not isinstance(value, str) or len(value) != 36:
            raise ValueError("run ID must be a canonical UUID")
        try:
            parsed = str(uuid.UUID(value))
        except ValueError as error:
            raise ValueError("run ID must be a canonical UUID") from error
        if parsed != value:
            raise ValueError("run ID must be a canonical UUID")
    if action == "overview":
        if params.get("before") is not None:
            positive(params["before"], "run cursor")
        if params.get("status") is not None and (
            not isinstance(params["status"], str) or params["status"] not in STATES
        ):
            raise ValueError("unknown queue status")
    if action == "publish":
        if (
            not isinstance(params.get("profile"), str)
            or params["profile"] not in PROFILES
        ):
            raise ValueError("unknown reviewed profile")
        positive(params.get("expected_version"), "expected version")
    if action == "submit":
        positive(params.get("version"), "workflow version")
        if (
            type(params.get("priority", 0)) is not int
            or not -100 <= params.get("priority", 0) <= 100
        ):
            raise ValueError("priority must be an integer between -100 and 100")
    if action in ("submit", "retry"):
        text(params.get("event_key"), "event key", 100)
    if action == "inspect":
        positive(params.get("after", 0), "event cursor", zero=True)
    operations = Operations(state_directory("dagrivane") / "operations")
    try:
        if action == "overview":
            return operations.overview(
                before=params.get("before"), status=params.get("status")
            )
        if action == "initialize":
            return operations.initialize()
        if action == "publish":
            return operations.publish_version(
                params["profile"], params["expected_version"]
            )
        if action == "submit":
            return operations.submit(
                params["version"], params["event_key"], params.get("priority", 0)
            )
        if action == "inspect":
            return operations.inspect(params["run_id"], after=params.get("after", 0))
        if action == "work":
            return operations.work(params["run_id"])
        if action == "cancel":
            return operations.cancel(params["run_id"])
        return operations.retry(params["run_id"], params["event_key"])
    except (ControlError, sqlite3.Error) as error:
        raise ValueError(str(error)) from error
