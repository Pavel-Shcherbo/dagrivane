"""Strict browser controls over reviewed local source profiles and retained evidence."""

import sqlite3
from urllib.parse import parse_qs, urlsplit

from .catalog_contracts import token
from .ingestion_desk import IngestionDesk, digest, loopback_origin
from .ingestion_views import require_profile
from .resources import state_directory

ORIGIN = None
FIELDS = {
    "overview": set(),
    "initialize": set(),
    "inspect": {"profile", "after_page", "after_generation", "row_offset"},
    "poll": {"profile", "request_key", "expected_revision"},
    "publish": {"profile", "request_key", "expected_revision"},
    "repair": {"profile", "request_key", "expected_revision", "page_id", "reason"},
    "discard": {"profile", "request_key", "expected_revision", "page_id", "reason"},
    "page": {"profile", "page_id"},
    "artifact": {"profile", "generation"},
    "audit": {"profile"},
    "operation": {"profile", "request_key"},
}


def start_origin(origin):
    global ORIGIN
    ORIGIN = loopback_origin(origin)


def nonnegative(value, label):
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError(label + " must be a bounded nonnegative integer")
    return value


def desk():
    return IngestionDesk(state_directory("dagrivane") / "ingestion-workbench", ORIGIN)


def evaluate_ingestion(params):
    if not isinstance(params, dict):
        raise ValueError("ingestion request requires an object")
    action = params.get("action", "overview")
    if (
        not isinstance(action, str)
        or action not in FIELDS
        or set(params) - ({"mode", "action"} | FIELDS[action])
    ):
        raise ValueError("unknown ingestion action or unsupported fields")
    if "profile" in FIELDS[action]:
        require_profile(params.get("profile"))
    for key in ("after_page", "after_generation", "row_offset"):
        if key in params:
            nonnegative(params[key], key)
    if action in ("poll", "publish", "repair", "discard"):
        token(params.get("request_key"), "ingestion request key")
        digest(params.get("expected_revision"), "view revision")
    if action in ("repair", "discard", "page"):
        digest(params.get("page_id"), "retained page")
    if action in ("repair", "discard"):
        reason = params.get("reason")
        if (
            not isinstance(reason, str)
            or not reason.strip()
            or len(reason) > 512
            or any(ord(char) < 32 for char in reason)
        ):
            raise ValueError("review requires a reason of 1–512 printable characters")
    if (
        action == "artifact"
        and nonnegative(params.get("generation"), "generation") == 0
    ):
        raise ValueError("generation must be positive")
    if action == "operation":
        token(params.get("request_key"), "ingestion request key")
    instance = desk()
    try:
        if action == "overview":
            return instance.overview()
        if action == "initialize":
            return instance.initialize()
        if action == "inspect":
            return instance.inspect(
                params["profile"],
                **{
                    key: params[key]
                    for key in ("after_page", "after_generation", "row_offset")
                    if key in params
                },
            )
        if action in ("poll", "publish", "repair", "discard"):
            return instance.mutate(
                params["profile"],
                action,
                **{
                    key: params[key]
                    for key in ("request_key", "expected_revision", "page_id", "reason")
                    if key in params
                },
            )
        if action == "page":
            return instance.page(params["profile"], params["page_id"])
        if action == "artifact":
            return instance.artifact(params["profile"], params["generation"])
        if action == "audit":
            return instance.audit(params["profile"])
        return instance.operation(params["profile"], params["request_key"])
    except sqlite3.Error as error:
        raise ValueError("ingestion workbench database: " + str(error)) from error


