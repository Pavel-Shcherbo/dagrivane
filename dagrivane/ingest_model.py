"""Immutable incremental-stream contracts and deterministic event reduction."""

import copy
from datetime import timedelta
from decimal import Context, Decimal

from .catalog_contracts import fingerprint, json_bytes, parse_json, token
from .contracts import Contract, convert, encode
from .quality import compile_gates
from .tables import Table

MAX_BATCH_BYTES = 4 * 1024 * 1024
MAX_EVENTS = 10000
MAX_KEYS = 100000


def instant(value):
    return (
        convert(value, "timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def offset(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError("stream offsets require nonnegative signed 64-bit integers")
    return value


def stream_definition(value):
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "version",
            "contract",
            "keys",
            "event_time_column",
            "allowed_lateness_seconds",
            "gates",
            "target",
        }
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError(
            "stream requires version 1 and explicit contract/key/time/quality/target fields"
        )
    result = copy.deepcopy(value)
    contract = Contract.compile(value["contract"])
    result["contract"] = contract.definition()
    keys = value["keys"]
    if (
        not isinstance(keys, list)
        or not 1 <= len(keys) <= 16
        or any(not isinstance(key, str) for key in keys)
        or len(set(keys)) != len(keys)
    ):
        raise ValueError("stream keys require 1–16 distinct column names")
    for key in keys:
        if contract.column(key).nullable:
            raise ValueError("stream keys cannot be nullable")
    event_column = value["event_time_column"]
    if (
        not isinstance(event_column, str)
        or contract.column(event_column).kind != "timestamp"
        or contract.column(event_column).nullable
    ):
        raise ValueError("event-time column must be a non-nullable timestamp")
    lateness = value["allowed_lateness_seconds"]
    if type(lateness) is not int or not 0 <= lateness <= 366 * 86400:
        raise ValueError("allowed lateness must be 0–366 days in integer seconds")
    target = value["target"]
    if not isinstance(target, dict) or set(target) != {
        "workspace",
        "dataset",
        "policy_version",
    }:
        raise ValueError("stream target requires workspace, dataset and policy_version")
    token(target["workspace"], "workspace")
    token(target["dataset"], "dataset")
    if (
        type(target["policy_version"]) is not int
        or target["policy_version"] != contract.version
    ):
        raise ValueError("target policy and contract versions must match")
    compile_gates(value["gates"], contract)
    json_bytes(result, 1024 * 1024)
    return result


