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


def normalized_key(contract, keys, value):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError("event keys must match the declared stream key columns")
    result = {}
    for key in keys:
        parsed = contract.column(key).parse(value[key])
        if isinstance(parsed, Decimal):
            parsed = (
                parsed.normalize(context=Context(prec=80)) if parsed else Decimal(0)
            )
        result[key] = encode(parsed)
    raw = json_bytes(result, 1024)
    return raw.decode(), result


def batch_definition(raw, definition):
    if not isinstance(raw, bytes) or len(raw) > MAX_BATCH_BYTES:
        raise ValueError("incremental batch requires at most 4 MiB of JSON bytes")
    value = parse_json(raw)
    if (
        not isinstance(value, dict)
        or set(value) - {"version", "after", "through", "events", "source_sha256"}
        or not {"version", "after", "through", "events"} <= set(value)
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("batch requires version 1, after, through and events")
    after = offset(value["after"])
    through = offset(value["through"])
    if through <= after:
        raise ValueError("batch through offset must advance after")
    events = value["events"]
    sources = value.get("source_sha256", [])
    if (
        not isinstance(sources, list)
        or len(sources) > 8
        or any(
            not isinstance(item, str)
            or len(item) != 64
            or any(c not in "0123456789abcdef" for c in item)
            for item in sources
        )
        or len(set(sources)) != len(sources)
    ):
        raise ValueError(
            "source_sha256 supports at most eight distinct lowercase hashes"
        )
    if not isinstance(events, list) or len(events) > MAX_EVENTS:
        raise ValueError("batch supports at most 10,000 ordered events")
    contract = Contract.compile(definition["contract"])
    output = []
    previous = after
    ids = set()
    for event in events:
        if not isinstance(event, dict) or set(event) != {
            "id",
            "offset",
            "operation",
            "key",
            "at",
            "row",
        }:
            raise ValueError("events require id, offset, operation, key, at and row")
        event_id = token(event["id"], "event ID")
        position = offset(event["offset"])
        if event_id in ids or not previous < position <= through:
            raise ValueError(
                "event IDs must be unique and offsets strictly increasing within the batch range"
            )
        ids.add(event_id)
        previous = position
        key_json, key = normalized_key(contract, definition["keys"], event["key"])
        at = instant(event["at"])
        operation = event["operation"]
        if operation == "upsert":
            row = contract.parse_row(event["row"])
            row = {name: encode(v) for name, v in row.items()}
            if (
                normalized_key(
                    contract,
                    definition["keys"],
                    {name: row[name] for name in definition["keys"]},
                )[0]
                != key_json
            ):
                raise ValueError("event key differs from row key")
            if instant(row[definition["event_time_column"]]) != at:
                raise ValueError("event at differs from row event-time column")
        elif operation == "delete":
            if event["row"] is not None:
                raise ValueError("delete events require a null row")
            row = None
        else:
            raise ValueError("event operation must be upsert or delete")
        normalized = {
            "id": event_id,
            "offset": position,
            "operation": operation,
            "key": key,
            "at": at,
            "row": row,
        }
        normalized["key_json"] = key_json
        normalized["content_sha256"] = fingerprint(
            {key: normalized[key] for key in ["operation", "key", "at", "row"]}
        )
        output.append(normalized)
    return {
        "version": 1,
        "after": after,
        "through": through,
        "source_sha256": sources,
        "events": output,
    }


def reduce_events(definition, records, seen, watermark, events):
    """Apply source-offset order with event-time staleness and tombstones."""
    records = copy.deepcopy(records)
    seen = dict(seen)
    counts = {"applied": 0, "duplicate": 0, "stale": 0}
    outcomes = []
    maximum = convert(watermark, "timestamp") if watermark is not None else None
    for event in events:
        prior = seen.get(event["id"])
        if prior is not None:
            if prior != event["content_sha256"]:
                raise ValueError("event ID was already used for different content")
            outcome = "duplicate"
        else:
            at = convert(event["at"], "timestamp")
            if maximum is not None:
                try:
                    boundary = maximum - timedelta(
                        seconds=definition["allowed_lateness_seconds"]
                    )
                except OverflowError:
                    boundary = maximum.replace(
                        year=1,
                        month=1,
                        day=1,
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                if at < boundary:
                    raise ValueError(
                        "event precedes the allowed-lateness boundary; batch was not committed"
                    )
            maximum = at if maximum is None else max(maximum, at)
            current = records.get(event["key_json"])
            if current is not None and event["at"] < current["at"]:
                outcome = "stale"
            else:
                records[event["key_json"]] = {
                    "row": event["row"],
                    "at": event["at"],
                    "offset": event["offset"],
                    "event_id": event["id"],
                }
                outcome = "applied"
            seen[event["id"]] = event["content_sha256"]
        counts[outcome] += 1
        outcomes.append(
            {"id": event["id"], "offset": event["offset"], "outcome": outcome}
        )
    if len(records) > MAX_KEYS:
        raise ValueError("stream exceeds 100,000 keys including retained tombstones")
    contract = Contract.compile(definition["contract"])
    table = Table.from_rows(
        contract,
        [
            records[key]["row"]
            for key in sorted(records)
            if records[key]["row"] is not None
        ],
    )
    checks = [
        gate.evaluate(table) for gate in compile_gates(definition["gates"], contract)
    ]
    if not all(check["passed"] for check in checks):
        raise ValueError(
            "materialized stream failed quality gates; batch was not committed"
        )
    state_sha = fingerprint([{"key": key, **records[key]} for key in sorted(records)])
    return (
        records,
        seen,
        instant(maximum) if maximum is not None else None,
        counts,
        outcomes,
        table,
        checks,
        state_sha,
    )
