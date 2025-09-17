"""Explicit column projection with lossless casts and preserved stream identities."""

import copy
import json
from decimal import Decimal

from .catalog_contracts import fingerprint, json_bytes, token
from .contracts import Contract, compatibility, convert, encode
from .ingest_model import normalized_key, stream_definition, instant
from .quality import compile_gates
from .tables import Table


def compile_projection(source, value):
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "version",
            "source",
            "successor",
            "definition",
            "columns",
            "drop_columns",
            "reason",
        }
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError(
            "migration requires version 1, source/successor, definition, columns, drop_columns and reason"
        )
    token(value["source"], "source stream")
    token(value["successor"], "successor stream")
    if value["source"] == value["successor"]:
        raise ValueError("migration must create a different successor stream")
    if (
        not isinstance(value["reason"], str)
        or not value["reason"].strip()
        or len(value["reason"]) > 1000
    ):
        raise ValueError("migration requires a bounded explicit reason")
    target = stream_definition(value["definition"])
    old = Contract.compile(source["contract"])
    new = Contract.compile(target["contract"])
    if new.name != old.name or new.version != old.version + 1:
        raise ValueError(
            "migration must advance the same contract by exactly one version"
        )
    if (
        target["target"]["workspace"] != source["target"]["workspace"]
        or target["target"]["dataset"] != source["target"]["dataset"]
    ):
        raise ValueError("schema migration retains the workspace/dataset identity")
    if target["allowed_lateness_seconds"] != source["allowed_lateness_seconds"]:
        raise ValueError(
            "schema migration cannot silently change the event lateness policy"
        )
    columns = value["columns"]
    if not isinstance(columns, dict) or set(columns) != set(new.names):
        raise ValueError("projection must explicitly define every target column")
    output = {}
    used = set()
    for name, item in columns.items():
        if not isinstance(item, dict):
            raise ValueError("column projection requires an object")
        operation = item.get("operation")
        if operation == "constant" and set(item) == {"operation", "value"}:
            output[name] = {
                "operation": "constant",
                "value": encode(new.column(name).parse(item["value"])),
            }
        elif operation in ("copy", "cast") and set(item) == {"operation", "source"}:
            column = old.column(item["source"])
            used.add(column.name)
            if operation == "copy" and column.kind != new.column(name).kind:
                raise ValueError("type changes require an explicit cast")
            if operation == "cast" and column.kind == new.column(name).kind:
                raise ValueError("same-type projection uses copy rather than cast")
            output[name] = dict(item)
        else:
            raise ValueError(
                "projection requires copy/source, cast/source or constant/value"
            )
    dropped = value["drop_columns"]
    if (
        not isinstance(dropped, list)
        or any(not isinstance(v, str) for v in dropped)
        or len(set(dropped)) != len(dropped)
        or set(dropped) != set(old.names) - used
    ):
        raise ValueError(
            "drop_columns must explicitly list exactly the unused source columns"
        )
    # Identity columns may be renamed, but their types, ordering and actual values
    # cannot change. No synthetic key or event clock is introduced by migration.
    if len(target["keys"]) != len(source["keys"]):
        raise ValueError("migration must preserve key arity")
    for before, after in [
        *zip(source["keys"], target["keys"]),
        (source["event_time_column"], target["event_time_column"]),
    ]:
        if output[after] != {"operation": "copy", "source": before}:
            raise ValueError("keys and event time require exact ordered copy mappings")
    result = copy.deepcopy(value)
    result["definition"] = target
    result["columns"] = output
    result["drop_columns"] = sorted(dropped)
    json_bytes(result, 2 * 1024 * 1024)
    return result


