"""Catalog input contracts and independent verification of typed table bundles."""

import hashlib
import copy
import json
import re
from datetime import datetime, timezone

from .contracts import Contract
from .object_store import digest_name, MAX_OBJECT_BYTES
from .quality import compile_gates
from .tables import Table

TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}$")
REVISION = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class CatalogError(ValueError):
    pass


def token(value, label):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        raise CatalogError(f"{label} must be a bounded ASCII identifier")
    return value


def positive_version(value):
    if type(value) is not int or not 1 <= value <= 1000000:
        raise CatalogError("version must be an integer between 1 and one million")
    return value


def json_bytes(value, maximum=MAX_OBJECT_BYTES):
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    except (ValueError, TypeError, RecursionError) as error:
        raise CatalogError("catalog values must contain bounded finite JSON") from error
    if len(encoded) > maximum:
        raise CatalogError("catalog JSON exceeds its byte budget")
    return encoded


def fingerprint(value):
    return hashlib.sha256(json_bytes(value)).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def parse_json(payload):
    if not isinstance(payload, bytes) or len(payload) > MAX_OBJECT_BYTES:
        raise CatalogError("catalog input must be bounded JSON bytes")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise CatalogError("duplicate catalog JSON key")
            result[key] = value
        return result

    def reject(_):
        raise CatalogError("catalog JSON numbers must be finite")

    try:
        value = json.loads(payload, object_pairs_hook=unique, parse_constant=reject)
    except RecursionError as error:
        raise CatalogError("catalog JSON nesting exceeds the parser limit") from error
    pending, count = [(value, 0)], 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if depth > 100 or count > 2500000:
            raise CatalogError("catalog JSON structure exceeds its budget")
        if isinstance(item, dict):
            pending.extend((entry, depth + 1) for entry in item.values())
        elif isinstance(item, list):
            pending.extend((entry, depth + 1) for entry in item)
    json_bytes(value)
    return value


def table_bundle(payload):
    bundle = parse_json(payload)
    if (
        not isinstance(bundle, dict)
        or set(bundle) != {"format", "report", "rows"}
        or bundle["format"] != "dagrivane.table.v1"
    ):
        raise CatalogError("unsupported typed table bundle format")
    report = bundle["report"]
    if not isinstance(report, dict) or report.get("passed") is not True:
        raise CatalogError("bundle does not declare a successful dataflow")
    summary = report.get("output")
    if not isinstance(summary, dict):
        raise CatalogError("bundle requires an output summary")
    contract = Contract.compile(summary.get("contract"))
    sources = summary.get("source_sha256")
    if not isinstance(sources, list) or len(sources) > 32:
        raise CatalogError("bundle sources must be a list of at most 32 hashes")
    for source in sources:
        digest_name(source)
    if len(set(sources)) != len(sources):
        raise CatalogError("bundle source identities must be unique")
    if not isinstance(bundle["rows"], list):
        raise CatalogError("bundle rows must be an array")
    table = Table.from_rows(contract, bundle["rows"], sources)
    actual = table.summary()
    if any(
        type(summary.get(key)) is not int or summary[key] != actual[key]
        for key in ("rows", "columns")
    ):
        raise CatalogError("bundle dimensions disagree with actual rows")
    if summary.get("data_sha256") != actual["data_sha256"]:
        raise CatalogError("bundle data identity disagrees with actual rows")
    digest_name(report.get("plan_sha256"))
    checks = report.get("checks")
    if (
        not isinstance(checks, list)
        or not 1 <= len(checks) <= 100
        or any(
            not isinstance(check, dict) or check.get("passed") is not True
            for check in checks
        )
    ):
        raise CatalogError(
            "bundle quality check declarations are incomplete or unsuccessful"
        )
    return table, report


