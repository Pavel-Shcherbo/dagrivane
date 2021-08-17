"""Pinned local release manifests and prerequisite-gated environment pointers."""

import json
import uuid
from .catalog_contracts import REVISION
from .object_store import digest_name, MAX_OBJECT_BYTES

from .catalog_contracts import (
    CatalogError,
    fingerprint,
    json_bytes,
    now,
    positive_version,
    token,
)

SNAPSHOT_FIELDS = {
    "id",
    "workspace",
    "name",
    "kind",
    "sha256",
    "bytes",
    "run_id",
    "revision",
    "policy_name",
    "policy_version",
    "parents",
}


def validate_snapshot(record):
    if not isinstance(record, dict) or set(record) != SNAPSHOT_FIELDS:
        raise CatalogError("artifact snapshot contains unsupported or missing fields")
    for field in ("id", "workspace", "name", "run_id"):
        token(record[field], "artifact " + field)
    digest_name(record["sha256"])
    if type(record["bytes"]) is not int or not 1 <= record["bytes"] <= MAX_OBJECT_BYTES:
        raise CatalogError("artifact snapshot byte count is invalid")
    parents = record["parents"]
    if not isinstance(parents, list) or len(parents) > 100:
        raise CatalogError("artifact snapshot parents must be a bounded list")
    for parent in parents:
        token(parent, "parent artifact ID")
    if len(set(parents)) != len(parents):
        raise CatalogError("artifact snapshot parent IDs must be unique")
    if record["kind"] == "table":
        token(record["policy_name"], "dataset policy name")
        positive_version(record["policy_version"])
        if record["revision"] is not None:
            raise CatalogError("table snapshot cannot claim a build revision")
    elif record["kind"] == "build":
        if (
            not isinstance(record["revision"], str)
            or not REVISION.fullmatch(record["revision"])
            or record["policy_name"] is not None
            or record["policy_version"] is not None
        ):
            raise CatalogError("build snapshot revision or policy fields are invalid")
    else:
        raise CatalogError("unsupported artifact snapshot kind")
    return record


