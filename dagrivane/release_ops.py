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


def snapshot(artifact):
    return validate_snapshot({key: artifact[key] for key in sorted(SNAPSHOT_FIELDS)})


def validate_manifest(manifest):
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"format", "workspace", "name", "bindings", "requirements"}
        or manifest["format"] != "dagrivane.release.v1"
    ):
        raise CatalogError("unsupported release manifest format")
    token(manifest["workspace"], "release workspace")
    token(manifest["name"], "release name")
    bindings = manifest["bindings"]
    if not isinstance(bindings, dict) or not 1 <= len(bindings) <= 32:
        raise CatalogError("release requires bounded named artifact bindings")
    for label, binding in bindings.items():
        token(label, "release binding name")
        validate_snapshot(binding)
        if binding["workspace"] != manifest["workspace"]:
            raise CatalogError("release binding belongs to a different workspace")
    requirements = manifest["requirements"]
    if not isinstance(requirements, dict) or set(requirements) - set(bindings):
        raise CatalogError("release requirements do not match its bindings")
    for label, rules in requirements.items():
        if not isinstance(rules, dict) or set(rules) - {
            "kind",
            "dataset",
            "policy_version",
        }:
            raise CatalogError("unsupported release requirement")
        for key, value in rules.items():
            if key == "policy_version":
                positive_version(value)
            elif key == "dataset":
                token(value, "required dataset")
            elif value not in ("table", "build"):
                raise CatalogError("unsupported required artifact kind")
            actual = (
                bindings[label]["policy_name"]
                if key == "dataset"
                else bindings[label][key]
            )
            if value != actual:
                raise CatalogError("release binding does not satisfy its requirement")
    return manifest


class ReleaseOperations:
    pass
