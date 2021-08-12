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


