"""Transactional dataset policies, content identities and artifact provenance."""

import json
import hashlib
import os
import re
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path

from .catalog_contracts import (
    CatalogError,
    REVISION,
    fingerprint,
    json_bytes,
    now,
    policy_definition,
    positive_version,
    table_bundle,
    token,
    verify_policy,
)
from .contracts import Contract, compatibility
from .object_store import ObjectStore, bounded_file
from .release_ops import ReleaseOperations

SCHEMA_VERSION = 1


class Catalog(ReleaseOperations):
    pass
