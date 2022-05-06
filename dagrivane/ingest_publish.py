"""Recoverable publication from ingestion's ordered outbox to the artifact catalog."""

import tempfile
from pathlib import Path

from .catalog_contracts import fingerprint, policy_definition


def configure_target(store, name, catalog):
    definition = store.status(name)["definition"]
    target = definition["target"]
    catalog.create_workspace(target["workspace"])
    return catalog.register_policy(
        target["workspace"],
        target["dataset"],
        target["policy_version"],
        definition["contract"],
        definition["gates"],
    )


