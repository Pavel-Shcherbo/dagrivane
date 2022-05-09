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


def publish_next(store, name, catalog, *, lease_seconds=30, after_publish=None):
    if after_publish is not None and not callable(after_publish):
        raise ValueError("publication callback must be callable")
    definition = store.status(name)["definition"]
    target = definition["target"]
    expected, _, _ = policy_definition(
        definition["contract"], definition["gates"], target["policy_version"]
    )
    policy = catalog.policy(
        target["workspace"], target["dataset"], target["policy_version"]
    )
    if policy["sha256"] != fingerprint(expected):
        raise ValueError("target policy differs from the immutable stream policy")
    claim = store.claim(name, catalog.root, lease_seconds=lease_seconds)
    if claim is None:
        return {
            "published": False,
            "reason": "no_claimable_batch",
            "publication_counts": store.status(name)["publication_counts"],
        }
    try:
        with tempfile.TemporaryDirectory(prefix="dagrivane-publish-") as directory:
            path = Path(directory) / "table.json"
            path.write_bytes(claim["bundle"])
            artifact = catalog.publish_table(
                target["workspace"],
                target["dataset"],
                target["policy_version"],
                path,
                run_id=claim["request_key"],
                request_key=claim["request_key"],
                parents=claim["parents"],
            )
        verification = catalog.verify_artifact(target["workspace"], artifact["id"])
        if not verification["passed"]:
            raise ValueError("catalog artifact did not pass independent verification")
        if after_publish is not None:
            after_publish(artifact)
        store.acknowledge(claim, artifact)
        return {
            "published": True,
            "stream": name,
            "generation": claim["generation"],
            "batch_id": claim["batch_id"],
            "artifact": artifact,
        }
    except Exception as error:
        store.release(claim, error)
        raise


