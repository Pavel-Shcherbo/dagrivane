"""Create-only local release exports and independent offline content verification."""

import hashlib
import os
import shutil
import tempfile
from pathlib import Path

from .catalog_contracts import (
    CatalogError,
    fingerprint,
    json_bytes,
    parse_json,
    table_bundle,
    verify_policy,
)
from .object_store import bounded_file, digest_name
from .release_ops import snapshot, validate_snapshot, validate_manifest

MAX_EXPORT_BYTES = 256 * 1024 * 1024


def export_release(catalog, workspace, release_id, destination):
    target = Path(destination).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError("release export destination already exists")
    staged = Path(tempfile.mkdtemp(prefix=".dagrivane-release-", dir=target.parent))
    reserved = False
    try:
        (staged / "objects").mkdir()
        with catalog._tx(False) as db:
            catalog._namespace(db, workspace)
            catalog._verify_release(db, workspace, release_id)
            release = catalog._release(db, workspace, release_id)
            records = catalog._closure(
                db,
                workspace,
                [binding["id"] for binding in release["manifest"]["bindings"].values()],
                maximum=256,
            )
            if sum(record["bytes"] for record in records.values()) > MAX_EXPORT_BYTES:
                raise CatalogError("release export exceeds 256 MiB")
            artifacts = []
            for identity, record in sorted(records.items()):
                payload = catalog.objects.get(record["sha256"])
                filename = record["sha256"]
                object_path = staged / "objects" / filename
                if not object_path.exists():
                    with object_path.open("xb") as stream:
                        stream.write(payload)
                        stream.flush()
                        os.fsync(stream.fileno())
                entry = {
                    "artifact": snapshot(record),
                    "metadata": record["metadata"],
                    "file": "objects/" + filename,
                }
                if record["kind"] == "table":
                    entry["policy"] = catalog._policy(
                        db, workspace, record["policy_name"], record["policy_version"]
                    )["definition"]
                artifacts.append(entry)
            descriptor = {
                "format": "dagrivane.release-export.v1",
                "release_id": release_id,
                "release": release["manifest"],
                "release_sha256": release["sha256"],
                "artifacts": artifacts,
            }
        with (staged / "release.json").open("xb") as stream:
            stream.write(json_bytes(descriptor))
            stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        verification = verify_export(staged)
        target.mkdir()
        reserved = True
        os.replace(staged / "objects", target / "objects")
        # The descriptor is the completion marker, published only after objects.
        os.replace(staged / "release.json", target / "release.json")
        return {
            "path": str(target),
            "release_id": release_id,
            "release_sha256": release["sha256"],
            "verification": verification,
        }
    except BaseException:
        if reserved:
            shutil.rmtree(target)
        raise
    finally:
        shutil.rmtree(staged)


def verify_export(directory):
    root = Path(directory).resolve()
    if (root / "release.json").is_symlink():
        raise CatalogError("release descriptor cannot be a symlink")
    descriptor = parse_json(bounded_file(root / "release.json"))
    if (
        not isinstance(descriptor, dict)
        or set(descriptor)
        != {"format", "release_id", "release", "release_sha256", "artifacts"}
        or descriptor["format"] != "dagrivane.release-export.v1"
    ):
        raise CatalogError("unsupported release export descriptor")
    release = validate_manifest(descriptor["release"])
    if (
        fingerprint(release) != descriptor["release_sha256"]
        or not isinstance(release, dict)
        or release.get("format") != "dagrivane.release.v1"
    ):
        raise CatalogError("release export manifest identity is invalid")
    entries = descriptor["artifacts"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= 256:
        raise CatalogError("release export requires 1–256 artifact entries")
    records, checked, total = {}, [], 0
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or set(entry) - {"artifact", "metadata", "file", "policy"}
            or not {"artifact", "metadata", "file"} <= set(entry)
        ):
            raise CatalogError("release export artifact contains unsupported fields")
        artifact = validate_snapshot(entry["artifact"])
        if (
            not isinstance(artifact, dict)
            or artifact.get("workspace") != release.get("workspace")
            or not isinstance(artifact.get("id"), str)
            or artifact["id"] in records
        ):
            raise CatalogError(
                "export artifact identity/workspace is invalid or repeated"
            )
        identity = digest_name(artifact.get("sha256"))
        if entry["file"] != "objects/" + identity:
            raise CatalogError("export object path differs from its content identity")
        path = root / entry["file"]
        if (
            path.is_symlink()
            or path.parent.is_symlink()
            or not path.resolve().is_relative_to(root)
        ):
            raise CatalogError(
                "export object symlinks or escaping paths are unsupported"
            )
        payload = bounded_file(path)
        total += len(payload)
        if total > MAX_EXPORT_BYTES:
            raise CatalogError("release export exceeds 256 MiB")
        if (
            type(artifact.get("bytes")) is not int
            or len(payload) != artifact["bytes"]
            or hashlib.sha256(payload).hexdigest() != identity
        ):
            raise CatalogError("export object hash or byte count is invalid")
        if artifact.get("kind") == "table":
            table, report = table_bundle(payload)
            definition = entry.get("policy")
            if not isinstance(definition, dict):
                raise CatalogError("exported table requires its dataset policy")
            verify_policy(table, definition)
            metadata = entry["metadata"]
            if (
                metadata.get("table") != table.summary()
                or metadata.get("plan_sha256") != report["plan_sha256"]
                or metadata.get("policy_sha256") != fingerprint(definition)
                or artifact.get("policy_version") != definition.get("version")
            ):
                raise CatalogError(
                    "exported table metadata or policy identity disagrees"
                )
        elif artifact.get("kind") != "build":
            raise CatalogError("unsupported exported artifact kind")
        records[artifact["id"]] = artifact
        checked.append(
            {
                "artifact": artifact["id"],
                "sha256": identity,
                "bytes": len(payload),
                "verified": True,
            }
        )
    bindings = release.get("bindings")
    if not isinstance(bindings, dict) or not 1 <= len(bindings) <= 32:
        raise CatalogError("release export requires bounded artifact bindings")
    for binding in bindings.values():
        if not isinstance(binding, dict) or records.get(binding.get("id")) != binding:
            raise CatalogError(
                "release binding differs from its exported artifact snapshot"
            )
    remaining = {}
    for identity, artifact in records.items():
        parents = artifact.get("parents")
        if (
            not isinstance(parents, list)
            or any(
                not isinstance(parent, str) or parent not in records
                for parent in parents
            )
            or len(parents) != len(set(parents))
        ):
            raise CatalogError("release export provenance is incomplete")
        remaining[identity] = set(parents)
    ready = [identity for identity, parents in remaining.items() if not parents]
    visited = set()
    while ready:
        identity = ready.pop()
        if identity in visited:
            continue
        visited.add(identity)
        for child, parents in remaining.items():
            parents.discard(identity)
            if not parents and child not in visited:
                ready.append(child)
    if len(visited) != len(records):
        raise CatalogError("release export provenance contains a cycle")
    return {
        "passed": True,
        "release_id": descriptor["release_id"],
        "release_sha256": descriptor["release_sha256"],
        "verified_artifacts": checked,
        "provenance_complete": True,
        "bytes": total,
        "scope": "content, contracts, quality policies and declared provenance; no signatures or deployment execution",
    }
