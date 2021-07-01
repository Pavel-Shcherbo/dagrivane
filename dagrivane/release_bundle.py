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


