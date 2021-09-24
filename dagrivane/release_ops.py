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
    def alias(self, workspace, name):
        token(name, "alias name")
        with self._tx(False) as db:
            self._namespace(db, workspace)
            row = db.execute(
                "SELECT * FROM aliases WHERE workspace=? AND name=?", (workspace, name)
            ).fetchone()
            if row is None:
                raise CatalogError("unknown artifact alias")
            return dict(row)

    def move_alias(self, workspace, name, artifact_id, *, expected_generation):
        token(name, "alias name")
        if type(expected_generation) is not int or expected_generation < 0:
            raise CatalogError("alias expected generation must be nonnegative")
        with self._tx() as db:
            self._namespace(db, workspace)
            self._verify_artifact(db, workspace, artifact_id)
            previous = db.execute(
                "SELECT * FROM aliases WHERE workspace=? AND name=?", (workspace, name)
            ).fetchone()
            generation = previous["generation"] if previous else 0
            if generation != expected_generation:
                raise CatalogError("artifact alias generation conflict")
            result = {
                "workspace": workspace,
                "name": name,
                "artifact_id": artifact_id,
                "generation": generation + 1,
                "updated_at": now(),
            }
            db.execute(
                "INSERT INTO aliases VALUES (?,?,?,?,?) ON CONFLICT(workspace,name) DO UPDATE SET artifact_id=excluded.artifact_id,generation=excluded.generation,updated_at=excluded.updated_at",
                tuple(result.values()),
            )
            self._event(db, workspace, "alias_moved", result)
            return result

    def _release(self, db, workspace, identity):
        token(identity, "release ID")
        row = db.execute(
            "SELECT * FROM releases WHERE workspace=? AND id=?", (workspace, identity)
        ).fetchone()
        if row is None:
            raise CatalogError("unknown release in this workspace")
        return {**dict(row), "manifest": json.loads(row["manifest"])}

    def _closure(self, db, workspace, identities, maximum=1000):
        seen, pending = {}, list(identities)
        while pending:
            identity = pending.pop()
            if identity in seen:
                continue
            if len(seen) >= maximum:
                raise CatalogError(
                    "release provenance exceeds the supported traversal budget"
                )
            artifact = self._artifact(db, workspace, identity)
            seen[identity] = artifact
            pending.extend(artifact["parents"])
        # Kahn traversal also detects a manually corrupted catalog cycle.
        remaining = {
            identity: set(artifact["parents"]) for identity, artifact in seen.items()
        }
        ready = [identity for identity, parents in remaining.items() if not parents]
        visited = 0
        while ready:
            identity = ready.pop()
            visited += 1
            for child, parents in remaining.items():
                if identity in parents:
                    parents.remove(identity)
                    if not parents:
                        ready.append(child)
        if visited != len(seen):
            raise CatalogError("artifact provenance contains a cycle")
        return seen

    def _verify_release(self, db, workspace, identity):
        release = self._release(db, workspace, identity)
        manifest = validate_manifest(release["manifest"])
        if fingerprint(manifest) != release["sha256"]:
            raise CatalogError(
                "release manifest hash differs from its catalog identity"
            )
        bindings = manifest.get("bindings")
        if not isinstance(bindings, dict) or not bindings:
            raise CatalogError("release manifest has no artifact bindings")
        for binding in bindings.values():
            artifact = self._artifact(db, workspace, binding["id"])
            if snapshot(artifact) != binding:
                raise CatalogError(
                    "release binding differs from immutable artifact metadata"
                )
        closure = self._closure(
            db, workspace, [binding["id"] for binding in bindings.values()]
        )
        checks = [
            self._verify_artifact(db, workspace, artifact)
            for artifact in sorted(closure)
        ]
        return {
            "release": identity,
            "sha256": release["sha256"],
            "passed": True,
            "artifact_checks": checks,
            "provenance_complete": True,
        }

    def create_release(
        self, workspace, name, bindings, *, request_key, requirements=None
    ):
        token(name, "release name")
        if not isinstance(bindings, dict) or not 1 <= len(bindings) <= 32:
            raise CatalogError("release requires 1–32 named artifact bindings")
        for label, reference in bindings.items():
            token(label, "binding name")
            if not isinstance(reference, dict) or set(reference) not in (
                {"artifact"},
                {"alias"},
            ):
                raise CatalogError(
                    "release binding requires exactly one artifact or alias"
                )
            token(next(iter(reference.values())), "binding reference")
        requirements = {} if requirements is None else requirements
        if not isinstance(requirements, dict) or set(requirements) - set(bindings):
            raise CatalogError("release requirements must reference declared bindings")
        for requirement in requirements.values():
            if not isinstance(requirement, dict) or set(requirement) - {
                "kind",
                "dataset",
                "policy_version",
            }:
                raise CatalogError("release requirement contains unsupported fields")
            if "kind" in requirement and requirement["kind"] not in ("table", "build"):
                raise CatalogError("release kind requirement must be table or build")
            if "dataset" in requirement:
                token(requirement["dataset"], "required dataset")
            if "policy_version" in requirement:
                positive_version(requirement["policy_version"])
        request = {"name": name, "bindings": bindings, "requirements": requirements}
        with self._tx() as db:
            self._namespace(db, workspace)
            previous, request_hash = self._request(
                db, workspace, request_key, "create_release", request
            )
            if previous is not None:
                return previous
            if db.execute(
                "SELECT id FROM releases WHERE workspace=? AND name=?",
                (workspace, name),
            ).fetchone():
                raise CatalogError(
                    "release name already exists; use its original request key or a new release name"
                )
            resolved = {}
            for label, reference in sorted(bindings.items()):
                identity = reference.get("artifact")
                if identity is None:
                    alias = db.execute(
                        "SELECT artifact_id FROM aliases WHERE workspace=? AND name=?",
                        (workspace, reference["alias"]),
                    ).fetchone()
                    if alias is None:
                        raise CatalogError(
                            "release references an unknown artifact alias"
                        )
                    identity = alias["artifact_id"]
                artifact = self._artifact(db, workspace, identity)
                requirement = requirements.get(label, {})
                for key, value in requirement.items():
                    actual = (
                        artifact["policy_name"] if key == "dataset" else artifact[key]
                    )
                    if value != actual:
                        raise CatalogError(
                            "release binding does not satisfy its kind/dataset/policy requirement"
                        )
                resolved[label] = snapshot(artifact)
            closure = self._closure(
                db, workspace, [artifact["id"] for artifact in resolved.values()]
            )
            for identity in closure:
                self._verify_artifact(db, workspace, identity)
            manifest = {
                "format": "dagrivane.release.v1",
                "workspace": workspace,
                "name": name,
                "bindings": resolved,
                "requirements": requirements,
            }
            validate_manifest(manifest)
            identity, manifest_hash = uuid.uuid4().hex, fingerprint(manifest)
            db.execute(
                "INSERT INTO releases VALUES (?,?,?,?,?,?)",
                (
                    identity,
                    workspace,
                    name,
                    json_bytes(manifest).decode(),
                    manifest_hash,
                    now(),
                ),
            )
            result = self._release(db, workspace, identity)
            self._event(
                db,
                workspace,
                "release_created",
                {
                    "release": identity,
                    "name": name,
                    "sha256": manifest_hash,
                    "bindings": resolved,
                },
            )
            return self._remember(
                db, workspace, request_key, "create_release", request_hash, result
            )

    def release(self, workspace, identity):
        with self._tx(False) as db:
            self._namespace(db, workspace)
            return self._release(db, workspace, identity)

    def verify_release(self, workspace, identity):
        with self._tx(False) as db:
            self._namespace(db, workspace)
            return self._verify_release(db, workspace, identity)

    def create_environment(self, workspace, name, requires=None):
        token(name, "environment name")
        if requires is not None:
            token(requires, "prerequisite environment")
            if name == requires:
                raise CatalogError("environment cannot require itself")
        with self._tx() as db:
            self._namespace(db, workspace)
            previous = db.execute(
                "SELECT * FROM environments WHERE workspace=? AND name=?",
                (workspace, name),
            ).fetchone()
            if previous:
                if previous["requires"] != requires:
                    raise CatalogError("environment prerequisite is immutable")
                return dict(previous)
            if (
                requires is not None
                and db.execute(
                    "SELECT name FROM environments WHERE workspace=? AND name=?",
                    (workspace, requires),
                ).fetchone()
                is None
            ):
                raise CatalogError("prerequisite environment must already exist")
            db.execute(
                "INSERT INTO environments VALUES (?,?,?,NULL,0,?)",
                (workspace, name, requires, now()),
            )
            result = dict(
                db.execute(
                    "SELECT * FROM environments WHERE workspace=? AND name=?",
                    (workspace, name),
                ).fetchone()
            )
            self._event(db, workspace, "environment_created", result)
            return result

