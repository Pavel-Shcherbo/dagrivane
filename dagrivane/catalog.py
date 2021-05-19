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
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.objects = ObjectStore(self.root / "objects")
        self.path = self.root / "catalog.sqlite"
        created = not self.path.exists()
        with self._tx() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL)"
            )
            row = db.execute(
                "SELECT version FROM metadata WHERE singleton=1"
            ).fetchone()
            if row is not None and row["version"] != SCHEMA_VERSION:
                raise CatalogError("unsupported artifact catalog schema version")
            db.execute("INSERT OR IGNORE INTO metadata VALUES (1,?)", (SCHEMA_VERSION,))
            for statement in [
                "CREATE TABLE IF NOT EXISTS namespaces (name TEXT PRIMARY KEY,created_at TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS policies (workspace TEXT NOT NULL REFERENCES namespaces(name),name TEXT NOT NULL,version INTEGER NOT NULL,payload TEXT NOT NULL,fingerprint TEXT NOT NULL,compatibility TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace,name,version))",
                "CREATE TABLE IF NOT EXISTS artifacts (seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,workspace TEXT NOT NULL REFERENCES namespaces(name),name TEXT NOT NULL,kind TEXT NOT NULL,sha256 TEXT NOT NULL,bytes INTEGER NOT NULL,run_id TEXT NOT NULL,revision TEXT,policy_name TEXT,policy_version INTEGER,metadata TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(id,workspace),FOREIGN KEY(workspace,policy_name,policy_version) REFERENCES policies(workspace,name,version))",
                "CREATE TABLE IF NOT EXISTS dataset_versions (workspace TEXT NOT NULL,name TEXT NOT NULL,version INTEGER NOT NULL,artifact_id TEXT UNIQUE NOT NULL,PRIMARY KEY(workspace,name,version),FOREIGN KEY(artifact_id,workspace) REFERENCES artifacts(id,workspace))",
                "CREATE TABLE IF NOT EXISTS parents (workspace TEXT NOT NULL,child TEXT NOT NULL,parent TEXT NOT NULL,PRIMARY KEY(workspace,child,parent),FOREIGN KEY(child,workspace) REFERENCES artifacts(id,workspace),FOREIGN KEY(parent,workspace) REFERENCES artifacts(id,workspace))",
                "CREATE TABLE IF NOT EXISTS requests (workspace TEXT NOT NULL REFERENCES namespaces(name),key TEXT NOT NULL,operation TEXT NOT NULL,fingerprint TEXT NOT NULL,result TEXT NOT NULL,PRIMARY KEY(workspace,key))",
                "CREATE TABLE IF NOT EXISTS catalog_events (seq INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT NOT NULL REFERENCES namespaces(name),kind TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS aliases (workspace TEXT NOT NULL,name TEXT NOT NULL,artifact_id TEXT NOT NULL,generation INTEGER NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(workspace,name),FOREIGN KEY(artifact_id,workspace) REFERENCES artifacts(id,workspace))",
                "CREATE TABLE IF NOT EXISTS releases (id TEXT PRIMARY KEY,workspace TEXT NOT NULL REFERENCES namespaces(name),name TEXT NOT NULL,manifest TEXT NOT NULL,sha256 TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(id,workspace),UNIQUE(workspace,name))",
                "CREATE TABLE IF NOT EXISTS environments (workspace TEXT NOT NULL REFERENCES namespaces(name),name TEXT NOT NULL,requires TEXT,release_id TEXT,generation INTEGER NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(workspace,name),FOREIGN KEY(workspace,requires) REFERENCES environments(workspace,name),FOREIGN KEY(release_id,workspace) REFERENCES releases(id,workspace))",
                "CREATE INDEX IF NOT EXISTS artifact_workspace_cursor ON artifacts(workspace,seq)",
                "CREATE INDEX IF NOT EXISTS parent_reverse ON parents(workspace,parent,child)",
            ]:
                db.execute(statement)
        if created:
            os.chmod(self.path, 0o600)

    @contextmanager
    def _tx(self, write=True):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _namespace(self, db, workspace):
        token(workspace, "workspace")
        if (
            db.execute(
                "SELECT name FROM namespaces WHERE name=?", (workspace,)
            ).fetchone()
            is None
        ):
            raise CatalogError("unknown catalog workspace")

    def create_workspace(self, name):
        token(name, "workspace")
        with self._tx() as db:
            db.execute("INSERT OR IGNORE INTO namespaces VALUES (?,?)", (name, now()))
            return dict(
                db.execute("SELECT * FROM namespaces WHERE name=?", (name,)).fetchone()
            )

    def _event(self, db, workspace, kind, payload):
        db.execute(
            "INSERT INTO catalog_events(workspace,kind,payload,created_at) VALUES (?,?,?,?)",
            (workspace, kind, json_bytes(payload, 1024 * 1024).decode(), now()),
        )

    def _request(self, db, workspace, key, operation, payload):
        token(key, "request key")
        identity = fingerprint(payload)
        row = db.execute(
            "SELECT * FROM requests WHERE workspace=? AND key=?", (workspace, key)
        ).fetchone()
        if row:
            if row["operation"] != operation or row["fingerprint"] != identity:
                raise CatalogError(
                    "request key conflicts with a different operation or payload"
                )
            return json.loads(row["result"]), identity
        return None, identity

    def _remember(self, db, workspace, key, operation, identity, result):
        db.execute(
            "INSERT INTO requests VALUES (?,?,?,?,?)",
            (
                workspace,
                key,
                operation,
                identity,
                json_bytes(result, 1024 * 1024).decode(),
            ),
        )
        return result

    def register_policy(
        self, workspace, name, version, contract, gates, *, allow_breaking=False
    ):
        token(name, "dataset name")
        if type(allow_breaking) is not bool:
            raise CatalogError("allow_breaking must be boolean")
        definition, compiled, _ = policy_definition(contract, gates, version)
        identity = fingerprint(definition)
        with self._tx() as db:
            self._namespace(db, workspace)
            existing = db.execute(
                "SELECT * FROM policies WHERE workspace=? AND name=? AND version=?",
                (workspace, name, version),
            ).fetchone()
            if existing:
                if existing["fingerprint"] != identity:
                    raise CatalogError(
                        "policy version is immutable and has different content"
                    )
                return self._policy(db, workspace, name, version)
            previous = db.execute(
                "SELECT * FROM policies WHERE workspace=? AND name=? ORDER BY version DESC LIMIT 1",
                (workspace, name),
            ).fetchone()
            expected = previous["version"] + 1 if previous else 1
            if version != expected:
                raise CatalogError(
                    "dataset policy versions must start at one and increase consecutively"
                )
            report = {"compatible": True, "issues": []}
            if previous:
                old = json.loads(previous["payload"])
                report = compatibility(Contract.compile(old["contract"]), compiled)
                if not report["compatible"] and not allow_breaking:
                    raise CatalogError(
                        "breaking contract evolution requires allow_breaking=True and an explicit migration decision"
                    )
            db.execute(
                "INSERT INTO policies VALUES (?,?,?,?,?,?,?)",
                (
                    workspace,
                    name,
                    version,
                    json_bytes(definition).decode(),
                    identity,
                    json_bytes(report).decode(),
                    now(),
                ),
            )
            self._event(
                db,
                workspace,
                "policy_registered",
                {
                    "dataset": name,
                    "version": version,
                    "sha256": identity,
                    "compatibility": report,
                    "breaking_allowed": allow_breaking,
                },
            )
            return self._policy(db, workspace, name, version)

    def _policy(self, db, workspace, name, version):
        row = db.execute(
            "SELECT * FROM policies WHERE workspace=? AND name=? AND version=?",
            (workspace, name, version),
        ).fetchone()
        if row is None:
            raise CatalogError("unknown dataset policy version")
        definition = json.loads(row["payload"])
        if fingerprint(definition) != row["fingerprint"]:
            raise CatalogError("stored dataset policy failed fingerprint verification")
        return {
            "workspace": workspace,
            "name": name,
            "version": version,
            "definition": definition,
            "sha256": row["fingerprint"],
            "compatibility": json.loads(row["compatibility"]),
        }

    def policy(self, workspace, name, version):
        token(name, "dataset name")
        positive_version(version)
        with self._tx(False) as db:
            self._namespace(db, workspace)
            return self._policy(db, workspace, name, version)

    def _artifact(self, db, workspace, identity):
        token(identity, "artifact ID")
        row = db.execute(
            "SELECT * FROM artifacts WHERE workspace=? AND id=?", (workspace, identity)
        ).fetchone()
        if row is None:
            raise CatalogError("unknown artifact in this workspace")
        result = dict(row)
        result["metadata"] = json.loads(result["metadata"])
        result["parents"] = [
            row["parent"]
            for row in db.execute(
                "SELECT parent FROM parents WHERE workspace=? AND child=? ORDER BY parent",
                (workspace, identity),
            )
        ]
        return result

    def _parents(self, db, workspace, parents):
        if not isinstance(parents, (list, tuple)) or len(parents) > 100:
            raise CatalogError("artifact parents must contain at most 100 IDs")
        for parent in parents:
            self._artifact(db, workspace, parent)
        if len(set(parents)) != len(parents):
            raise CatalogError("artifact parent IDs must be unique")
        return sorted(parents)

    def _insert_artifact(
        self,
        db,
        workspace,
        name,
        kind,
        payload,
        run_id,
        parents,
        metadata,
        *,
        revision=None,
        policy_name=None,
        policy_version=None,
    ):
        stored = self.objects.put(payload)
        identity = uuid.uuid4().hex
        db.execute(
            "INSERT INTO artifacts(id,workspace,name,kind,sha256,bytes,run_id,revision,policy_name,policy_version,metadata,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                identity,
                workspace,
                name,
                kind,
                stored["sha256"],
                stored["bytes"],
                run_id,
                revision,
                policy_name,
                policy_version,
                json_bytes(metadata).decode(),
                now(),
            ),
        )
        db.executemany(
            "INSERT INTO parents VALUES (?,?,?)",
            [(workspace, identity, parent) for parent in parents],
        )
        self._event(
            db,
            workspace,
            "artifact_recorded",
            {
                "artifact": identity,
                "kind": kind,
                "sha256": stored["sha256"],
                "parents": parents,
            },
        )
        return identity

    def publish_table(
        self, workspace, name, policy_version, path, *, run_id, request_key, parents=()
    ):
        token(name, "dataset name")
        token(run_id, "run ID")
        positive_version(policy_version)
        payload = bounded_file(path)
        table, report = table_bundle(payload)
        with self._tx() as db:
            self._namespace(db, workspace)
            parents = self._parents(db, workspace, parents)
            policy = self._policy(db, workspace, name, policy_version)
            checks = verify_policy(table, policy["definition"])
            request = {
                "dataset": name,
                "policy_version": policy_version,
                "blob_sha256": hashlib.sha256(payload).hexdigest(),
                "run_id": run_id,
                "parents": parents,
            }
            previous, request_hash = self._request(
                db, workspace, request_key, "publish_table", request
            )
            if previous is not None:
                return previous
            sequence = db.execute(
                "SELECT COALESCE(MAX(version),0)+1 FROM dataset_versions WHERE workspace=? AND name=?",
                (workspace, name),
            ).fetchone()[0]
            metadata = {
                "dataset_version": sequence,
                "table": table.summary(),
                "quality_checks": checks,
                "plan_sha256": report["plan_sha256"],
                "policy_sha256": policy["sha256"],
            }
            identity = self._insert_artifact(
                db,
                workspace,
                name,
                "table",
                payload,
                run_id,
                parents,
                metadata,
                policy_name=name,
                policy_version=policy_version,
            )
            db.execute(
                "INSERT INTO dataset_versions VALUES (?,?,?,?)",
                (workspace, name, sequence, identity),
            )
            result = self._artifact(db, workspace, identity)
            return self._remember(
                db, workspace, request_key, "publish_table", request_hash, result
            )

    def record_build(
        self,
        workspace,
        name,
        path,
        *,
        revision,
        run_id,
        request_key,
        parents=(),
        media_type="application/octet-stream",
    ):
        token(name, "build name")
        token(run_id, "run ID")
        if not isinstance(revision, str) or not REVISION.fullmatch(revision):
            raise CatalogError(
                "build revision must be a full 40- or 64-character lowercase source hash"
            )
        if not isinstance(media_type, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,63}/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,63}",
            media_type,
        ):
            raise CatalogError("build media_type must be a bounded MIME type/subtype")
        payload = bounded_file(path)
        if not payload:
            raise CatalogError("build artifact must not be empty")
        with self._tx() as db:
            self._namespace(db, workspace)
            parents = self._parents(db, workspace, parents)
            request = {
                "name": name,
                "blob_sha256": hashlib.sha256(payload).hexdigest(),
                "run_id": run_id,
                "revision": revision,
                "parents": parents,
                "media_type": media_type,
            }
            previous, request_hash = self._request(
                db, workspace, request_key, "record_build", request
            )
            if previous is not None:
                return previous
            identity = self._insert_artifact(
                db,
                workspace,
                name,
                "build",
                payload,
                run_id,
                parents,
                {"media_type": media_type},
                revision=revision,
            )
            return self._remember(
                db,
                workspace,
                request_key,
                "record_build",
                request_hash,
                self._artifact(db, workspace, identity),
            )

    def _verify_artifact(self, db, workspace, identity):
        artifact = self._artifact(db, workspace, identity)
        payload = self.objects.get(artifact["sha256"])
        if len(payload) != artifact["bytes"]:
            raise CatalogError("artifact byte count differs from stored content")
        result = {
            "artifact": identity,
            "kind": artifact["kind"],
            "sha256": artifact["sha256"],
            "passed": True,
        }
        if artifact["kind"] == "table":
            table, _ = table_bundle(payload)
            policy = self._policy(
                db, workspace, artifact["policy_name"], artifact["policy_version"]
            )
            result["quality_checks"] = verify_policy(table, policy["definition"])
            if (
                table.summary() != artifact["metadata"]["table"]
                or policy["sha256"] != artifact["metadata"]["policy_sha256"]
            ):
                raise CatalogError(
                    "artifact table metadata differs from verified contents or policy"
                )
        elif artifact["kind"] != "build":
            raise CatalogError("unsupported stored artifact kind")
        return result

    def verify_artifact(self, workspace, identity):
        with self._tx(False) as db:
            self._namespace(db, workspace)
            return self._verify_artifact(db, workspace, identity)

    def artifact(self, workspace, identity):
        with self._tx(False) as db:
            self._namespace(db, workspace)
            return self._artifact(db, workspace, identity)

    def dataset_version(self, workspace, name, version):
        token(name, "dataset name")
        positive_version(version)
        with self._tx(False) as db:
            self._namespace(db, workspace)
            row = db.execute(
                "SELECT artifact_id FROM dataset_versions WHERE workspace=? AND name=? AND version=?",
                (workspace, name, version),
            ).fetchone()
            if row is None:
                raise CatalogError("unknown dataset version")
            return self._artifact(db, workspace, row["artifact_id"])

    def list_artifacts(self, workspace, after=0, limit=100):
        if (
            type(after) is not int
            or after < 0
            or type(limit) is not int
            or not 1 <= limit <= 1000
        ):
            raise CatalogError(
                "artifact cursor must be nonnegative and limit between 1 and 1000"
            )
        with self._tx(False) as db:
            self._namespace(db, workspace)
            rows = db.execute(
                "SELECT id FROM artifacts WHERE workspace=? AND seq>? ORDER BY seq LIMIT ?",
                (workspace, after, limit),
            ).fetchall()
            result = [self._artifact(db, workspace, row["id"]) for row in rows]
            return {
                "artifacts": result,
                "next_cursor": result[-1]["seq"] if result else after,
            }

    def events(self, workspace, after=0, limit=100):
        if (
            type(after) is not int
            or after < 0
            or type(limit) is not int
            or not 1 <= limit <= 1000
        ):
            raise CatalogError(
                "event cursor must be nonnegative and limit between 1 and 1000"
            )
        with self._tx(False) as db:
            self._namespace(db, workspace)
            return [
                {**dict(row), "payload": json.loads(row["payload"])}
                for row in db.execute(
                    "SELECT * FROM catalog_events WHERE workspace=? AND seq>? ORDER BY seq LIMIT ?",
                    (workspace, after, limit),
                )
            ]

    def lineage(
        self,
        workspace,
        identity,
        *,
        direction="ancestors",
        max_nodes=1000,
        max_depth=100,
    ):
        if (
            direction not in ("ancestors", "descendants")
            or type(max_nodes) is not int
            or not 1 <= max_nodes <= 1000
            or type(max_depth) is not int
            or not 0 <= max_depth <= 100
        ):
            raise CatalogError("invalid lineage direction or traversal budget")
        with self._tx(False) as db:
            self._namespace(db, workspace)
            self._artifact(db, workspace, identity)
            seen, frontier, edges, complete = {identity}, [(identity, 0)], set(), True
            while frontier:
                current, depth = frontier.pop(0)
                column, target = (
                    ("child", "parent")
                    if direction == "ancestors"
                    else ("parent", "child")
                )
                rows = db.execute(
                    f"SELECT child,parent FROM parents WHERE workspace=? AND {column}=? ORDER BY child,parent",
                    (workspace, current),
                ).fetchall()
                for row in rows:
                    other = row[target]
                    if (
                        depth >= max_depth
                        or other not in seen
                        and len(seen) >= max_nodes
                    ):
                        complete = False
                        continue
                    edges.add((row["child"], row["parent"]))
                    if other not in seen:
                        seen.add(other)
                        frontier.append((other, depth + 1))
            return {
                "root": identity,
                "direction": direction,
                "complete": complete,
                "nodes": [self._artifact(db, workspace, item) for item in sorted(seen)],
                "edges": [
                    {"child": child, "parent": parent}
                    for child, parent in sorted(edges)
                ],
            }

