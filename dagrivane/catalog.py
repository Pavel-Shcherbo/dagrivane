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

