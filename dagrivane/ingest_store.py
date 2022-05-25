"""Durable incremental checkpoints and an ordered transactional publication outbox."""

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .catalog_contracts import fingerprint, json_bytes, parse_json, token
from .ingest_model import (
    MAX_BATCH_BYTES,
    batch_definition,
    reduce_events,
    stream_definition,
)

SCHEMA = """
CREATE TABLE metadata(singleton INTEGER PRIMARY KEY,store_id TEXT NOT NULL);
CREATE TABLE streams(name TEXT PRIMARY KEY,definition TEXT NOT NULL,definition_sha256 TEXT NOT NULL,
 generation INTEGER NOT NULL,offset INTEGER NOT NULL,watermark TEXT,state_sha256 TEXT NOT NULL,
 seen_count INTEGER NOT NULL,target_root TEXT);
CREATE TABLE batches(stream TEXT NOT NULL REFERENCES streams(name),id TEXT NOT NULL,generation INTEGER NOT NULL,
 raw BLOB NOT NULL,raw_sha256 TEXT NOT NULL,receipt TEXT NOT NULL,bundle BLOB NOT NULL,bundle_sha256 TEXT NOT NULL,
 PRIMARY KEY(stream,id),UNIQUE(stream,generation));
CREATE TABLE requests(stream TEXT NOT NULL,request_key TEXT NOT NULL,batch_id TEXT NOT NULL,
 PRIMARY KEY(stream,request_key),FOREIGN KEY(stream,batch_id) REFERENCES batches(stream,id));
CREATE TABLE records(stream TEXT NOT NULL REFERENCES streams(name),key TEXT NOT NULL,payload TEXT NOT NULL,
 PRIMARY KEY(stream,key));
CREATE TABLE events(stream TEXT NOT NULL REFERENCES streams(name),event_id TEXT NOT NULL,content_sha256 TEXT NOT NULL,
 batch_id TEXT NOT NULL,PRIMARY KEY(stream,event_id),FOREIGN KEY(stream,batch_id) REFERENCES batches(stream,id));
CREATE TABLE publications(stream TEXT NOT NULL,batch_id TEXT NOT NULL,status TEXT NOT NULL,
 claim TEXT,lease_until TEXT,attempts INTEGER NOT NULL,last_error TEXT,artifact TEXT,
 PRIMARY KEY(stream,batch_id),FOREIGN KEY(stream,batch_id) REFERENCES batches(stream,id));
"""


def utc():
    return datetime.now(timezone.utc)


class IngestStore:
    def __init__(self, path, *, create=False):
        self.path = Path(path).expanduser()
        if self.path.is_symlink():
            raise ValueError("ingestion database cannot be a symlink")
        if not create and not self.path.is_file():
            raise ValueError(
                "ingestion database does not exist; initialize it explicitly"
            )
        if create:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._connect() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in {0, 1} or version == 0 and not create:
                raise ValueError("unsupported ingestion schema")
            if version == 0:
                db.execute("BEGIN IMMEDIATE")
                try:
                    version = db.execute("PRAGMA user_version").fetchone()[0]
                    if version not in {0, 1}:
                        raise ValueError("unsupported ingestion schema")
                    if version == 0:
                        if db.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                        ).fetchone():
                            raise ValueError("refusing an unrelated SQLite database")
                        for statement in SCHEMA.split(";"):
                            if statement.strip():
                                db.execute(statement)
                        db.execute(
                            "INSERT INTO metadata VALUES(1,?)", (uuid.uuid4().hex,)
                        )
                        db.execute("PRAGMA user_version=1")
                        self.path.chmod(0o600)
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
            deadline = time.monotonic() + 10
            while True:
                try:
                    db.execute("PRAGMA journal_mode=WAL").fetchone()
                    break
                except sqlite3.OperationalError as error:
                    if (
                        getattr(error, "sqlite_errorcode", None)
                        not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
                        or time.monotonic() >= deadline
                    ):
                        raise
                    time.sleep(0.02)
            self.id = db.execute(
                "SELECT store_id FROM metadata WHERE singleton=1"
            ).fetchone()[0]

    @contextmanager
    def _connect(self):
        with closing(
            sqlite3.connect(self.path, timeout=10, isolation_level=None)
        ) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            yield db

    @contextmanager
    def _tx(self, write=True):
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _stream(self, db, name):
        token(name, "stream")
        row = db.execute("SELECT * FROM streams WHERE name=?", (name,)).fetchone()
        if row is None:
            raise ValueError("unknown ingestion stream")
        definition = stream_definition(json.loads(row["definition"]))
        if fingerprint(definition) != row["definition_sha256"]:
            raise ValueError("stored stream definition fingerprint mismatch")
        return dict(row), definition

    def register(self, name, definition):
        token(name, "stream")
        definition = stream_definition(definition)
        sha = fingerprint(definition)
        with self._tx() as db:
            row = db.execute(
                "SELECT definition_sha256 FROM streams WHERE name=?", (name,)
            ).fetchone()
            if row:
                if row["definition_sha256"] != sha:
                    raise ValueError(
                        "stream definition is immutable; register a new stream for changed semantics"
                    )
            else:
                if db.execute("SELECT count(*) FROM streams").fetchone()[0] >= 1000:
                    raise ValueError("ingestion store exceeds 1,000 streams")
                db.execute(
                    "INSERT INTO streams VALUES(?,?,?,0,0,NULL,?,0,NULL)",
                    (name, json_bytes(definition).decode(), sha, fingerprint([])),
                )
            return self._status(db, name)

    def _status(self, db, name):
        stream, definition = self._stream(db, name)
        return {
            "name": name,
            "store_id": self.id,
            "definition": definition,
            "definition_sha256": stream["definition_sha256"],
            "generation": stream["generation"],
            "offset": stream["offset"],
            "watermark": stream["watermark"],
            "state_sha256": stream["state_sha256"],
            "seen_events": stream["seen_count"],
            "target_root": stream["target_root"],
            "publication_counts": {
                row["status"]: row["n"]
                for row in db.execute(
                    "SELECT status,count(*) AS n FROM publications WHERE stream=? GROUP BY status",
                    (name,),
                )
            },
        }

    def status(self, name):
        with self._tx(False) as db:
            return self._status(db, name)

