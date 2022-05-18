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

