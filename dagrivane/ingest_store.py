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

    def _records(self, db, name):
        records = {}
        size = 0
        for row in db.execute(
            "SELECT key,payload FROM records WHERE stream=? ORDER BY key", (name,)
        ):
            size += len(row["key"].encode()) + len(row["payload"].encode())
            if len(records) >= 100000 or size > 32 * 1024 * 1024:
                raise ValueError("stored ingestion state exceeds key/byte budget")
            records[row["key"]] = parse_json(row["payload"].encode())
        return records

    def ingest(self, name, raw, *, request_key):
        token(request_key, "request key")
        if not isinstance(raw, bytes) or len(raw) > MAX_BATCH_BYTES:
            raise ValueError("batch exceeds 4 MiB or is not bytes")
        raw_sha = hashlib.sha256(raw).hexdigest()
        with self._tx() as db:
            stream, definition = self._stream(db, name)
            batch = batch_definition(raw, definition)
            batch_id = fingerprint(
                [self.id, name, stream["definition_sha256"], raw_sha]
            )
            prior = db.execute(
                "SELECT batch_id FROM requests WHERE stream=? AND request_key=?",
                (name, request_key),
            ).fetchone()
            if prior and prior["batch_id"] != batch_id:
                raise ValueError("request key conflicts with different batch bytes")
            existing = db.execute(
                "SELECT receipt FROM batches WHERE stream=? AND id=?", (name, batch_id)
            ).fetchone()
            if existing:
                db.execute(
                    "INSERT OR IGNORE INTO requests VALUES(?,?,?)",
                    (name, request_key, batch_id),
                )
                return {**json.loads(existing["receipt"]), "reused": True}
            if batch["after"] != stream["offset"]:
                raise ValueError(
                    "batch after offset differs from the committed checkpoint"
                )
            records = self._records(db, name)
            actual = fingerprint(
                [{"key": key, **records[key]} for key in sorted(records)]
            )
            if actual != stream["state_sha256"]:
                raise ValueError("materialized stream state fingerprint mismatch")
            ids = [event["id"] for event in batch["events"]]
            seen = {}
            for start in range(0, len(ids), 200):
                chunk = ids[start : start + 200]
                for row in db.execute(
                    "SELECT event_id,content_sha256 FROM events WHERE stream=? AND event_id IN ("
                    + ",".join("?" for _ in chunk)
                    + ")",
                    [name, *chunk],
                ):
                    seen[row["event_id"]] = row["content_sha256"]
            updated, new_seen, watermark, counts, outcomes, table, checks, state_sha = (
                reduce_events(
                    definition, records, seen, stream["watermark"], batch["events"]
                )
            )
            new_events = {
                key: value for key, value in new_seen.items() if key not in seen
            }
            if stream["seen_count"] + len(new_events) > 1000000:
                raise ValueError("stream exceeds one million retained event IDs")
            generation = stream["generation"] + 1
            previous = db.execute(
                "SELECT bundle_sha256 FROM batches WHERE stream=? AND generation=?",
                (name, generation - 1),
            ).fetchone()
            sources = list(
                dict.fromkeys(
                    [
                        raw_sha,
                        *batch["source_sha256"],
                        *([previous["bundle_sha256"]] if previous else []),
                    ]
                )
            )
            from .tables import Table

            table = Table(table.contract, table.records, tuple(sources))
            bundle = json_bytes(
                {
                    "format": "dagrivane.table.v1",
                    "report": {
                        "passed": True,
                        "plan_sha256": stream["definition_sha256"],
                        "output": table.summary(),
                        "checks": checks,
                    },
                    "rows": table.json_rows(),
                }
            )
            bundle_sha = hashlib.sha256(bundle).hexdigest()
            receipt = {
                "stream": name,
                "batch_id": batch_id,
                "generation": generation,
                "after": batch["after"],
                "through": batch["through"],
                "raw_sha256": raw_sha,
                "previous_state_sha256": stream["state_sha256"],
                "state_sha256": state_sha,
                "bundle_sha256": bundle_sha,
                "data_sha256": table.digest(),
                "watermark": watermark,
                "counts": counts,
                "outcomes": outcomes,
                "rows": len(table.records),
                "tombstones": sum(item["row"] is None for item in updated.values()),
                "committed_at": utc().isoformat(),
            }
            json_bytes(updated, 32 * 1024 * 1024)
            db.execute(
                "INSERT INTO batches VALUES(?,?,?,?,?,?,?,?)",
                (
                    name,
                    batch_id,
                    generation,
                    raw,
                    raw_sha,
                    json_bytes(receipt).decode(),
                    bundle,
                    bundle_sha,
                ),
            )
            db.execute(
                "INSERT INTO requests VALUES(?,?,?)", (name, request_key, batch_id)
            )
            db.execute(
                "INSERT INTO publications VALUES(?,?,'pending',NULL,NULL,0,NULL,NULL)",
                (name, batch_id),
            )
            db.executemany(
                "INSERT INTO events VALUES(?,?,?,?)",
                [
                    (name, event_id, sha, batch_id)
                    for event_id, sha in new_events.items()
                ],
            )
            for key, item in updated.items():
                if records.get(key) != item:
                    db.execute(
                        "INSERT INTO records VALUES(?,?,?) ON CONFLICT(stream,key) DO UPDATE SET payload=excluded.payload",
                        (name, key, json_bytes(item).decode()),
                    )
            db.execute(
                "UPDATE streams SET generation=?,offset=?,watermark=?,state_sha256=?,seen_count=seen_count+? WHERE name=?",
                (
                    generation,
                    batch["through"],
                    watermark,
                    state_sha,
                    len(new_events),
                    name,
                ),
            )
            return {**receipt, "reused": False}

    def ingest_file(self, name, path, *, request_key):
        with Path(path).open("rb") as stream:
            raw = stream.read(MAX_BATCH_BYTES + 1)
        return self.ingest(name, raw, request_key=request_key)

    def rows(self, name):
        with self._tx(False) as db:
            stream, definition = self._stream(db, name)
            records = self._records(db, name)
            actual = fingerprint(
                [{"key": key, **records[key]} for key in sorted(records)]
            )
            if actual != stream["state_sha256"]:
                raise ValueError("materialized stream state fingerprint mismatch")
            return {
                "stream": name,
                "generation": stream["generation"],
                "offset": stream["offset"],
                "state_sha256": actual,
                "rows": [
                    records[key]["row"]
                    for key in sorted(records)
                    if records[key]["row"] is not None
                ],
                "tombstones": sum(item["row"] is None for item in records.values()),
            }

    def batches(self, name, *, after_generation=0, limit=100):
        if (
            type(after_generation) is not int
            or after_generation < 0
            or type(limit) is not int
            or not 1 <= limit <= 1000
        ):
            raise ValueError("invalid generation cursor or page limit")
        with self._tx(False) as db:
            self._stream(db, name)
            cursor = db.execute(
                "SELECT b.receipt,p.status,p.attempts,p.last_error,p.artifact FROM batches b JOIN publications p ON p.stream=b.stream AND p.batch_id=b.id WHERE b.stream=? AND b.generation>? ORDER BY b.generation LIMIT ?",
                (name, after_generation, limit + 1),
            )
            items = []
            size = 0
            next_generation = None
            for row in cursor:
                receipt = json.loads(row["receipt"])
                receipt.pop("outcomes", None)
                item = {
                    **receipt,
                    "publication": {
                        "status": row["status"],
                        "attempts": row["attempts"],
                        "last_error": row["last_error"],
                        "artifact": (
                            json.loads(row["artifact"]) if row["artifact"] else None
                        ),
                    },
                }
                encoded_size = len(json_bytes(item))
                if len(items) >= limit or size + encoded_size > 4 * 1024 * 1024:
                    next_generation = (
                        items[-1]["generation"] if items else after_generation
                    )
                    break
                items.append(item)
                size += encoded_size
            return {
                "items": items,
                "next_generation": next_generation,
                "event_outcomes_omitted": True,
            }

    def claim(self, name, target_root, *, lease_seconds=30):
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ValueError("publication lease must be 1–300 seconds")
        target_root = str(Path(target_root).resolve())
        moment = utc()
        with self._tx() as db:
            stream, definition = self._stream(db, name)
            if stream["target_root"] not in (None, target_root):
                raise ValueError(
                    "stream publication destination is already bound to another catalog path"
                )
            row = db.execute(
                "SELECT b.*,p.status,p.claim,p.lease_until FROM batches b JOIN publications p ON p.stream=b.stream AND p.batch_id=b.id WHERE b.stream=? AND p.status!='published' ORDER BY b.generation LIMIT 1",
                (name,),
            ).fetchone()
            if row is None:
                return None
            if (
                row["status"] == "leased"
                and datetime.fromisoformat(row["lease_until"]) > moment
            ):
                return None
            if hashlib.sha256(bytes(row["bundle"])).hexdigest() != row["bundle_sha256"]:
                raise ValueError("stored outbox bundle SHA-256 mismatch")
            claim = uuid.uuid4().hex
            until = (moment + timedelta(seconds=lease_seconds)).isoformat()
            previous = db.execute(
                "SELECT p.artifact FROM batches b JOIN publications p ON p.stream=b.stream AND p.batch_id=b.id WHERE b.stream=? AND b.generation=?",
                (name, row["generation"] - 1),
            ).fetchone()
            if previous and previous["artifact"] is None:
                raise ValueError("prior publication has not been acknowledged")
            db.execute(
                "UPDATE publications SET status='leased',claim=?,lease_until=?,attempts=attempts+1,last_error=NULL WHERE stream=? AND batch_id=?",
                (claim, until, name, row["id"]),
            )
            db.execute(
                "UPDATE streams SET target_root=? WHERE name=?", (target_root, name)
            )
            return {
                "stream": name,
                "batch_id": row["id"],
                "generation": row["generation"],
                "claim": claim,
                "lease_until": until,
                "definition": definition,
                "bundle": bytes(row["bundle"]),
                "bundle_sha256": row["bundle_sha256"],
                "parents": [json.loads(previous["artifact"])["id"]] if previous else [],
                "request_key": "ingest:"
                + self.id
                + ":"
                + fingerprint(name)[:12]
                + ":"
                + str(row["generation"]),
            }

