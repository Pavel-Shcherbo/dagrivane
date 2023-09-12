"""Fenced HTTP page journals sharing ingestion's atomic checkpoint transaction."""

import hashlib
import math
import time
import uuid

from .catalog_contracts import fingerprint, json_bytes, parse_json, token
from .contracts import Contract
from .http_client import HTTPPage
from .http_contract import adapt_page, definition
from .quality import compile_gates
from .tables import Table

SCHEMA = [
    "CREATE TABLE IF NOT EXISTS http_meta(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL)",
    """CREATE TABLE IF NOT EXISTS http_sources(name TEXT PRIMARY KEY,stream TEXT UNIQUE NOT NULL REFERENCES streams(name),
       configuration TEXT NOT NULL,configuration_sha TEXT NOT NULL,stream_sha TEXT NOT NULL,
       initial_offset INTEGER NOT NULL,initial_generation INTEGER NOT NULL,initial_cursor TEXT NOT NULL,
       offset INTEGER NOT NULL,generation INTEGER NOT NULL,cursor TEXT NOT NULL,round INTEGER NOT NULL,
       chain TEXT NOT NULL,pending TEXT,claim TEXT,owner TEXT,lease_until REAL,not_before REAL,
       last_error TEXT,pages INTEGER NOT NULL,journal_bytes INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS http_pages(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,
       source TEXT NOT NULL REFERENCES http_sources(name),offset INTEGER NOT NULL,generation INTEGER NOT NULL,
       round INTEGER NOT NULL,cursor TEXT NOT NULL,previous_chain TEXT NOT NULL,raw BLOB NOT NULL,
       raw_sha TEXT NOT NULL,metadata TEXT NOT NULL,state TEXT NOT NULL,receipt TEXT,error TEXT,
       reason TEXT,received_at REAL NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS http_page_history ON http_pages(source,seq)",
]


class SourceLeaseLost(ValueError):
    pass


def moment(value=None):
    value = time.time() if value is None else value
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("HTTP lease time must be finite and nonnegative")
    return value


class HTTPSourceStore:
    def __init__(self, ingestion):
        self.ingestion = ingestion

    def _source(self, db, name):
        token(name, "HTTP source")
        if (
            db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='http_meta'"
            ).fetchone()
            is None
        ):
            raise ValueError(
                "HTTP sources are not initialized; register one explicitly"
            )
        version = db.execute(
            "SELECT version FROM http_meta WHERE singleton=1"
        ).fetchone()
        if version is None or version[0] != 1:
            raise ValueError("unsupported HTTP journal schema")
        row = db.execute("SELECT * FROM http_sources WHERE name=?", (name,)).fetchone()
        if row is None:
            raise ValueError("unknown HTTP source")
        stream, stream_def = self.ingestion._stream(db, row["stream"])
        config = definition(parse_json(row["configuration"].encode()), stream_def)
        if (
            fingerprint(config) != row["configuration_sha"]
            or stream["definition_sha256"] != row["stream_sha"]
        ):
            raise ValueError("HTTP source or stream definition identity mismatch")
        return dict(row), config, stream, stream_def

    def register(self, name, stream_name, config):
        token(name, "HTTP source")
        token(stream_name, "stream")
        status = self.ingestion.status(stream_name)
        config = definition(config, status["definition"])
        encoded = json_bytes(config).decode()
        sha = fingerprint(config)
        with self.ingestion._tx() as db:
            for statement in SCHEMA:
                db.execute(statement)
            db.execute("INSERT OR IGNORE INTO http_meta VALUES(1,1)")
            version = db.execute(
                "SELECT version FROM http_meta WHERE singleton=1"
            ).fetchone()[0]
            if version != 1:
                raise ValueError("unsupported HTTP journal schema")
            existing = db.execute(
                "SELECT * FROM http_sources WHERE name=?", (name,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["configuration_sha"] != sha
                    or existing["stream"] != stream_name
                ):
                    raise ValueError("HTTP source configuration is immutable")
                return self._public(db, name)
            if db.execute("SELECT count(*) FROM http_sources").fetchone()[0] >= 1000:
                raise ValueError("store exceeds 1,000 HTTP sources")
            if db.execute(
                "SELECT name FROM http_sources WHERE stream=?", (stream_name,)
            ).fetchone():
                raise ValueError("stream already belongs to another HTTP source")
            current = self.ingestion._status(db, stream_name)
            cursor = json_bytes(config["initial_cursor"]).decode()
            db.execute(
                "INSERT INTO http_sources VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,0,0)",
                (
                    name,
                    stream_name,
                    encoded,
                    sha,
                    current["definition_sha256"],
                    current["offset"],
                    current["generation"],
                    cursor,
                    current["offset"],
                    current["generation"],
                    cursor,
                    0,
                    fingerprint([]),
                ),
            )
            return self._public(db, name)

    def _public(self, db, name):
        source, config, stream, _ = self._source(db, name)
        pending = None
        if source["pending"]:
            row = db.execute(
                "SELECT id,seq,state,error,received_at FROM http_pages WHERE source=? AND id=?",
                (name, source["pending"]),
            ).fetchone()
            if row is None:
                raise ValueError("HTTP pending page is missing")
            pending = dict(row)
        return {
            "name": name,
            "stream": source["stream"],
            "configuration_sha256": source["configuration_sha"],
            "stream_definition_sha256": source["stream_sha"],
            "offset": source["offset"],
            "generation": source["generation"],
            "stream_synchronized": stream["offset"] == source["offset"]
            and stream["generation"] == source["generation"],
            "cursor_sha256": fingerprint(parse_json(source["cursor"].encode())),
            "round": source["round"],
            "chain_sha256": source["chain"],
            "pending": pending,
            "lease_active": source["claim"] is not None
            and source["lease_until"] > time.time(),
            "not_before": source["not_before"],
            "last_error": source["last_error"],
            "retained_pages": source["pages"],
            "journal_bytes": source["journal_bytes"],
            "mode": config["response"]["mode"],
        }

    def status(self, name):
        with self.ingestion._tx(False) as db:
            return self._public(db, name)

    def claim(self, name, owner, *, lease_seconds=30, now=None):
        token(owner, "HTTP worker")
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ValueError("HTTP lease must be 1–300 seconds")
        now = moment(now)
        with self.ingestion._tx() as db:
            source, _, stream, _ = self._source(db, name)
            if (
                stream["offset"] != source["offset"]
                or stream["generation"] != source["generation"]
            ):
                raise ValueError("stream was advanced outside its HTTP connector")
            if (
                source["claim"] is not None
                and source["lease_until"] > now
                or source["not_before"] is not None
                and source["not_before"] > now
            ):
                return None
            if source["pending"]:
                page = db.execute(
                    "SELECT state FROM http_pages WHERE id=? AND source=?",
                    (source["pending"], name),
                ).fetchone()
                if page is None:
                    raise ValueError("HTTP pending page is missing")
                if page["state"] == "blocked":
                    return None
                if page["state"] != "fetched":
                    raise ValueError("HTTP pending page has an invalid state")
            claim = uuid.uuid4().hex
            db.execute(
                "UPDATE http_sources SET claim=?,owner=?,lease_until=?,not_before=NULL WHERE name=?",
                (claim, owner, now + lease_seconds, name),
            )
            return {
                "name": name,
                "token": claim,
                "owner": owner,
                "lease_seconds": lease_seconds,
            }

    def _held(self, db, claim, now=None):
        source, config, stream, stream_def = self._source(db, claim["name"])
        if (
            source["claim"] != claim["token"]
            or source["owner"] != claim["owner"]
            or source["lease_until"] <= moment(now)
        ):
            raise SourceLeaseLost("HTTP source lease expired or was replaced")
        if (
            stream["offset"] != source["offset"]
            or stream["generation"] != source["generation"]
        ):
            raise ValueError("stream was advanced outside its HTTP connector")
        return source, config, stream, stream_def

    def heartbeat(self, claim, *, now=None):
        now = moment(now)
        with self.ingestion._tx() as db:
            self._held(db, claim, now)
            db.execute(
                "UPDATE http_sources SET lease_until=? WHERE name=?",
                (now + claim["lease_seconds"], claim["name"]),
            )

    def checkpoint(self, claim):
        with self.ingestion._tx(False) as db:
            source, config, _, stream_def = self._held(db, claim)
            return {
                "offset": source["offset"],
                "generation": source["generation"],
                "cursor": parse_json(source["cursor"].encode()),
                "round": source["round"],
                "chain": source["chain"],
                "pending": source["pending"],
                "configuration": config,
                "stream_definition": stream_def,
            }

    def stage(self, claim, checkpoint, page):
        if not isinstance(page, HTTPPage) or not isinstance(page.raw, bytes):
            raise ValueError("HTTP stage requires received page bytes")
        raw_sha = hashlib.sha256(page.raw).hexdigest()
        if (
            page.metadata.get("entity_sha256") != raw_sha
            or page.metadata.get("entity_bytes") != len(page.raw)
            or page.metadata.get("status") not in (200, 204)
        ):
            raise ValueError("HTTP response metadata differs from entity bytes")
        metadata = json_bytes(page.metadata, 8192).decode()
        with self.ingestion._tx() as db:
            source, config, _, _ = self._held(db, claim)
            for key in ("offset", "generation", "round", "chain"):
                if checkpoint[key] != source[key]:
                    raise ValueError("HTTP fetch checkpoint changed before staging")
            if json_bytes(checkpoint["cursor"]).decode() != source["cursor"]:
                raise ValueError("HTTP cursor changed before staging")
            page_id = fingerprint(
                [
                    self.ingestion.id,
                    claim["name"],
                    source["round"],
                    source["offset"],
                    source["generation"],
                    source["cursor"],
                    source["chain"],
                    raw_sha,
                    fingerprint(page.metadata),
                ]
            )
            if source["pending"]:
                if source["pending"] == page_id:
                    return page_id
                raise ValueError(
                    "another HTTP entity is already pending at this checkpoint"
                )
            if (
                len(page.raw) > config["limits"]["page_bytes"]
                or source["journal_bytes"] + len(page.raw)
                > config["limits"]["journal_bytes"]
                or source["pages"] >= 10000
            ):
                raise ValueError("HTTP source page or retained journal quota exceeded")
            db.execute(
                "INSERT INTO http_pages(id,source,offset,generation,round,cursor,previous_chain,raw,raw_sha,metadata,state,received_at) VALUES(?,?,?,?,?,?,?,?,?,?,'fetched',?)",
                (
                    page_id,
                    claim["name"],
                    source["offset"],
                    source["generation"],
                    source["round"],
                    source["cursor"],
                    source["chain"],
                    page.raw,
                    raw_sha,
                    metadata,
                    time.time(),
                ),
            )
            db.execute(
                "UPDATE http_sources SET pending=?,pages=pages+1,journal_bytes=journal_bytes+? WHERE name=?",
                (page_id, len(page.raw), claim["name"]),
            )
            return page_id

    def _page(self, db, name, page_id):
        token(page_id, "HTTP page")
        row = db.execute(
            "SELECT * FROM http_pages WHERE source=? AND id=?", (name, page_id)
        ).fetchone()
        if row is None:
            raise ValueError("unknown HTTP page in this source")
        result = dict(row)
        result["raw"] = bytes(row["raw"])
        if hashlib.sha256(result["raw"]).hexdigest() != row["raw_sha"]:
            raise ValueError("HTTP raw page identity mismatch")
        expected = fingerprint(
            [
                self.ingestion.id,
                name,
                row["round"],
                row["offset"],
                row["generation"],
                row["cursor"],
                row["previous_chain"],
                row["raw_sha"],
                fingerprint(parse_json(row["metadata"].encode())),
            ]
        )
        if expected != page_id:
            raise ValueError("HTTP page checkpoint identity mismatch")
        return result

