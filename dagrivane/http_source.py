"""Register, poll and review explicit HTTP feeds into typed incremental streams."""

import argparse
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from .catalog_contracts import parse_json
from .http_audit import audit_source
from .http_runner import poll_source
from .http_store import HTTPSourceStore
from .ingest_store import IngestStore


def write_raw(raw, destination):
    destination = Path(destination)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=".http-entity-", dir=destination.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint explicit HTTP pages before typed ingestion; retain rejected entities"
    )
    parser.add_argument(
        "--database", type=Path, default=Path(".local/ingestion.sqlite")
    )
    sub = parser.add_subparsers(dest="action", required=True)
    register = sub.add_parser("register")
    register.add_argument("source")
    register.add_argument("stream")
    register.add_argument("configuration", type=Path)
    status = sub.add_parser("status")
    status.add_argument("source")
    poll = sub.add_parser("poll")
    poll.add_argument("source")
    poll.add_argument("--max-pages", type=int, default=25)
    poll.add_argument("--lease-seconds", type=int, default=30)
    poll.add_argument("--deadline-seconds", type=float, default=60)
    pages = sub.add_parser("pages")
    pages.add_argument("source")
    pages.add_argument("--after", type=int, default=0)
    pages.add_argument("--limit", type=int, default=25)
    for name in ("page", "export-page", "discard"):
        command = sub.add_parser(name)
        command.add_argument("source")
        command.add_argument("page")
        if name == "export-page":
            command.add_argument("--output", type=Path, required=True)
        if name == "discard":
            command.add_argument("--reason", required=True)
    audit = sub.add_parser("audit")
    audit.add_argument("source")
    audit.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()
    try:
        store = HTTPSourceStore(IngestStore(args.database))
        if args.action == "register":
            with args.configuration.open("rb") as stream:
                raw = stream.read(65537)
            if len(raw) > 65536:
                raise ValueError("HTTP source configuration exceeds 64 KiB")
            result = store.register(args.source, args.stream, parse_json(raw))
        elif args.action == "status":
            result = store.status(args.source)
        elif args.action == "poll":
            result = poll_source(
                store,
                args.source,
                max_pages=args.max_pages,
                lease_seconds=args.lease_seconds,
                deadline_seconds=args.deadline_seconds,
            )
        elif args.action == "pages":
            result = store.pages(args.source, after=args.after, limit=args.limit)
        elif args.action == "discard":
            result = store.discard(args.source, args.page, args.reason)
        elif args.action == "audit":
            result = audit_source(store, args.source, limit=args.limit)
        else:
            result = store.page(
                args.source, args.page, include_raw=args.action == "export-page"
            )
            if args.action == "export-page":
                raw = result.pop("raw")
                write_raw(raw, args.output)
                result["exported_path"] = str(args.output)
        print(json.dumps(result, indent=2, allow_nan=False))
        if args.action == "poll" and not result["drained"]:
            raise SystemExit(
                3
                if result["status"]
                in ("busy", "cooldown", "deferred", "limited", "deadline")
                else 130 if result["status"] == "cancelled" else 2
            )
        if args.action == "audit" and not result["passed"]:
            raise SystemExit(1)
    except KeyboardInterrupt:
        parser.exit(130, "HTTP source polling cancelled\n")
    except (
        ValueError,
        TypeError,
        KeyError,
        OSError,
        OverflowError,
        sqlite3.Error,
    ) as error:
        parser.exit(2, str(error) + "\n")


if __name__ == "__main__":
    main()
