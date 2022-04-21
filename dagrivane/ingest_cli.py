"""Operate local incremental streams and recover their artifact publications."""

import argparse
import json
import sqlite3
from pathlib import Path

from .catalog import Catalog
from .catalog_contracts import parse_json
from .ingest_audit import audit
from .ingest_publish import configure_target, publish_pending
from .ingest_source import csv_batch, write_batch
from .ingest_store import IngestStore


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint typed local event batches and publish verified datasets"
    )
    parser.add_argument(
        "--database", type=Path, default=Path(".local/ingestion.sqlite")
    )
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("init")
    register = commands.add_parser("register")
    register.add_argument("stream")
    register.add_argument("definition", type=Path)
    ingest = commands.add_parser("ingest")
    ingest.add_argument("stream")
    ingest.add_argument("batch", type=Path)
    ingest.add_argument("--request-key", required=True)
    for action in ["status", "rows", "batches", "audit", "configure-target", "publish"]:
        command = commands.add_parser(action)
        command.add_argument("stream")
        if action in {"configure-target", "publish"}:
            command.add_argument("--catalog", type=Path, required=True)
        if action == "publish":
            command.add_argument("--lease-seconds", type=int, default=30)
        if action == "audit":
            command.add_argument("--catalog", type=Path)
            command.add_argument("--limit", type=int, default=1000)
        if action in {"batches", "publish"}:
            command.add_argument("--limit", type=int, default=100)
        if action == "batches":
            command.add_argument("--after-generation", type=int, default=0)
    csv = commands.add_parser("csv-batch")
    csv.add_argument("stream")
    csv.add_argument("csv", type=Path)
    csv.add_argument("--after", type=int, required=True)
    csv.add_argument("--sha256")
    csv.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        store = IngestStore(args.database, create=args.action == "init")
        catalog = None
        if getattr(args, "catalog", None) is not None:
            if (
                args.action != "configure-target"
                and not (args.catalog / "catalog.sqlite").is_file()
            ):
                raise ValueError(
                    "catalog destination does not exist; configure it explicitly"
                )
            catalog = Catalog(args.catalog)
        if args.action == "init":
            result = {
                "database": str(store.path),
                "store_id": store.id,
                "initialized": True,
            }
        elif args.action == "register":
            with args.definition.open("rb") as stream:
                raw = stream.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError("stream definition exceeds 1 MiB")
            result = store.register(args.stream, parse_json(raw))
        elif args.action == "ingest":
            result = store.ingest_file(
                args.stream, args.batch, request_key=args.request_key
            )
        elif args.action == "status":
            result = store.status(args.stream)
        elif args.action == "rows":
            result = store.rows(args.stream)
        elif args.action == "batches":
            result = store.batches(
                args.stream, after_generation=args.after_generation, limit=args.limit
            )
        elif args.action == "audit":
            result = audit(store, args.stream, limit=args.limit, catalog=catalog)
        elif args.action == "configure-target":
            result = configure_target(store, args.stream, catalog)
        elif args.action == "publish":
            result = publish_pending(
                store,
                args.stream,
                catalog,
                limit=args.limit,
                lease_seconds=args.lease_seconds,
            )
        else:
            definition = store.status(args.stream)["definition"]
            result = write_batch(
                csv_batch(
                    definition, args.csv, after=args.after, expected_sha256=args.sha256
                ),
                args.output,
            )
        print(json.dumps(result, indent=2, allow_nan=False))
        return 1 if result.get("passed") is False else 0
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
    raise SystemExit(main())
