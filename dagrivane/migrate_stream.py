"""Prepare, explicitly configure and apply a replayable stream-schema successor."""

import argparse
import json
from pathlib import Path
import sqlite3

from .catalog import Catalog
from .catalog_contracts import parse_json
from .ingest_store import IngestStore
from .stream_migration import (
    prepare_migration,
    configure_migration_policy,
    apply_migration,
    write_migration,
)
from .stream_migration_seed import verify_base


def read(path):
    with Path(path).open("rb") as stream:
        return parse_json(stream.read(64 * 1024 * 1024 + 1))


def main():
    parser = argparse.ArgumentParser(
        description="Fork a verified stream checkpoint into a new immutable schema version"
    )
    parser.add_argument(
        "--database", type=Path, default=Path(".local/ingestion.sqlite")
    )
    actions = parser.add_subparsers(dest="action", required=True)
    prepare = actions.add_parser("prepare")
    prepare.add_argument("proposal", type=Path)
    prepare.add_argument("--catalog", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    for action in ("policy", "apply"):
        sub = actions.add_parser(action)
        sub.add_argument("plan", type=Path)
        sub.add_argument("--catalog", type=Path, required=True)
        if action == "apply":
            sub.add_argument("--request-key", required=True)
    verify = actions.add_parser("verify")
    verify.add_argument("plan", type=Path)
    args = parser.parse_args()
    try:
        if args.action == "verify":
            result = verify_base(read(args.plan))
        else:
            if not (args.catalog / "catalog.sqlite").is_file():
                raise ValueError(
                    "catalog must already exist; configure the source publication first"
                )
            catalog = Catalog(args.catalog)
            if args.action == "policy":
                result = configure_migration_policy(read(args.plan), catalog)
            else:
                store = IngestStore(args.database)
                if args.action == "prepare":
                    result = write_migration(
                        prepare_migration(store, read(args.proposal), catalog=catalog),
                        args.output,
                    )
                else:
                    result = apply_migration(
                        store,
                        read(args.plan),
                        catalog=catalog,
                        request_key=args.request_key,
                    )
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0
    except (
        ValueError,
        TypeError,
        KeyError,
        OSError,
        sqlite3.Error,
        RecursionError,
    ) as error:
        parser.exit(2, str(error) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
