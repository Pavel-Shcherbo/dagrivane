"""Register, preview, dispatch and inspect retained local workflow schedules."""

import argparse
import json
from pathlib import Path
import sqlite3
import time

from .control import ControlError, ControlStore
from .schedule_clock import instant
from .schedule_evidence import export_schedule, save_evidence, verify_schedule_evidence
from .schedule_model import preview_schedule
from .schedule_store import ScheduleStore


def load(path, maximum=16 * 1024 * 1024):
    with Path(path).open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("schedule input exceeds byte limit")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    return json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(
            ValueError("nonfinite JSON number")
        ),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Manage retained trusted-local workflow schedules"
    )
    parser.add_argument("--db", type=Path, default=Path(".local/control.sqlite"))
    commands = parser.add_subparsers(dest="action", required=True)
    preview = commands.add_parser("preview")
    preview.add_argument("definition", type=Path)
    preview.add_argument("--after")
    preview.add_argument("--limit", type=int, default=20)
    create = commands.add_parser("create")
    create.add_argument("workspace")
    create.add_argument("workflow")
    create.add_argument("--version", type=int, required=True)
    create.add_argument("--definition", type=Path, required=True)
    create.add_argument("--request-key", required=True)
    create.add_argument("--now")
    listing = commands.add_parser("list")
    listing.add_argument("workspace")
    listing.add_argument("--after", default="")
    listing.add_argument("--limit", type=int, default=100)
    dispatch = commands.add_parser("dispatch")
    dispatch.add_argument("workspace")
    dispatch.add_argument("--request-key", required=True)
    dispatch.add_argument(
        "--now",
        required=True,
        help="Explicit clock; use the same value when retrying a lost response",
    )
    dispatch.add_argument("--limit", type=int, default=100)
    for name in ("inspect", "events", "occurrences", "set-enabled", "export", "audit"):
        sub = commands.add_parser(name)
        sub.add_argument("workspace")
        sub.add_argument("schedule")
        if name in ("events", "occurrences"):
            sub.add_argument("--after", type=int, default=0 if name == "events" else -1)
            sub.add_argument("--limit", type=int, default=100)
        elif name == "set-enabled":
            sub.add_argument(
                "--enabled", action=argparse.BooleanOptionalAction, required=True
            )
            sub.add_argument("--generation", type=int, required=True)
            sub.add_argument("--reason", required=True)
            sub.add_argument("--now")
        elif name == "export":
            sub.add_argument("--output", type=Path, required=True)
    watch = commands.add_parser(
        "watch", help="Foreground automatic dispatcher; run workers separately"
    )
    watch.add_argument("workspace")
    watch.add_argument("--identity", default="local")
    watch.add_argument("--every", type=float, default=5)
    watch.add_argument("--limit", type=int, default=100)
    watch.add_argument("--max-ticks", type=int)
    verify = commands.add_parser("verify")
    verify.add_argument("file", type=Path)
    args = parser.parse_args()
    try:
        if args.action == "preview":
            result = preview_schedule(
                load(args.definition, 400000), after=args.after, limit=args.limit
            )
        elif args.action == "verify":
            result = verify_schedule_evidence(load(args.file))
        else:
            store = ScheduleStore(ControlStore(args.db))
            if args.action == "watch":
                from .schedule_watch import watch_schedules

                watch_schedules(
                    store,
                    args.workspace,
                    identity=args.identity,
                    every=args.every,
                    limit=args.limit,
                    max_ticks=args.max_ticks,
                    emit=lambda value: print(
                        json.dumps(value, allow_nan=False), flush=True
                    ),
                )
                return 0
            if args.action == "create":
                result = store.create(
                    args.workspace,
                    args.workflow,
                    args.version,
                    load(args.definition, 400000),
                    args.request_key,
                    now=instant(args.now) if args.now is not None else int(time.time()),
                )
            elif args.action == "list":
                result = store.list(args.workspace, after=args.after, limit=args.limit)
            elif args.action == "inspect":
                result = store.inspect(args.workspace, args.schedule)
            elif args.action == "dispatch":
                result = store.dispatch(
                    args.workspace,
                    args.request_key,
                    now=instant(args.now),
                    limit=args.limit,
                )
            elif args.action == "set-enabled":
                result = store.set_enabled(
                    args.workspace,
                    args.schedule,
                    args.enabled,
                    args.generation,
                    args.reason,
                    now=instant(args.now) if args.now is not None else int(time.time()),
                )
            elif args.action in ("events", "occurrences"):
                result = getattr(store, args.action)(
                    args.workspace, args.schedule, after=args.after, limit=args.limit
                )
            else:
                document = export_schedule(store, args.workspace, args.schedule)
                result = (
                    save_evidence(document, args.output)
                    if args.action == "export"
                    else verify_schedule_evidence(document)
                )
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0
    except (
        ValueError,
        TypeError,
        KeyError,
        OSError,
        ControlError,
        sqlite3.Error,
        OverflowError,
        RecursionError,
    ) as error:
        parser.exit(2, str(error) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
