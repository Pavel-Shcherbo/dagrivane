"""Operator CLI for local data artifacts, pinned releases and export verification."""

import argparse
import json
import sqlite3
from pathlib import Path

from .catalog import Catalog
from .catalog_contracts import parse_json
from .object_store import bounded_file
from .release_bundle import export_release, verify_export


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Inspect and verify local artifact/release identities"
    )
    parser.add_argument("--store", type=Path, default=Path(".local/artifacts"))
    actions = parser.add_subparsers(dest="action", required=True)
    workspace = actions.add_parser("workspace-create")
    workspace.add_argument("workspace")
    policy = actions.add_parser("policy-register")
    policy.add_argument("workspace")
    policy.add_argument("name")
    policy.add_argument(
        "definition", type=Path, help="JSON with contract, gates and version"
    )
    policy.add_argument("--allow-breaking", action="store_true")
    publish = actions.add_parser("publish-table")
    publish.add_argument("workspace")
    publish.add_argument("name")
    publish.add_argument("file", type=Path)
    publish.add_argument("--policy-version", type=int, required=True)
    publish.add_argument("--run-id", required=True)
    publish.add_argument("--request-key", required=True)
    publish.add_argument("--parent", action="append", default=[])
    build = actions.add_parser("record-build")
    build.add_argument("workspace")
    build.add_argument("name")
    build.add_argument("file", type=Path)
    build.add_argument("--revision", required=True)
    build.add_argument("--run-id", required=True)
    build.add_argument("--request-key", required=True)
    build.add_argument("--parent", action="append", default=[])
    build.add_argument("--media-type", default="application/octet-stream")
    listing = actions.add_parser("artifacts")
    listing.add_argument("workspace")
    listing.add_argument("--after", type=int, default=0)
    listing.add_argument("--limit", type=int, default=100)
    for name in ("artifact", "verify-artifact", "lineage"):
        action = actions.add_parser(name)
        action.add_argument("workspace")
        action.add_argument("artifact")
        if name == "lineage":
            action.add_argument(
                "--direction", choices=["ancestors", "descendants"], default="ancestors"
            )
            action.add_argument("--max-depth", type=int, default=100)
    alias = actions.add_parser("alias-move")
    alias.add_argument("workspace")
    alias.add_argument("name")
    alias.add_argument("artifact")
    alias.add_argument("--expected-generation", type=int, required=True)
    release = actions.add_parser("release-create")
    release.add_argument("workspace")
    release.add_argument("name")
    release.add_argument(
        "definition", type=Path, help="JSON with bindings and optional requirements"
    )
    release.add_argument("--request-key", required=True)
    for name in ("release", "verify-release", "export-release"):
        action = actions.add_parser(name)
        action.add_argument("workspace")
        action.add_argument("release")
        if name == "export-release":
            action.add_argument("destination", type=Path)
    offline = actions.add_parser("verify-export")
    offline.add_argument("directory", type=Path)
    environment = actions.add_parser("environment-create")
    environment.add_argument("workspace")
    environment.add_argument("name")
    environment.add_argument("--requires")
    promote = actions.add_parser("promote")
    promote.add_argument("workspace")
    promote.add_argument("environment")
    promote.add_argument("release")
    promote.add_argument("--expected-generation", type=int, required=True)
    promote.add_argument("--request-key", required=True)
    audit = actions.add_parser("audit")
    audit.add_argument("workspace")
    audit.add_argument("--limit", type=int, default=1000)
    events = actions.add_parser("events")
    events.add_argument("workspace")
    events.add_argument("--after", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        if args.action == "verify-export":
            report = verify_export(args.directory)
        else:
            if (
                args.action != "workspace-create"
                and not (args.store / "catalog.sqlite").is_file()
            ):
                raise ValueError(
                    "artifact catalog does not exist; create a workspace first"
                )
            catalog = Catalog(args.store)
            action = args.action
            if action == "workspace-create":
                report = catalog.create_workspace(args.workspace)
            elif action == "policy-register":
                definition = parse_json(bounded_file(args.definition, 1024 * 1024))
                if not isinstance(definition, dict) or set(definition) != {
                    "contract",
                    "gates",
                    "version",
                }:
                    raise ValueError(
                        "policy definition requires exactly contract, gates and version"
                    )
                report = catalog.register_policy(
                    args.workspace,
                    args.name,
                    definition["version"],
                    definition["contract"],
                    definition["gates"],
                    allow_breaking=args.allow_breaking,
                )
            elif action == "publish-table":
                report = catalog.publish_table(
                    args.workspace,
                    args.name,
                    args.policy_version,
                    args.file,
                    run_id=args.run_id,
                    request_key=args.request_key,
                    parents=args.parent,
                )
            elif action == "record-build":
                report = catalog.record_build(
                    args.workspace,
                    args.name,
                    args.file,
                    revision=args.revision,
                    run_id=args.run_id,
                    request_key=args.request_key,
                    parents=args.parent,
                    media_type=args.media_type,
                )
            elif action == "artifacts":
                report = catalog.list_artifacts(args.workspace, args.after, args.limit)
            elif action == "artifact":
                report = catalog.artifact(args.workspace, args.artifact)
            elif action == "verify-artifact":
                report = catalog.verify_artifact(args.workspace, args.artifact)
            elif action == "lineage":
                report = catalog.lineage(
                    args.workspace,
                    args.artifact,
                    direction=args.direction,
                    max_depth=args.max_depth,
                )
            elif action == "alias-move":
                report = catalog.move_alias(
                    args.workspace,
                    args.name,
                    args.artifact,
                    expected_generation=args.expected_generation,
                )
            elif action == "release-create":
                definition = parse_json(bounded_file(args.definition, 1024 * 1024))
                if (
                    not isinstance(definition, dict)
                    or set(definition) - {"bindings", "requirements"}
                    or "bindings" not in definition
                ):
                    raise ValueError(
                        "release definition requires bindings and optional requirements"
                    )
                report = catalog.create_release(
                    args.workspace,
                    args.name,
                    definition["bindings"],
                    request_key=args.request_key,
                    requirements=definition.get("requirements"),
                )
            elif action == "release":
                report = catalog.release(args.workspace, args.release)
            elif action == "verify-release":
                report = catalog.verify_release(args.workspace, args.release)
            elif action == "export-release":
                report = export_release(
                    catalog, args.workspace, args.release, args.destination
                )
            elif action == "environment-create":
                report = catalog.create_environment(
                    args.workspace, args.name, args.requires
                )
            elif action == "promote":
                report = catalog.promote(
                    args.workspace,
                    args.environment,
                    args.release,
                    expected_generation=args.expected_generation,
                    request_key=args.request_key,
                )
            elif action == "audit":
                report = catalog.audit(args.workspace, args.limit)
            else:
                report = {"events": catalog.events(args.workspace, args.after)}
        print(json.dumps(report, indent=2, allow_nan=False))
        return 1 if report.get("passed") is False else 0
    except (ValueError, KeyError, OSError, sqlite3.Error) as error:
        parser.exit(2, f"{error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
