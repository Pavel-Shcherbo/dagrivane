"""An executable local data-to-application release control with retained evidence."""

import argparse
import hashlib
import json
import os
import subprocess
import sqlite3
import sys
import zipfile
from pathlib import Path
from shutil import copy2

from .catalog import Catalog
from .catalog_contracts import CatalogError
from .dataflow import compile_plan
from .release_bundle import export_release
from .resources import examples_directory

SCENARIOS = ("healthy", "corrupt-data", "missing-staging")


def build_application(source, destination):
    content = Path(source).read_bytes()
    info = zipfile.ZipInfo("__main__.py", date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    with zipfile.ZipFile(
        destination, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        archive.writestr(info, content)
    return hashlib.sha256(content).hexdigest()


def run_demo(directory, scenario="healthy"):
    if scenario not in SCENARIOS:
        raise ValueError("unknown local release control scenario")
    root = Path(directory).resolve()
    root.mkdir(parents=True)
    inputs = root / "inputs"
    inputs.mkdir()
    for name in ("orders.csv", "summary_app.py"):
        copy2(examples_directory() / name, inputs / name)
    definition = json.loads((examples_directory() / "dataflow.json").read_text())
    plan = compile_plan(definition)
    data_report, table = plan.execute(inputs)
    if not data_report["passed"]:
        raise CatalogError("bundled control data failed its own quality gates")
    bundle = inputs / definition["destination"]
    build = root / "summary-app.pyz"
    revision = build_application(inputs / "summary_app.py", build)
    environment = {
        key: os.environ[key]
        for key in ("PATH", "SYSTEMROOT", "WINDIR", "TMPDIR")
        if key in os.environ
    }
    execution = subprocess.run(
        [sys.executable, str(build), str(bundle)],
        env=environment,
        cwd=root,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if execution.returncode != 0:
        raise CatalogError("bundled application failed its executable package control")
    application_result = json.loads(execution.stdout)
    if application_result != {"currencies": {"USD": "112.00"}, "orders": 4}:
        raise CatalogError(
            "bundled application result differs from the hand-checked order total"
        )
    catalog = Catalog(root / "catalog")
    workspace = "local-release"
    catalog.create_workspace(workspace)
    gates = [
        {"id": "currencies", "kind": "unique", "columns": ["currency"]},
        {"id": "row_count", "kind": "row_count", "min": 1, "max": 1},
        {
            "id": "revenue",
            "kind": "sum",
            "columns": ["revenue"],
            "min": "112.00",
            "max": "112.00",
        },
        {"id": "orders", "kind": "sum", "columns": ["orders"], "min": 4, "max": 4},
    ]
    catalog.register_policy(
        workspace, "orders_summary", 1, table.contract.definition(), gates
    )
    dataset = catalog.publish_table(
        workspace,
        "orders_summary",
        1,
        bundle,
        run_id="data-control",
        request_key="dataset",
    )
    application = catalog.record_build(
        workspace,
        "summary_application",
        build,
        revision=revision,
        run_id="build-control",
        request_key="build",
        parents=[dataset["id"]],
        media_type="application/zip",
    )
    release = catalog.create_release(
        workspace,
        "orders-release",
        {
            "application": {"artifact": application["id"]},
            "orders": {"artifact": dataset["id"]},
        },
        request_key="release",
        requirements={
            "application": {"kind": "build"},
            "orders": {
                "kind": "table",
                "dataset": "orders_summary",
                "policy_version": 1,
            },
        },
    )
    catalog.create_environment(workspace, "staging")
    catalog.create_environment(workspace, "production", requires="staging")
    if scenario == "corrupt-data":
        catalog.objects.path(dataset["sha256"]).write_bytes(
            b"controlled corruption in this isolated demo store"
        )
    steps = [
        {"name": "Data quality", "status": "passed"},
        {"name": "Executable package", "status": "passed"},
        {"name": "Pinned manifest", "status": "passed"},
    ]
    blocked = None
    try:
        if scenario != "missing-staging":
            catalog.promote(
                workspace,
                "staging",
                release["id"],
                expected_generation=0,
                request_key="stage",
            )
            steps.append({"name": "Staging pointer", "status": "passed"})
        else:
            steps.append({"name": "Staging pointer", "status": "not selected"})
        catalog.promote(
            workspace,
            "production",
            release["id"],
            expected_generation=0,
            request_key="promote",
        )
        steps.append({"name": "Production pointer", "status": "passed"})
    except ValueError as error:
        blocked = str(error)
        if len(steps) == 3:
            steps.append({"name": "Staging pointer", "status": "blocked"})
        steps.append({"name": "Production pointer", "status": "blocked"})
    artifacts = []
    for record in (dataset, application):
        try:
            verification = catalog.verify_artifact(workspace, record["id"])
        except ValueError as error:
            verification = {"passed": False, "error": str(error)}
        artifacts.append({**record, "verification": verification})
    exported = (
        export_release(catalog, workspace, release["id"], root / "release-export")
        if blocked is None
        else None
    )
    return {
        "scenario": scenario,
        "passed": blocked is None,
        "blocked_reason": blocked,
        "steps": steps,
        "data": data_report,
        "application": {
            "source_sha256": revision,
            "result": application_result,
            "exit_code": execution.returncode,
        },
        "artifacts": artifacts,
        "release": release,
        "lineage": catalog.lineage(workspace, application["id"]),
        "environments": [
            catalog.environment(workspace, name) for name in ("staging", "production")
        ],
        "audit": catalog.audit(workspace),
        "export": exported,
        "catalog_root": str(catalog.root),
        "scope": "local artifact validation and environment metadata; no external rollout is performed",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the reviewed local data/application release control"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="New control-run directory"
    )
    parser.add_argument("--scenario", choices=SCENARIOS, default="healthy")
    args = parser.parse_args(argv)
    try:
        report = run_demo(args.output, args.scenario)
    except (ValueError, OSError, sqlite3.Error, subprocess.SubprocessError) as error:
        parser.exit(2, f"{error}\n")
    print(json.dumps(report, indent=2, allow_nan=False))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
