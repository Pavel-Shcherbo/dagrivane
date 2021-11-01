import json
from pathlib import Path

from dagrivane.contracts import Contract
from dagrivane.tables import Table


def contract(version=1):
    return {
        "name": "orders",
        "version": version,
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "amount", "type": "decimal", "min": "0"},
        ],
    }


def gates():
    return [
        {"id": "ids", "kind": "unique", "columns": ["id"]},
        {
            "id": "total",
            "kind": "sum",
            "columns": ["amount"],
            "min": "0",
            "max": "1000",
        },
    ]


def write_bundle(directory, name="orders.json", rows=None, version=1):
    rows = (
        [{"id": 1, "amount": "10.00"}, {"id": 2, "amount": "20.00"}]
        if rows is None
        else rows
    )
    table = Table.from_rows(Contract.compile(contract(version)), rows, ["1" * 64])
    payload = {
        "format": "dagrivane.table.v1",
        "report": {
            "passed": True,
            "plan_sha256": "2" * 64,
            "output": table.summary(),
            "checks": [{"id": "producer", "passed": True}],
        },
        "rows": table.json_rows(),
    }
    path = Path(directory) / name
    path.write_text(json.dumps(payload))
    return path


def prepare(catalog, directory, workspace="research"):
    catalog.create_workspace(workspace)
    catalog.register_policy(workspace, "orders", 1, contract(), gates())
    table = catalog.publish_table(
        workspace,
        "orders",
        1,
        write_bundle(directory),
        run_id="data-run",
        request_key="publish-orders",
    )
    path = Path(directory) / "application.txt"
    path.write_text("validated application package\n")
    build = catalog.record_build(
        workspace,
        "api",
        path,
        revision="a" * 40,
        run_id="build-run",
        request_key="record-build",
        parents=[table["id"]],
    )
    release = catalog.create_release(
        workspace,
        "release-1",
        {"app": {"artifact": build["id"]}, "orders": {"artifact": table["id"]}},
        request_key="create-release",
        requirements={
            "app": {"kind": "build"},
            "orders": {"kind": "table", "dataset": "orders", "policy_version": 1},
        },
    )
    return table, build, release
