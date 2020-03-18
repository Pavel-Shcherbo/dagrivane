"""Compile and run bounded local data plans, publishing only quality-passing output."""

import argparse
import copy
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .contracts import Contract, identifier
from .quality import compile_gates
from .tables import read_csv, local_path
from .transforms import compile_operation, fields

MAX_PLAN_BYTES = 1024 * 1024


def load_plan(payload):
    if (
        not isinstance(payload, (str, bytes))
        or len(payload if isinstance(payload, bytes) else payload.encode())
        > MAX_PLAN_BYTES
    ):
        raise ValueError("data plan must be bounded JSON text of at most 1 MiB")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON plan key")
            result[key] = value
        return result

    def reject(_):
        raise ValueError("plan numbers must be finite")

    try:
        return json.loads(payload, object_pairs_hook=unique, parse_constant=reject)
    except RecursionError as error:
        raise ValueError("plan nesting exceeds parser limit") from error


@dataclass(frozen=True)
class DataPlan:
    definition: dict
    sources: tuple
    steps: tuple
    output: str
    gates: tuple
    digest: str

    def execute(self, root):
        tables, events = {}, []
        # Resolve every source before any I/O; no output is published until all
        # transformations and gates finish. Input bytes are pinned when specified.
        paths = [local_path(root, source["path"]) for source, _ in self.sources]
        if sum(path.stat().st_size for path in paths) > 64 * 1024 * 1024:
            raise ValueError("combined CSV sources exceed 64 MiB")
        references = {
            name: 0
            for name in [source["id"] for source, _ in self.sources]
            + [step["id"] for step, _ in self.steps]
        }
        references[self.output] += 1
        for definition, operation in self.steps:
            references[definition["input"]] += 1
            if operation.kind == "join":
                references[definition["right"]] += 1

        def remember(name, table):
            tables[name] = table
            if (
                sum(
                    len(item.records) * len(item.contract.columns)
                    for item in tables.values()
                )
                > 4000000
            ):
                raise ValueError("retained dataflow tables exceed 4 million cells")
            events.append({"id": name, **table.summary()})
            if references[name] == 0:
                del tables[name]

        for (source, contract), path in zip(self.sources, paths):
            remember(
                source["id"],
                read_csv(
                    path,
                    contract,
                    null_token=source.get("null_token", "\\N"),
                    expected_sha256=source.get("sha256"),
                ),
            )
        for definition, operation in self.steps:
            left = tables[definition["input"]]
            right = tables[definition["right"]] if operation.kind == "join" else None
            result = operation.apply(left, right)
            for name in [definition["input"]] + (
                [definition["right"]] if right is not None else []
            ):
                references[name] -= 1
                if references[name] == 0:
                    del tables[name]
            remember(definition["id"], result)
        table = tables[self.output]
        checks = [gate.evaluate(table) for gate in self.gates]
        report = {
            "passed": all(check["passed"] for check in checks),
            "plan_sha256": self.digest,
            "output_node": self.output,
            "output": table.summary(),
            "checks": checks,
            "nodes": events,
        }
        if report["passed"] and "destination" in self.definition:
            target = local_path(root, self.definition["destination"])
            bundle = {
                "format": "dagrivane.table.v1",
                "report": report,
                "rows": table.json_rows(),
            }
            publish_bundle(target, bundle)
            report = {**report, "artifact": self.definition["destination"]}
        return report, table


