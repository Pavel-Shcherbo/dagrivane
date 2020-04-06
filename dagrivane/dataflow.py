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


def publish_bundle(path, bundle):
    encoded = json.dumps(bundle, indent=2, allow_nan=False).encode() + b"\n"
    if len(encoded) > 64 * 1024 * 1024:
        raise ValueError("output bundle exceeds 64 MiB")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".dagrivane-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        # Atomic create without replacement. A failed or repeated run must not
        # silently replace an artifact that another run already published.
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def compile_plan(definition):
    fields(
        definition,
        {"version", "sources", "steps", "output", "gates", "destination"},
        ("version", "sources", "output", "gates"),
    )
    try:
        serialized = json.dumps(
            definition, sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode()
    except (ValueError, TypeError, RecursionError) as error:
        raise ValueError("data plan must contain finite JSON values") from error
    if len(serialized) > MAX_PLAN_BYTES:
        raise ValueError("data plan exceeds 1 MiB")
    if type(definition["version"]) is not int or definition["version"] != 1:
        raise ValueError("only data plan version 1 is supported")
    definitions = copy.deepcopy(definition)
    source_specs, steps = definitions["sources"], definitions.get("steps", [])
    if not isinstance(source_specs, list) or not 1 <= len(source_specs) <= 16:
        raise ValueError("data plan requires 1–16 sources")
    if not isinstance(steps, list) or len(steps) > 128:
        raise ValueError("data plan supports at most 128 steps")
    contracts, sources, compiled = {}, [], []
    for source in source_specs:
        fields(
            source,
            {"id", "path", "contract", "null_token", "sha256"},
            ("id", "path", "contract"),
        )
        name = identifier(source["id"], "source ID")
        if name in contracts:
            raise ValueError("source and step IDs must be unique")
        local_path(".", source["path"])
        contract = Contract.compile(source["contract"])
        token = source.get("null_token", "\\N")
        if (
            not isinstance(token, str)
            or not 1 <= len(token) <= 32
            or any(c in token for c in "\r\n\x00")
        ):
            raise ValueError("invalid CSV null_token")
        if "sha256" in source and (
            not isinstance(source["sha256"], str)
            or len(source["sha256"]) != 64
            or any(c not in "0123456789abcdef" for c in source["sha256"])
        ):
            raise ValueError("source sha256 must be lowercase SHA-256")
        contracts[name] = contract
        sources.append((source, contract))
    for step in steps:
        if not isinstance(step, dict):
            raise ValueError("each step must be an object")
        name = identifier(step.get("id"), "step ID")
        if name in contracts:
            raise ValueError("source and step IDs must be unique")
        input_name = step.get("input")
        if not isinstance(input_name, str) or input_name not in contracts:
            raise ValueError("step input must reference an earlier source or step")
        right = None
        if step.get("op") == "join":
            reference = step.get("right")
            if not isinstance(reference, str) or reference not in contracts:
                raise ValueError("join right must reference an earlier source or step")
            right = contracts[reference]
        operation = compile_operation(step, contracts[input_name], right)
        contracts[name] = operation.contract
        compiled.append((step, operation))
    output = definitions["output"]
    if not isinstance(output, str) or output not in contracts:
        raise ValueError("output must name a source or step")
    gates = compile_gates(definitions["gates"], contracts[output])
    if "destination" in definitions:
        local_path(".", definitions["destination"])
    return DataPlan(
        definitions,
        tuple(sources),
        tuple(compiled),
        output,
        gates,
        hashlib.sha256(serialized).hexdigest(),
    )


def run_plan(definition, root="."):
    return compile_plan(definition).execute(root)[0]


