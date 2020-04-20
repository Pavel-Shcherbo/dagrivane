"""Compile and evaluate explicit data-quality gates without evaluating expressions."""

from dataclasses import dataclass
from decimal import Decimal, localcontext

from .contracts import convert, identifier, encode


@dataclass(frozen=True)
class Gate:
    id: str
    kind: str
    columns: tuple = ()
    minimum: object = None
    maximum: object = None
    ignore_nulls: bool = False

    def evaluate(self, table):
        rows = list(table.rows())
        failures = []
        observed = None
        if self.kind == "row_count":
            observed = len(rows)
        elif self.kind == "not_null":
            failures = [
                index
                for index, row in enumerate(rows, 1)
                if any(row[name] is None for name in self.columns)
            ]
        elif self.kind == "unique":
            seen = set()
            for index, row in enumerate(rows, 1):
                key = tuple(row[name] for name in self.columns)
                if self.ignore_nulls and any(value is None for value in key):
                    continue
                if key in seen:
                    failures.append(index)
                seen.add(key)
        elif self.kind == "sum":
            with localcontext() as context:
                context.prec = 80
                observed = sum(
                    (
                        row[self.columns[0]]
                        for row in rows
                        if row[self.columns[0]] is not None
                    ),
                    Decimal(0),
                )
        if observed is not None:
            passed = (self.minimum is None or observed >= self.minimum) and (
                self.maximum is None or observed <= self.maximum
            )
            return {
                "id": self.id,
                "kind": self.kind,
                "passed": passed,
                "observed": encode(observed),
                "min": encode(self.minimum),
                "max": encode(self.maximum),
            }
        return {
            "id": self.id,
            "kind": self.kind,
            "passed": not failures,
            "failure_count": len(failures),
            "sample_record_numbers": failures[:20],
        }


def compile_gates(definitions, contract):
    if not isinstance(definitions, list) or not 1 <= len(definitions) <= 100:
        raise ValueError("quality gates require 1–100 rules")
    gates, ids = [], set()
    for definition in definitions:
        if not isinstance(definition, dict) or set(definition) - {
            "id",
            "kind",
            "columns",
            "min",
            "max",
            "ignore_nulls",
        }:
            raise ValueError("quality gate contains unsupported fields")
        name = identifier(definition.get("id"), "gate ID")
        if name in ids:
            raise ValueError("quality gate IDs must be unique")
        ids.add(name)
        kind = definition.get("kind")
        if kind not in ("row_count", "not_null", "unique", "sum"):
            raise ValueError("unsupported quality gate")
        columns = definition.get("columns", [])
        if (
            not isinstance(columns, list)
            or any(not isinstance(name, str) for name in columns)
            or len(set(columns)) != len(columns)
        ):
            raise ValueError("gate columns must be unique names")
        for column in columns:
            contract.column(column)
        if (
            kind == "row_count"
            and columns
            or kind in ("not_null", "unique")
            and not columns
            or kind == "sum"
            and len(columns) != 1
        ):
            raise ValueError("invalid column count for quality gate")
        if "ignore_nulls" in definition and kind != "unique":
            raise ValueError("ignore_nulls is supported only by unique")
        ignore = definition.get("ignore_nulls", False)
        if type(ignore) is not bool:
            raise ValueError("ignore_nulls must be boolean")
        lower = upper = None
        if kind in ("row_count", "sum"):
            if not set(definition) & {"min", "max"}:
                raise ValueError("numeric gate requires min or max")
            dtype = (
                "integer" if kind == "row_count" else contract.column(columns[0]).kind
            )
            if dtype not in ("integer", "decimal"):
                raise ValueError("sum gate requires a numeric column")
            lower = convert(definition["min"], dtype) if "min" in definition else None
            upper = convert(definition["max"], dtype) if "max" in definition else None
            if lower is not None and upper is not None and lower > upper:
                raise ValueError("quality min exceeds max")
            if kind == "row_count" and any(
                value is not None and value < 0 for value in (lower, upper)
            ):
                raise ValueError("row-count boundaries cannot be negative")
        elif set(definition) & {"min", "max"}:
            raise ValueError("range boundaries are supported only by row_count and sum")
        gates.append(Gate(name, kind, tuple(columns), lower, upper, ignore))
    return tuple(gates)
