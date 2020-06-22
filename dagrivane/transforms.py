"""Schema-aware relational operations with bounded cardinality and typed values."""

import copy
from dataclasses import dataclass
from decimal import Decimal, localcontext, ROUND_HALF_EVEN

from .contracts import Column, Contract, convert, identifier
from .tables import Table, MAX_ROWS, MAX_CELLS


def fields(definition, allowed, required=()):
    if (
        not isinstance(definition, dict)
        or set(definition) - set(allowed)
        or not set(required) <= set(definition)
    ):
        raise ValueError("operation contains unsupported or missing fields")


def names(value, contract, *, nonempty=True):
    if (
        not isinstance(value, list)
        or nonempty
        and not value
        or any(not isinstance(item, str) for item in value)
    ):
        raise ValueError("columns must be a list of names")
    if len(set(value)) != len(value):
        raise ValueError("columns must be unique")
    for name in value:
        contract.column(name)
    return tuple(value)


def output_contract(previous, columns):
    return Contract.compile(
        {
            "name": previous.name,
            "version": previous.version,
            "columns": [column.definition() for column in columns],
        }
    )


def widened(column, *, name=None, nullable=None):
    definition = column.definition()
    if name is not None:
        definition["name"] = name
    if nullable is not None:
        definition["nullable"] = nullable
    return Column.compile(definition)


def operand(definition, contract, kind):
    if not isinstance(definition, dict) or len(definition) != 1:
        raise ValueError("operand requires exactly one column or literal")
    if "column" in definition:
        column = contract.column(definition["column"])
        if column.kind != kind and not (kind == "decimal" and column.kind == "integer"):
            raise ValueError("operand column has an incompatible type")
        return ("column", column.name)
    if "literal" in definition:
        value = definition["literal"]
        return ("literal", None if value is None else convert(value, kind))
    raise ValueError("unsupported operand")


def value_of(expression, row):
    return row[expression[1]] if expression[0] == "column" else expression[1]


@dataclass(frozen=True)
class Operation:
    kind: str
    contract: Contract
    config: dict

    def apply(self, table, right=None):
        rows = list(table.rows())
        config = self.config
        if self.kind == "select":
            selected = [
                {
                    config["rename"].get(name, name): row[name]
                    for name in config["columns"]
                }
                for row in rows
            ]
        elif self.kind == "filter":
            selected = [
                row
                for row in rows
                if all(
                    condition_matches(row, condition)
                    for condition in config["conditions"]
                )
            ]
        elif self.kind == "calculate":
            selected = (
                {**row, config["column"].name: calculate(config["expression"], row)}
                for row in rows
            )
        elif self.kind == "aggregate":
            selected = aggregate(rows, config)
        elif self.kind == "join":
            if not isinstance(right, Table):
                raise ValueError("join requires a right-hand table")
            selected = join(
                rows, list(right.rows()), config, len(self.contract.columns)
            )
        elif self.kind == "sort":
            selected = rows
            for key in reversed(config["keys"]):
                name, descending, nulls_first = key
                present = [row for row in selected if row[name] is not None]
                missing = [row for row in selected if row[name] is None]
                present.sort(key=lambda row: row[name], reverse=descending)
                selected = missing + present if nulls_first else present + missing
        elif self.kind == "distinct":
            selected, seen = [], set()
            for row in rows:
                key = tuple(row[name] for name in config["columns"])
                if key not in seen:
                    selected.append(row)
                    seen.add(key)
        elif self.kind == "limit":
            selected = rows[config["offset"] : config["offset"] + config["count"]]
        else:
            raise ValueError("uncompiled operation")
        sources = tuple(
            dict.fromkeys(table.sources + (right.sources if right is not None else ()))
        )
        return Table.from_rows(self.contract, selected, sources)


def condition_matches(row, condition):
    column, operation, expected = condition
    actual = row[column]
    if operation == "is_null":
        return actual is None
    if operation == "not_null":
        return actual is not None
    if actual is None:
        return False
    if operation == "eq":
        return actual == expected
    if operation == "ne":
        return actual != expected
    if operation == "gt":
        return actual > expected
    if operation == "gte":
        return actual >= expected
    if operation == "lt":
        return actual < expected
    if operation == "lte":
        return actual <= expected
    if operation == "in":
        return actual in expected
    if operation == "not_in":
        return actual not in expected
    if operation == "starts_with":
        return actual.startswith(expected)
    return expected in actual


