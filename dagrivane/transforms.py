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

