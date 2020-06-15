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


