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


