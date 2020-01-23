"""Versioned tabular contracts with explicit CSV conversion and compatibility checks."""

import datetime as dt
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
INTEGER = re.compile(r"^-?(?:0|[1-9][0-9]*)$")
KINDS = frozenset({"string", "integer", "decimal", "boolean", "date", "timestamp"})


def identifier(value, label="name"):
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise ValueError(
            f"{label} must be an ASCII identifier of at most 64 characters"
        )
    return value


