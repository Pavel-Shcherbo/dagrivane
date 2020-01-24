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


def finite_decimal(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("decimal requires text, an integer or Decimal, never float")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ValueError("invalid decimal") from error
    if not parsed.is_finite():
        raise ValueError("decimal must be finite")
    sign, digits, exponent = parsed.as_tuple()
    if len(digits) > 38 or abs(exponent) > 18 or parsed.adjusted() > 37:
        raise ValueError("decimal exceeds 38-digit / 18-scale limits")
    return parsed


