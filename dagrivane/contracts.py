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


def convert(value, kind, from_csv=False):
    if kind == "string":
        if not isinstance(value, str) or len(value) > 65536 or "\x00" in value:
            raise ValueError("string must contain at most 65536 characters without NUL")
        return value
    if kind == "integer":
        if from_csv:
            if (
                not isinstance(value, str)
                or not INTEGER.fullmatch(value)
                or len(value) > 20
            ):
                raise ValueError("integer must use canonical decimal notation")
            value = int(value)
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ValueError("integer must fit signed 64-bit range")
        return value
    if kind == "decimal":
        return finite_decimal(value)
    if kind == "boolean":
        if from_csv:
            if value not in ("true", "false"):
                raise ValueError("boolean must be true or false")
            return value == "true"
        if type(value) is not bool:
            raise ValueError("boolean requires a boolean value")
        return value
    if kind == "date":
        if type(value) is dt.date:
            return value
        if not isinstance(value, str):
            raise ValueError("date requires YYYY-MM-DD")
        try:
            result = dt.date.fromisoformat(value)
        except ValueError as error:
            raise ValueError("invalid ISO date") from error
        if result.isoformat() != value:
            raise ValueError("date requires canonical YYYY-MM-DD")
        return result
    if kind == "timestamp":
        if isinstance(value, str):
            try:
                value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError("invalid ISO timestamp") from error
        if (
            not isinstance(value, dt.datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError("timestamp requires an explicit UTC offset")
        try:
            return value.astimezone(dt.timezone.utc)
        except (OverflowError, ValueError) as error:
            raise ValueError(
                "timestamp lies outside the supported UTC range"
            ) from error
    raise ValueError("unsupported column type")


def encode(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dt.datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    nullable: bool = False
    minimum: object = None
    maximum: object = None
    choices: tuple = ()

