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

