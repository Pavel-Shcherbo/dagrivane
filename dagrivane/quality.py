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


