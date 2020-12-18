import unittest
from decimal import Decimal

from dagrivane.contracts import Contract
from dagrivane.tables import Table
from dagrivane.transforms import compile_operation
from dagrivane.quality import compile_gates


def table():
    schema = Contract.compile(
        {
            "name": "orders",
            "version": 1,
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "region", "type": "string", "nullable": True},
                {"name": "price", "type": "decimal", "nullable": True},
                {"name": "quantity", "type": "integer"},
            ],
        }
    )
    return Table.from_rows(
        schema,
        [
            {"id": 1, "region": "EU", "price": "0.10", "quantity": 3},
            {"id": 2, "region": "EU", "price": "0.20", "quantity": 2},
            {"id": 3, "region": None, "price": None, "quantity": 1},
        ],
    )


class TransformTests(unittest.TestCase):
    def apply(self, definition, source=None, right=None):
        source = source or table()
        compiled = compile_operation(
            definition, source.contract, right.contract if right else None
        )
        return compiled.apply(source, right)

