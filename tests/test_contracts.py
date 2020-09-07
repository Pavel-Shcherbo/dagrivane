import datetime as dt
import unittest
from decimal import Decimal

from dagrivane.contracts import Column, Contract, compatibility


def contract(columns, version=1):
    return Contract.compile({"name": "orders", "version": version, "columns": columns})


class ContractTests(unittest.TestCase):
    def test_csv_preserves_money_and_distinguishes_null_from_empty(self):
        schema = contract(
            [
                {"name": "amount", "type": "decimal"},
                {"name": "note", "type": "string", "nullable": True},
            ]
        )
        row = schema.parse_row({"amount": "0.10", "note": ""}, from_csv=True)
        self.assertEqual(row["amount"] + Decimal("0.20"), Decimal("0.30"))
        self.assertEqual(row["note"], "")
        self.assertIsNone(
            schema.parse_row({"amount": "1", "note": "\\N"}, from_csv=True)["note"]
        )

