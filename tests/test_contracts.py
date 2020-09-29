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

    def test_types_do_not_coerce_booleans_floats_or_ambiguous_csv(self):
        invalid = [
            ("integer", True),
            ("integer", 1.0),
            ("decimal", 0.1),
            ("boolean", 1),
            ("date", "20260101"),
            ("timestamp", "2026-01-01T00:00:00"),
            ("decimal", "NaN"),
            ("decimal", "1e999"),
            ("integer", 2**63),
            ("timestamp", "0001-01-01T00:00:00+23:59"),
        ]
        for kind, value in invalid:
            with self.subTest(kind=kind, value=value), self.assertRaises(ValueError):
                Column.compile({"name": "value", "type": kind}).parse(value)
        for kind, text in [
            ("integer", "01"),
            ("integer", "+1"),
            ("integer", " 1"),
            ("boolean", "TRUE"),
            ("integer", ""),
        ]:
            with self.subTest(kind=kind, text=text), self.assertRaises(ValueError):
                Column.compile({"name": "value", "type": kind}).parse(
                    text, from_csv=True
                )

    def test_timestamp_normalizes_offsets_and_date_remains_date(self):
        timestamp = Column.compile({"name": "created", "type": "timestamp"})
        self.assertEqual(
            timestamp.parse("2026-01-01T03:00:00+03:00"),
            dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        )
        self.assertEqual(
            Column.compile({"name": "day", "type": "date"}).parse("2026-01-01"),
            dt.date(2026, 1, 1),
        )

    def test_range_enum_and_null_constraints_are_enforced(self):
        column = Column.compile(
            {"name": "state", "type": "integer", "min": 0, "max": 10, "enum": [1, 2]}
        )
        self.assertEqual(column.parse(2), 2)
        for value in [None, -1, 3, 11]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                column.parse(value)

    def test_contract_round_trip_and_exact_fields(self):
        schema = contract([{"name": "price", "type": "decimal", "min": "0.01"}])
        self.assertEqual(Contract.compile(schema.definition()), schema)
        for row in [{}, {"price": "1", "extra": 2}]:
            with self.assertRaises(ValueError):
                schema.parse_row(row)

