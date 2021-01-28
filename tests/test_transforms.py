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

    def test_money_calculation_and_hand_computed_group_sum(self):
        priced = self.apply(
            {
                "op": "calculate",
                "column": {"name": "total", "type": "decimal", "nullable": True},
                "expression": {
                    "op": "multiply",
                    "args": [{"column": "price"}, {"column": "quantity"}],
                },
            }
        )
        summary = self.apply(
            {
                "op": "aggregate",
                "group_by": ["region"],
                "metrics": [
                    {"name": "revenue", "op": "sum", "column": "total"},
                    {"name": "orders", "op": "count"},
                ],
            },
            priced,
        )
        self.assertEqual(
            summary.json_rows(),
            [
                {"region": "EU", "revenue": "0.70", "orders": 2},
                {"region": None, "revenue": None, "orders": 1},
            ],
        )

    def test_divide_rounds_to_explicit_places_and_zero_fails(self):
        definition = {
            "op": "calculate",
            "column": {"name": "third", "type": "decimal"},
            "expression": {
                "op": "divide",
                "args": [{"column": "quantity"}, {"literal": "3"}],
                "places": 4,
            },
        }
        result = self.apply(definition)
        self.assertEqual(list(result.rows())[1]["third"], Decimal("0.6667"))
        definition["expression"]["args"][1]["literal"] = "0"
        with self.assertRaisesRegex(ValueError, "zero"):
            self.apply(definition)

    def test_null_filter_does_not_treat_unknown_as_not_equal(self):
        result = self.apply(
            {
                "op": "filter",
                "conditions": [{"column": "region", "op": "ne", "value": "EU"}],
            }
        )
        self.assertEqual(len(result.records), 0)
        result = self.apply(
            {"op": "filter", "conditions": [{"column": "region", "op": "is_null"}]}
        )
        self.assertEqual(result.json_rows()[0]["id"], 3)

    def test_select_rename_and_collision_validation(self):
        result = self.apply(
            {"op": "select", "columns": ["id", "region"], "rename": {"id": "order_id"}}
        )
        self.assertEqual(result.contract.names, ("order_id", "region"))
        with self.assertRaises(ValueError):
            self.apply(
                {
                    "op": "select",
                    "columns": ["id", "region"],
                    "rename": {"id": "region"},
                }
            )

    def test_left_join_preserves_unmatched_rows_and_null_never_matches(self):
        right_schema = Contract.compile(
            {
                "name": "regions",
                "version": 1,
                "columns": [
                    {"name": "code", "type": "string", "nullable": True},
                    {"name": "tax", "type": "decimal"},
                ],
            }
        )
        right = Table.from_rows(
            right_schema, [{"code": "EU", "tax": "0.2"}, {"code": None, "tax": "0.9"}]
        )
        definition = {
            "op": "join",
            "right": "regions",
            "left_on": ["region"],
            "right_on": ["code"],
            "right_columns": {"tax": "tax_rate"},
            "how": "left",
        }
        rows = self.apply(definition, right=right).json_rows()
        self.assertEqual([row["tax_rate"] for row in rows], ["0.2", "0.2", None])
        duplicated = Table.from_rows(right_schema, [{"code": "EU", "tax": "0.2"}] * 2)
        with self.assertRaisesRegex(ValueError, "cardinality"):
            self.apply(definition, right=duplicated)
        definition["cardinality"] = "one_to_one"
        with self.assertRaisesRegex(ValueError, "one_to_one"):
            self.apply(definition, right=right)

    def test_global_empty_aggregate_has_count_zero_and_sum_null(self):
        source = Table(table().contract, ())
        result = self.apply(
            {
                "op": "aggregate",
                "group_by": [],
                "metrics": [
                    {"name": "n", "op": "count"},
                    {"name": "total", "op": "sum", "column": "price"},
                ],
            },
            source,
        )
        self.assertEqual(result.json_rows(), [{"n": 0, "total": None}])

