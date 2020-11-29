"""Compare integer relational behavior with an independent SQLite implementation."""

import random
import sqlite3
import unittest

from dagrivane.contracts import Contract
from dagrivane.tables import Table
from dagrivane.transforms import compile_operation


class RelationalOracleTests(unittest.TestCase):
    def test_seeded_left_join_and_grouped_counts_match_sql_null_semantics(self):
        randomizer = random.Random(44784481)
        left_schema = Contract.compile(
            {
                "name": "events",
                "version": 1,
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "key", "type": "integer", "nullable": True},
                ],
            }
        )
        right_schema = Contract.compile(
            {
                "name": "labels",
                "version": 1,
                "columns": [
                    {"name": "key", "type": "integer", "nullable": True},
                    {"name": "value", "type": "integer"},
                ],
            }
        )
        for _ in range(15):
            left_rows = [
                {"id": i, "key": randomizer.choice([None, 0, 1, 2, 3])}
                for i in range(25)
            ]
            right_rows = [
                {
                    "key": randomizer.choice([None, 0, 1, 2]),
                    "value": randomizer.randrange(-10, 11),
                }
                for _ in range(8)
            ]
            left, right = Table.from_rows(left_schema, left_rows), Table.from_rows(
                right_schema, right_rows
            )
            join = compile_operation(
                {
                    "op": "join",
                    "right": "labels",
                    "left_on": ["key"],
                    "right_on": ["key"],
                    "right_columns": {"value": "value"},
                    "how": "left",
                    "cardinality": "many_to_many",
                },
                left_schema,
                right_schema,
            )
            result = join.apply(left, right)
            aggregate = compile_operation(
                {
                    "op": "aggregate",
                    "group_by": ["key"],
                    "metrics": [
                        {"name": "n", "op": "count"},
                        {"name": "present", "op": "count", "column": "value"},
                        {"name": "total", "op": "sum", "column": "value"},
                    ],
                },
                result.contract,
            )
            actual = [
                tuple(row[name] for name in ("key", "n", "present", "total"))
                for row in aggregate.apply(result).rows()
            ]
            with sqlite3.connect(":memory:") as database:
                database.execute("create table events(id integer, key integer)")
                database.execute("create table labels(key integer, value integer)")
                database.executemany(
                    "insert into events values (?,?)",
                    [(row["id"], row["key"]) for row in left_rows],
                )
                database.executemany(
                    "insert into labels values (?,?)",
                    [(row["key"], row["value"]) for row in right_rows],
                )
                expected = database.execute(
                    "select events.key, count(*), count(labels.value), sum(labels.value) from events left join labels on events.key=labels.key group by events.key"
                ).fetchall()
            self.assertCountEqual(actual, expected)

    def test_many_to_many_growth_is_bounded(self):
        schema = Contract.compile(
            {
                "name": "rows",
                "version": 1,
                "columns": [{"name": "key", "type": "integer"}],
            }
        )
        table = Table.from_rows(schema, [{"key": 1}] * 317)
        operation = compile_operation(
            {
                "op": "join",
                "right": "rows",
                "left_on": ["key"],
                "right_on": ["key"],
                "right_columns": {"key": "right_key"},
                "cardinality": "many_to_many",
            },
            schema,
            schema,
        )
        with self.assertRaisesRegex(ValueError, "budget"):
            operation.apply(table, table)
