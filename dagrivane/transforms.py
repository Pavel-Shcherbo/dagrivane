"""Schema-aware relational operations with bounded cardinality and typed values."""

import copy
from dataclasses import dataclass
from decimal import Decimal, localcontext, ROUND_HALF_EVEN

from .contracts import Column, Contract, convert, identifier
from .tables import Table, MAX_ROWS, MAX_CELLS


def fields(definition, allowed, required=()):
    if (
        not isinstance(definition, dict)
        or set(definition) - set(allowed)
        or not set(required) <= set(definition)
    ):
        raise ValueError("operation contains unsupported or missing fields")


def names(value, contract, *, nonempty=True):
    if (
        not isinstance(value, list)
        or nonempty
        and not value
        or any(not isinstance(item, str) for item in value)
    ):
        raise ValueError("columns must be a list of names")
    if len(set(value)) != len(value):
        raise ValueError("columns must be unique")
    for name in value:
        contract.column(name)
    return tuple(value)


def output_contract(previous, columns):
    return Contract.compile(
        {
            "name": previous.name,
            "version": previous.version,
            "columns": [column.definition() for column in columns],
        }
    )


def widened(column, *, name=None, nullable=None):
    definition = column.definition()
    if name is not None:
        definition["name"] = name
    if nullable is not None:
        definition["nullable"] = nullable
    return Column.compile(definition)


def operand(definition, contract, kind):
    if not isinstance(definition, dict) or len(definition) != 1:
        raise ValueError("operand requires exactly one column or literal")
    if "column" in definition:
        column = contract.column(definition["column"])
        if column.kind != kind and not (kind == "decimal" and column.kind == "integer"):
            raise ValueError("operand column has an incompatible type")
        return ("column", column.name)
    if "literal" in definition:
        value = definition["literal"]
        return ("literal", None if value is None else convert(value, kind))
    raise ValueError("unsupported operand")


def value_of(expression, row):
    return row[expression[1]] if expression[0] == "column" else expression[1]


@dataclass(frozen=True)
class Operation:
    kind: str
    contract: Contract
    config: dict

    def apply(self, table, right=None):
        rows = list(table.rows())
        config = self.config
        if self.kind == "select":
            selected = [
                {
                    config["rename"].get(name, name): row[name]
                    for name in config["columns"]
                }
                for row in rows
            ]
        elif self.kind == "filter":
            selected = [
                row
                for row in rows
                if all(
                    condition_matches(row, condition)
                    for condition in config["conditions"]
                )
            ]
        elif self.kind == "calculate":
            selected = (
                {**row, config["column"].name: calculate(config["expression"], row)}
                for row in rows
            )
        elif self.kind == "aggregate":
            selected = aggregate(rows, config)
        elif self.kind == "join":
            if not isinstance(right, Table):
                raise ValueError("join requires a right-hand table")
            selected = join(
                rows, list(right.rows()), config, len(self.contract.columns)
            )
        elif self.kind == "sort":
            selected = rows
            for key in reversed(config["keys"]):
                name, descending, nulls_first = key
                present = [row for row in selected if row[name] is not None]
                missing = [row for row in selected if row[name] is None]
                present.sort(key=lambda row: row[name], reverse=descending)
                selected = missing + present if nulls_first else present + missing
        elif self.kind == "distinct":
            selected, seen = [], set()
            for row in rows:
                key = tuple(row[name] for name in config["columns"])
                if key not in seen:
                    selected.append(row)
                    seen.add(key)
        elif self.kind == "limit":
            selected = rows[config["offset"] : config["offset"] + config["count"]]
        else:
            raise ValueError("uncompiled operation")
        sources = tuple(
            dict.fromkeys(table.sources + (right.sources if right is not None else ()))
        )
        return Table.from_rows(self.contract, selected, sources)


def condition_matches(row, condition):
    column, operation, expected = condition
    actual = row[column]
    if operation == "is_null":
        return actual is None
    if operation == "not_null":
        return actual is not None
    if actual is None:
        return False
    if operation == "eq":
        return actual == expected
    if operation == "ne":
        return actual != expected
    if operation == "gt":
        return actual > expected
    if operation == "gte":
        return actual >= expected
    if operation == "lt":
        return actual < expected
    if operation == "lte":
        return actual <= expected
    if operation == "in":
        return actual in expected
    if operation == "not_in":
        return actual not in expected
    if operation == "starts_with":
        return actual.startswith(expected)
    return expected in actual


def calculate(expression, row):
    operation, arguments, places = expression
    values = [value_of(argument, row) for argument in arguments]
    if operation == "coalesce":
        return next((value for value in values if value is not None), None)
    if any(value is None for value in values):
        return None
    if operation == "concat":
        return "".join(values)
    if operation in ("lower", "upper", "trim"):
        return {"lower": str.lower, "upper": str.upper, "trim": str.strip}[operation](
            values[0]
        )
    with localcontext() as context:
        context.prec = 80
        if operation == "add":
            return values[0] + values[1]
        if operation == "subtract":
            return values[0] - values[1]
        if operation == "multiply":
            return values[0] * values[1]
        if operation == "divide":
            if values[1] == 0:
                raise ValueError("calculated division by zero")
            return (Decimal(values[0]) / Decimal(values[1])).quantize(
                Decimal(1).scaleb(-places), rounding=ROUND_HALF_EVEN
            )
        if operation == "round":
            return Decimal(values[0]).quantize(
                Decimal(1).scaleb(-places), rounding=ROUND_HALF_EVEN
            )
    raise ValueError("unsupported calculation")


def aggregate(rows, config):
    groups = {}
    if not config["group_by"]:
        groups[()] = []
    for row in rows:
        key = tuple(row[name] for name in config["group_by"])
        groups.setdefault(key, []).append(row)
    output = []
    for key, members in groups.items():
        result = dict(zip(config["group_by"], key))
        for name, operation, column in config["metrics"]:
            values = (
                [row[column] for row in members if row[column] is not None]
                if column
                else []
            )
            if operation == "count":
                value = len(values) if column else len(members)
            elif not values:
                value = None
            elif operation in ("sum", "avg"):
                with localcontext() as context:
                    context.prec = 80
                    value = sum(values)
                    if operation == "avg":
                        value = Decimal(value) / Decimal(len(values))
                        value = value.quantize(
                            Decimal("0.000000000000000001"), rounding=ROUND_HALF_EVEN
                        )
            elif operation == "min":
                value = min(values)
            else:
                value = max(values)
            result[name] = value
        output.append(result)
    return output


def join(left, right, config, width):
    indexed = {}
    for row in right:
        key = tuple(row[name] for name in config["right_on"])
        if any(value is None for value in key):
            continue
        indexed.setdefault(key, []).append(row)
    if config["cardinality"] in ("many_to_one", "one_to_one") and any(
        len(values) > 1 for values in indexed.values()
    ):
        raise ValueError("join right keys violate declared cardinality")
    seen, output = set(), []
    for row in left:
        key = tuple(row[name] for name in config["left_on"])
        nonnull = not any(value is None for value in key)
        if nonnull and config["cardinality"] == "one_to_one" and key in seen:
            raise ValueError("join left keys violate one_to_one cardinality")
        seen.add(key)
        matches = indexed.get(key, []) if nonnull else []
        if not matches and config["how"] == "left":
            matches = [None]
        for match in matches:
            output.append(
                {
                    **row,
                    **{
                        target: match[source] if match is not None else None
                        for source, target in config["right_columns"]
                    },
                }
            )
            if len(output) > MAX_ROWS or len(output) * width > MAX_CELLS:
                raise ValueError("join exceeds output row or cell budget")
    return output


def compile_operation(definition, contract, right=None):
    if not isinstance(definition, dict):
        raise ValueError("operation must be an object")
    kind = definition.get("op")
    config = {}
    columns = list(contract.columns)
    base = {"id", "input", "op"}
    if kind == "select":
        fields(definition, base | {"columns", "rename"}, ("columns",))
        selected = names(definition["columns"], contract)
        rename = definition.get("rename", {})
        if not isinstance(rename, dict) or set(rename) - set(selected):
            raise ValueError("rename keys must be selected columns")
        for target in rename.values():
            identifier(target, "renamed column")
        columns = [
            widened(contract.column(name), name=rename.get(name, name))
            for name in selected
        ]
        config = {"columns": selected, "rename": dict(rename)}
    elif kind == "filter":
        fields(definition, base | {"conditions"}, ("conditions",))
        conditions = definition["conditions"]
        if not isinstance(conditions, list) or not 1 <= len(conditions) <= 100:
            raise ValueError("filter requires 1–100 conditions")
        compiled = []
        for condition in conditions:
            fields(condition, {"column", "op", "value"}, ("column", "op"))
            column = contract.column(condition["column"])
            operation = condition["op"]
            if operation not in (
                "eq",
                "ne",
                "gt",
                "gte",
                "lt",
                "lte",
                "in",
                "not_in",
                "is_null",
                "not_null",
                "starts_with",
                "contains",
            ):
                raise ValueError("unsupported filter operator")
            expected = None
            if operation in ("is_null", "not_null"):
                if "value" in condition:
                    raise ValueError("null predicates do not take a value")
            else:
                if "value" not in condition or condition["value"] is None:
                    raise ValueError("filter requires a non-null value")
                if operation in ("starts_with", "contains") and column.kind != "string":
                    raise ValueError("text predicates require a string column")
                if operation in ("gt", "gte", "lt", "lte") and column.kind == "boolean":
                    raise ValueError("ordered predicates do not accept booleans")
                value = condition["value"]
                if operation in ("in", "not_in"):
                    if (
                        not isinstance(value, list)
                        or not 1 <= len(value) <= 1000
                        or any(item is None for item in value)
                    ):
                        raise ValueError("membership requires 1–1000 non-null values")
                    expected = tuple(convert(item, column.kind) for item in value)
                else:
                    expected = convert(value, column.kind)
            compiled.append((column.name, operation, expected))
        config = {"conditions": tuple(compiled)}
    elif kind == "calculate":
        fields(definition, base | {"column", "expression"}, ("column", "expression"))
        column = Column.compile(definition["column"])
        if column.name in contract.names:
            raise ValueError("calculated column cannot overwrite an existing column")
        expression = definition["expression"]
        fields(expression, {"op", "args", "places"}, ("op", "args"))
        operation, args = expression["op"], expression["args"]
        if not isinstance(args, list):
            raise ValueError("calculation args must be a list")
        if operation in ("add", "subtract", "multiply", "divide"):
            if (
                len(args) != 2
                or column.kind not in ("integer", "decimal")
                or operation == "divide"
                and column.kind != "decimal"
            ):
                raise ValueError("invalid numeric calculation signature")
        elif operation in ("lower", "upper", "trim"):
            if len(args) != 1 or column.kind != "string":
                raise ValueError("invalid text calculation signature")
        elif operation == "concat":
            if not 1 <= len(args) <= 16 or column.kind != "string":
                raise ValueError("concat requires 1–16 string operands")
        elif operation == "coalesce":
            if not 1 <= len(args) <= 16:
                raise ValueError("coalesce requires 1–16 operands")
        elif operation == "round":
            if len(args) != 1 or column.kind != "decimal":
                raise ValueError("round requires one decimal operand")
        else:
            raise ValueError("unsupported calculation")
        places = expression.get("places", 2)
        if "places" in expression and operation not in ("round", "divide"):
            raise ValueError("places is only supported by round and divide")
        if type(places) is not int or not 0 <= places <= 18:
            raise ValueError("places must be between 0 and 18")
        columns.append(column)
        config = {
            "column": column,
            "expression": (
                operation,
                tuple(operand(arg, contract, column.kind) for arg in args),
                places,
            ),
        }
    elif kind == "aggregate":
        fields(definition, base | {"group_by", "metrics"}, ("group_by", "metrics"))
        group_by = names(definition["group_by"], contract, nonempty=False)
        metrics = definition["metrics"]
        if not isinstance(metrics, list) or not 1 <= len(metrics) <= 100:
            raise ValueError("aggregate requires 1–100 metrics")
        columns = [contract.column(name) for name in group_by]
        compiled = []
        for metric in metrics:
            fields(metric, {"name", "op", "column"}, ("name", "op"))
            name = identifier(metric["name"], "metric name")
            operation = metric["op"]
            source = contract.column(metric["column"]) if "column" in metric else None
            if (
                operation not in ("sum", "avg", "min", "max", "count")
                or source is None
                and operation != "count"
            ):
                raise ValueError("invalid aggregate signature")
            if operation in ("sum", "avg") and source.kind not in (
                "integer",
                "decimal",
            ):
                raise ValueError("sum and avg require numeric columns")
            dtype = (
                "integer"
                if operation == "count"
                else "decimal" if operation == "avg" else source.kind
            )
            columns.append(
                Column.compile(
                    {"name": name, "type": dtype, "nullable": operation != "count"}
                )
            )
            compiled.append((name, operation, source.name if source else None))
        config = {"group_by": group_by, "metrics": tuple(compiled)}
    elif kind == "join":
        fields(
            definition,
            base
            | {"right", "left_on", "right_on", "right_columns", "how", "cardinality"},
            ("right", "left_on", "right_on", "right_columns"),
        )
        if not isinstance(right, Contract):
            raise ValueError("join requires a right-hand contract")
        left_on = names(definition["left_on"], contract)
        right_on = names(definition["right_on"], right)
        if len(left_on) != len(right_on) or any(
            contract.column(a).kind != right.column(b).kind
            for a, b in zip(left_on, right_on)
        ):
            raise ValueError("join keys must have matching arity and types")
        how, cardinality = definition.get("how", "inner"), definition.get(
            "cardinality", "many_to_one"
        )
        if how not in ("inner", "left") or cardinality not in (
            "many_to_one",
            "one_to_one",
            "many_to_many",
        ):
            raise ValueError("unsupported join mode or cardinality")
        selected = definition["right_columns"]
        if not isinstance(selected, dict) or not selected:
            raise ValueError("right_columns maps source columns to new target names")
        for source, target in selected.items():
            identifier(target, "join target")
            column = right.column(source)
            columns.append(
                widened(column, name=target, nullable=column.nullable or how == "left")
            )
        config = {
            "left_on": left_on,
            "right_on": right_on,
            "right_columns": tuple(selected.items()),
            "how": how,
            "cardinality": cardinality,
        }
    elif kind == "sort":
        fields(definition, base | {"keys"}, ("keys",))
        keys = definition["keys"]
        if not isinstance(keys, list) or not 1 <= len(keys) <= 16:
            raise ValueError("sort requires 1–16 keys")
        compiled = []
        for key in keys:
            fields(key, {"column", "descending", "nulls_first"}, ("column",))
            contract.column(key["column"])
            descending, nulls_first = key.get("descending", False), key.get(
                "nulls_first", False
            )
            if type(descending) is not bool or type(nulls_first) is not bool:
                raise ValueError("sort flags must be boolean")
            compiled.append((key["column"], descending, nulls_first))
        if len({item[0] for item in compiled}) != len(compiled):
            raise ValueError("sort columns must be unique")
        config = {"keys": tuple(compiled)}
    elif kind == "distinct":
        fields(definition, base | {"columns"}, ("columns",))
        config = {"columns": names(definition["columns"], contract)}
    elif kind == "limit":
        fields(definition, base | {"count", "offset"}, ("count",))
        count, offset = definition["count"], definition.get("offset", 0)
        if any(
            type(value) is not int or not 0 <= value <= MAX_ROWS
            for value in (count, offset)
        ):
            raise ValueError(
                "limit count and offset must be bounded nonnegative integers"
            )
        config = {"count": count, "offset": offset}
    else:
        raise ValueError("unsupported table operation")
    return Operation(kind, output_contract(contract, columns), copy.deepcopy(config))
