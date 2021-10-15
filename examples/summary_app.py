"""A small standalone application consuming a verified order-summary bundle."""

import argparse
import json
from decimal import Decimal
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("table", type=Path)
    args = parser.parse_args()
    bundle = json.loads(args.table.read_text())
    if (
        bundle.get("format") != "dagrivane.table.v1"
        or bundle["report"].get("passed") is not True
    ):
        raise ValueError("expected a successful typed table bundle")
    currencies, orders = {}, 0
    for row in bundle["rows"]:
        if (
            set(row) != {"currency", "revenue", "orders"}
            or type(row["orders"]) is not int
            or row["orders"] < 0
        ):
            raise ValueError("unexpected order summary fields")
        amount = Decimal(row["revenue"])
        if not amount.is_finite() or amount < 0 or row["currency"] in currencies:
            raise ValueError("invalid or duplicate currency summary")
        currencies[row["currency"]] = format(amount, "f")
        orders += row["orders"]
    print(json.dumps({"currencies": currencies, "orders": orders}, sort_keys=True))


if __name__ == "__main__":
    main()
