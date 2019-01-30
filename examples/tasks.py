"""Small, hand-checkable synthetic order pipeline."""

import csv
import json
import sys
from decimal import Decimal
from pathlib import Path

root = Path(".local")
root.mkdir(exist_ok=True)
action = sys.argv[1]
if action == "extract":
    with open("orders.csv", newline="") as stream:
        rows = list(csv.DictReader(stream))
    (root / "orders.json").write_text(json.dumps(rows))
    print(f"Extracted {len(rows)} synthetic orders")
elif action == "quality":
    rows = json.loads((root / "orders.json").read_text())
    if not rows or len({row["id"] for row in rows}) != len(rows):
        raise ValueError("order IDs must be non-empty and unique")
    if any(
        not row["id"] or row["currency"] != "USD" or Decimal(row["amount"]) < 0
        for row in rows
    ):
        raise ValueError("unexpected order values")
    print("Verified IDs, currency and amounts")
elif action == "aggregate":
    rows = json.loads((root / "orders.json").read_text())
    total = sum((Decimal(row["amount"]) for row in rows), Decimal(0))
    (root / "summary.json").write_text(
        json.dumps({"orders": len(rows), "total": str(total), "currency": "USD"})
    )
    print(f"Wrote aggregate: USD {total}")
elif action == "release-check":
    summary = json.loads((root / "summary.json").read_text())
    if summary != {"orders": 4, "total": "112.00", "currency": "USD"}:
        raise ValueError("aggregate differs from the control fixture")
    print("Release gate passed: aggregate matches control fixture")
else:
    raise ValueError("unknown task")
