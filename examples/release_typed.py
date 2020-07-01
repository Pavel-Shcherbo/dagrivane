"""Check the bundled order release against its validated source identities."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path


def main():
    validated = json.loads(Path(".local/typed-orders.json").read_text())
    summary = json.loads(Path(".local/typed-summary.json").read_text())
    source = hashlib.sha256(Path("orders.csv").read_bytes()).hexdigest()
    checks = {
        "source_quality": validated["report"]["passed"],
        "summary_quality": summary["report"]["passed"],
        "same_pinned_source": validated["report"]["output"]["source_sha256"]
        == summary["report"]["output"]["source_sha256"]
        == [source],
        "source_row_count": len(validated["rows"]) == 4,
        "currency_summary": len(summary["rows"]) == 1
        and summary["rows"][0]["currency"] == "USD"
        and Decimal(summary["rows"][0]["revenue"]) == Decimal("112.00")
        and summary["rows"][0]["orders"] == 4,
    }
    print(json.dumps({"passed": all(checks.values()), "checks": checks}, indent=2))
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
