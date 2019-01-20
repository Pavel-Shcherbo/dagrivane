import argparse
import json
from pathlib import Path

from .engine import run


def main():
    parser = argparse.ArgumentParser(description="Run a trusted local command graph")
    parser.add_argument("spec", type=Path)
    parser.add_argument("--state", default=".local/runs.sqlite")
    parser.add_argument("--run-id", default="demo")
    args = parser.parse_args()
    try:
        report = run(
            json.loads(args.spec.read_text()),
            args.state,
            args.run_id,
            args.spec.resolve().parent,
        )
    except (ValueError, OSError) as error:
        parser.exit(2, f"{error}\n")
    print(json.dumps(report, indent=2))
    return 0 if all(task["status"] == "succeeded" for task in report) else 1


raise SystemExit(main())
