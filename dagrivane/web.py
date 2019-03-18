import json
import sys
import threading
import uuid
from pathlib import Path

from .engine import run
from .httpapp import serve

ROOT = Path(__file__).resolve().parents[1]
RUN_LOCK = threading.Lock()


def example():
    return json.loads((ROOT / "examples/pipeline.json").read_text())


def evaluate(params):
    spec = example()
    if params.get("fail_quality"):
        spec["tasks"][1]["command"] = [
            sys.executable,
            "-c",
            "raise SystemExit('quality gate rejected: injected failure')",
        ]
    run_id = str(uuid.uuid4())
    with RUN_LOCK:
        tasks = run(spec, ROOT / ".local/runs.sqlite", run_id, ROOT / "examples")
    return {
        "run_id": run_id,
        "tasks": tasks,
        "succeeded": all(x["status"] == "succeeded" for x in tasks),
    }


if __name__ == "__main__":
    serve(evaluate, example, 8411)
