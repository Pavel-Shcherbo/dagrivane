import json
import sys
import uuid
from shutil import copy2

from .engine import run
from .httpapp import serve
from .resources import examples_directory, state_directory


def example():
    return json.loads((examples_directory() / "pipeline.json").read_text())


def evaluate(params):
    spec = example()
    for task in spec["tasks"]:
        if task["command"][0] == "python3":
            task["command"][0] = sys.executable
    if params.get("fail_quality"):
        spec["tasks"][1]["command"] = [
            sys.executable,
            "-c",
            "raise SystemExit('quality gate rejected: injected failure')",
        ]
    run_id = str(uuid.uuid4())
    state = state_directory("dagrivane")
    inputs = state / "demo-inputs" / run_id
    inputs.mkdir(parents=True)
    for name in ["orders.csv", "tasks.py"]:
        copy2(examples_directory() / name, inputs / name)
    tasks = run(spec, state / "runs.sqlite", run_id, inputs)
    return {
        "run_id": run_id,
        "tasks": tasks,
        "succeeded": all(x["status"] == "succeeded" for x in tasks),
    }


if __name__ == "__main__":
    serve(evaluate, example, 8411)
