"""Run built-in dataflow tasks through the same process boundary as command tasks."""

import json
import sys
import tempfile
from pathlib import Path

from .dataflow import compile_plan


def validate_data_task(definition):
    compile_plan(definition)


def execute_data_task(definition, cwd, timeout, should_cancel, execute):
    # A fixed bootstrap pins imports to this installation even when a source
    # checkout executes a plan from an unrelated workspace directory.
    package_root = str(Path(__file__).resolve().parent.parent)
    bootstrap = (
        "import sys; sys.path.insert(0,sys.argv[1]); "
        "from dagrivane.dataflow import main; raise SystemExit(main(sys.argv[2:]))"
    )
    with tempfile.TemporaryDirectory(prefix="dagrivane-data-task-") as temporary:
        plan = Path(temporary) / "plan.json"
        plan.write_text(json.dumps(definition, allow_nan=False))
        command = [
            sys.executable,
            "-c",
            bootstrap,
            package_root,
            str(plan),
            "--root",
            str(cwd),
        ]
        return execute(command, cwd, timeout, should_cancel)
