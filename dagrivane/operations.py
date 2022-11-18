"""Durable local order-workflow operations over reviewed installation templates."""

import copy
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

from .catalog_contracts import table_bundle
from .control import ControlStore, Conflict, canonical
from .quality import compile_gates
from .resources import examples_directory
from .worker import Worker

PROFILES = {
    "standard": "Order checks",
    "quality-failure": "Quality failure control",
    "cancellation": "Cancellation control",
}


class Operations:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.database = self.root / "control.sqlite"
        self.worker_root = self.root / "worker"

    def _reviewed(self, prepare=False):
        examples = examples_directory()
        files = {}
        for name in ["orders.csv", "release_typed.py"]:
            with (examples / name).open("rb") as stream:
                raw = stream.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError("reviewed input exceeds its byte budget")
            files[name] = raw
        base = json.loads((examples / "typed-pipeline.json").read_text())
        base["execution_policy"] = "isolated_inputs"
        base["inputs"] = [
            {"path": name, "sha256": hashlib.sha256(raw).hexdigest()}
            for name, raw in sorted(files.items())
        ]
        for task in base["tasks"]:
            if "command" in task and task["command"][0] == "python3":
                task["command"][0] = sys.executable
        source_id = hashlib.sha256(
            canonical({"specification": base, "inputs": base["inputs"]}).encode()
        ).hexdigest()
        directory = self.root / "templates" / source_id
        if directory.is_symlink():
            raise ValueError("reviewed template cannot be a symlink")
        if prepare:
            directory.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not directory.exists():
                temporary = Path(
                    tempfile.mkdtemp(prefix=".inputs-", dir=directory.parent)
                )
                try:
                    for name, raw in files.items():
                        (temporary / name).write_bytes(raw)
                    (temporary / "manifest.json").write_text(canonical(base["inputs"]))
                    try:
                        os.rename(temporary, directory)
                    except OSError:
                        if not directory.is_dir() or directory.is_symlink():
                            raise
                finally:
                    shutil.rmtree(temporary, ignore_errors=True)
            for name, raw in files.items():
                path = directory / name
                if path.is_symlink() or not path.is_file() or path.read_bytes() != raw:
                    raise ValueError("reviewed template content changed")
        result = {}
        for profile, label in PROFILES.items():
            spec = copy.deepcopy(base)
            if profile == "quality-failure":
                spec["tasks"][0]["dataflow"]["gates"][1].update(min=5, max=5)
            elif profile == "cancellation":
                spec["tasks"].insert(
                    0,
                    {
                        "id": "pause",
                        "command": [sys.executable, "-c", "import time; time.sleep(4)"],
                        "timeout": 10,
                    },
                )
                spec["tasks"][1]["needs"] = ["pause"]
            fingerprint = hashlib.sha256(
                canonical({"specification": spec, "directory": str(directory)}).encode()
            ).hexdigest()
            result[profile] = {
                "profile": profile,
                "label": label,
                "specification": spec,
                "directory": str(directory),
                "fingerprint": fingerprint,
            }
        return result

