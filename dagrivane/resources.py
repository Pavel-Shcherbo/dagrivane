"""Locate read-only examples and a separate writable local state directory."""

import os
import argparse
import json
import re
from shutil import copy2
from pathlib import Path


def examples_directory():
    package = Path(__file__).resolve().parent
    bundled = package / "_examples"
    source = package.parent / "examples"
    if bundled.is_dir():
        return bundled
    if source.is_dir():
        return source
    raise FileNotFoundError(
        "Bundled examples are unavailable; reinstall the package or use a complete source checkout"
    )


def state_directory(product):
    if not isinstance(product, str) or not re.fullmatch(
        r"[a-z][a-z0-9_-]{0,63}", product
    ):
        raise ValueError("product must be a simple lowercase identifier")
    configured = os.environ.get(product.upper() + "_STATE_DIR")
    path = (
        Path(configured).expanduser() if configured else Path.cwd() / ".local" / product
    )
    path = path.resolve()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def export_examples(destination):
    requested = Path(destination).expanduser()
    if requested.is_symlink():
        raise ValueError("example destination must not be a symbolic link")
    destination = requested.resolve()
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    copied = []
    for source in sorted(examples_directory().iterdir()):
        if source.is_file() and source.suffix in {".py", ".csv", ".json"}:
            copy2(source, destination / source.name)
            copied.append(source.name)
    return {"directory": str(destination), "files": copied}


