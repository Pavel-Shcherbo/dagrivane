"""Clone bounded pinned inputs into a new worker-owned execution directory."""

import hashlib
import re
import shutil
from pathlib import Path


def create_run_workspace(specification, source, destination):
    source = Path(source).resolve()
    destination = Path(destination)
    declarations = specification.get("inputs", [])
    if not isinstance(declarations, list) or len(declarations) > 64:
        raise ValueError("isolated execution supports at most 64 pinned input files")
    files = []
    names = set()
    total = 0
    for item in declarations:
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            raise ValueError("isolated input requires exactly path and sha256")
        raw_name = item["path"]
        expected = item["sha256"]
        if (
            not isinstance(raw_name, str)
            or not raw_name
            or len(raw_name) > 512
            or "\\" in raw_name
            or "\x00" in raw_name
        ):
            raise ValueError(
                "isolated input path must use bounded relative forward-slash text"
            )
        relative = Path(raw_name)
        if relative.is_absolute() or ".." in relative.parts or relative == Path("."):
            raise ValueError("isolated input must stay inside the source directory")
        name = str(relative)
        key = name.casefold()
        if key in names:
            raise ValueError("isolated input paths must be unique ignoring case")
        names.add(key)
        if not isinstance(expected, str) or not re.fullmatch("[0-9a-f]{64}", expected):
            raise ValueError("isolated input requires a lowercase SHA-256")
        lexical = source / relative
        if any(
            part.is_symlink()
            for part in [lexical, *lexical.parents]
            if part.is_relative_to(source)
        ):
            raise ValueError("isolated input symlinks are unsupported")
        path = lexical.resolve()
        if not path.is_relative_to(source) or not path.is_file():
            raise ValueError("isolated input is missing or outside its source")
        with path.open("rb") as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        total += len(raw)
        if len(raw) > 4 * 1024 * 1024 or total > 16 * 1024 * 1024:
            raise ValueError("isolated inputs exceed per-file or combined byte budget")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("isolated input differs from its pinned SHA-256: " + name)
        files.append((name, raw, expected))
    if any(
        left != right and Path(left) in Path(right).parents
        for left in names
        for right in names
    ):
        raise ValueError("isolated file paths overlap as file and directory")
    if destination.is_symlink():
        raise ValueError("run workspace destination cannot be a symbolic link")
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    try:
        for name, raw, _ in files:
            target = destination / name
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with target.open("xb") as stream:
                stream.write(raw)
    except BaseException:
        shutil.rmtree(destination)
        raise
    return {
        "directory": str(destination.resolve()),
        "inputs": [{"path": name, "sha256": sha} for name, _, sha in files],
        "bytes": total,
    }
