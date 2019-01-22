"""Verify declared workflow inputs before using saved checkpoints."""

import hashlib
import re
from pathlib import Path


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def verify_inputs(spec, cwd):
    declarations = spec.get("inputs", [])
    if not isinstance(declarations, list):
        raise ValueError("inputs must be a list of path and sha256 declarations")
    root = Path(cwd).resolve()
    seen, verified = set(), []
    for item in declarations:
        if not isinstance(item, dict):
            raise ValueError("each input declaration must be an object")
        name, expected = item.get("path"), item.get("sha256")
        if not isinstance(name, str) or not name or Path(name).is_absolute():
            raise ValueError("input paths must be relative to the workflow directory")
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError(
                "input sha256 must contain 64 lowercase hexadecimal characters"
            )
        path = (root / name).resolve()
        if not path.is_relative_to(root):
            raise ValueError("input path escapes the workflow directory")
        if path in seen:
            raise ValueError("duplicate input declaration")
        seen.add(path)
        if not path.is_file():
            raise ValueError(f"declared input is not a file: {name}")
        actual = file_digest(path)
        if actual != expected:
            raise ValueError(
                f"input content changed: {name}; inspect the change and use an updated declaration with a new run ID"
            )
        verified.append({"path": name, "sha256": actual})
    return verified
