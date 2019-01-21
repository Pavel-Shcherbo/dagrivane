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


