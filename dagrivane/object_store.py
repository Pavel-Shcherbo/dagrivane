"""Local content-addressed objects with verified, create-only publication."""

import hashlib
import os
import re
import tempfile
from pathlib import Path

MAX_OBJECT_BYTES = 64 * 1024 * 1024
DIGEST = re.compile(r"^[0-9a-f]{64}$")


class ObjectError(ValueError):
    pass


def digest_name(value):
    if not isinstance(value, str) or not DIGEST.fullmatch(value):
        raise ObjectError("object identity must be lowercase SHA-256")
    return value


def bounded_file(path, maximum=MAX_OBJECT_BYTES):
    path = Path(path)
    if not path.is_file():
        raise ObjectError("object input must be a regular file")
    with path.open("rb") as stream:
        payload = stream.read(maximum + 1)
    if len(payload) > maximum:
        raise ObjectError("object exceeds the supported byte budget")
    return payload


