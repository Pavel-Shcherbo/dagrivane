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


def atomic_create(path, payload):
    """Publish a complete file without replacing an existing destination."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".dagrivane-object-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class ObjectStore:
    pass
