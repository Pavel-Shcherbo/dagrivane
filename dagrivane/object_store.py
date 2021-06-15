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
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def path(self, identity):
        digest_name(identity)
        target = self.root / identity[:2] / identity[2:]
        if not target.resolve().is_relative_to(self.root):
            raise ObjectError("object storage path escapes its root")
        for component in (target, target.parent):
            if component.is_symlink():
                raise ObjectError("object storage symlinks are unsupported")
        return target

    def put(self, payload):
        if not isinstance(payload, bytes) or len(payload) > MAX_OBJECT_BYTES:
            raise ObjectError("object must be bytes within the 64 MiB budget")
        identity = hashlib.sha256(payload).hexdigest()
        target = self.path(identity)
        target.parent.mkdir(mode=0o700, exist_ok=True)
        try:
            atomic_create(target, payload)
        except FileExistsError:
            # A racing identical publication is safe only after reading the
            # winner; an existing corrupt object must never be silently reused.
            existing = self.get(identity)
            if existing != payload:
                raise ObjectError("stored object differs despite its declared digest")
        return {"sha256": identity, "bytes": len(payload)}

    def get(self, identity):
        target = self.path(identity)
        payload = bounded_file(target)
        if hashlib.sha256(payload).hexdigest() != identity:
            raise ObjectError("stored object failed SHA-256 verification")
        return payload

