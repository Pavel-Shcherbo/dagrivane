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


