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


