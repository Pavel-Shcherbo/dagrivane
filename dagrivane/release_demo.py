"""An executable local data-to-application release control with retained evidence."""

import argparse
import hashlib
import json
import os
import subprocess
import sqlite3
import sys
import zipfile
from pathlib import Path
from shutil import copy2

from .catalog import Catalog
from .catalog_contracts import CatalogError
from .dataflow import compile_plan
from .release_bundle import export_release
from .resources import examples_directory

SCENARIOS = ("healthy", "corrupt-data", "missing-staging")


def build_application(source, destination):
    content = Path(source).read_bytes()
    info = zipfile.ZipInfo("__main__.py", date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    with zipfile.ZipFile(
        destination, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        archive.writestr(info, content)
    return hashlib.sha256(content).hexdigest()


