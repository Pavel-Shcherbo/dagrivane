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


