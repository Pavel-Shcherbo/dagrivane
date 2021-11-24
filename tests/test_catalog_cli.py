import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from catalog_fixtures import contract, gates, write_bundle

ROOT = Path(__file__).resolve().parents[1]


class CatalogCliTests(unittest.TestCase):
    pass
