from dataclasses import FrozenInstanceError
import hashlib
import tempfile
import unittest
from pathlib import Path

from dagrivane.contracts import Contract
from dagrivane.tables import Table, read_csv, local_path


class TableTests(unittest.TestCase):
    pass
