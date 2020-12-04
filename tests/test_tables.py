from dataclasses import FrozenInstanceError
import hashlib
import tempfile
import unittest
from pathlib import Path

from dagrivane.contracts import Contract
from dagrivane.tables import Table, read_csv, local_path


class TableTests(unittest.TestCase):
    def setUp(self):
        self.schema = Contract.compile(
            {
                "name": "orders",
                "version": 1,
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "amount", "type": "decimal"},
                ],
            }
        )

    def test_csv_hash_pin_and_typed_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "orders.csv"
            payload = b"id,amount\r\n1,0.10\r\n2,0.20\r\n"
            path.write_bytes(payload)
            table = read_csv(
                path, self.schema, expected_sha256=hashlib.sha256(payload).hexdigest()
            )
            self.assertEqual(
                table.json_rows(),
                [{"id": 1, "amount": "0.10"}, {"id": 2, "amount": "0.20"}],
            )
            with self.assertRaisesRegex(ValueError, "pinned hash"):
                read_csv(path, self.schema, expected_sha256="0" * 64)

