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

    def test_invalid_csv_is_rejected_with_record_context(self):
        for text in [
            "amount,id\n1,2\n",
            "id,amount\n1\n",
            "id,amount\n1,nope\n",
            "id,amount\n1,2,3\n",
            'id,amount\n"1,2\n',
            "id,amount\n1,\x00\n",
        ]:
            with tempfile.TemporaryDirectory() as folder, self.subTest(text=text):
                path = Path(folder) / "input.csv"
                path.write_text(text)
                with self.assertRaises(ValueError):
                    read_csv(path, self.schema)

    def test_table_is_immutable_and_identity_covers_schema_and_order(self):
        rows = [{"id": 1, "amount": "1"}, {"id": 2, "amount": "2"}]
        table = Table.from_rows(self.schema, rows)
        identity = table.digest()
        rows[0]["id"] = 99
        next(table.rows())["id"] = 100
        self.assertEqual(table.digest(), identity)
        reversed_table = Table.from_rows(self.schema, list(reversed(table.json_rows())))
        self.assertNotEqual(reversed_table.digest(), identity)
        with self.assertRaises(FrozenInstanceError):
            table.records = ()

    def test_paths_cannot_escape_including_symlinks(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            tempfile.TemporaryDirectory() as external,
        ):
            root = Path(folder)
            (root / "outside").symlink_to(external, target_is_directory=True)
            for relative in [
                "../outside.csv",
                "/tmp/outside.csv",
                "outside/data.csv",
                ".",
            ]:
                with self.subTest(relative=relative), self.assertRaises(ValueError):
                    local_path(root, relative)
            self.assertEqual(
                local_path(root, "safe/input.csv"), (root / "safe/input.csv").resolve()
            )
