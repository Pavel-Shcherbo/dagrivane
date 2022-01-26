import hashlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dagrivane.object_store import ObjectStore, ObjectError


class ObjectStoreTests(unittest.TestCase):
    def test_concurrent_identical_writes_deduplicate_without_torn_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ObjectStore(directory)
            payload = b"content-addressed control" * 1000
            with ThreadPoolExecutor(max_workers=6) as pool:
                records = list(pool.map(store.put, [payload] * 12))
            self.assertEqual(len({record["sha256"] for record in records}), 1)
            identity = hashlib.sha256(payload).hexdigest()
            self.assertEqual(store.get(identity), payload)
            self.assertEqual(store.inventory()["objects"], [identity])
            self.assertTrue(store.inventory()["complete"])

    def test_corrupt_existing_object_is_never_silently_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ObjectStore(directory)
            identity = store.put(b"original")["sha256"]
            store.path(identity).write_bytes(b"changed")
            for operation in [
                lambda: store.get(identity),
                lambda: store.put(b"original"),
            ]:
                with self.assertRaisesRegex(ObjectError, "SHA-256"):
                    operation()
            self.assertEqual(store.path(identity).read_bytes(), b"changed")

    def test_export_is_verified_and_create_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ObjectStore(root / "objects")
            identity = store.put(b"payload")["sha256"]
            destination = root / "export.bin"
            store.export(identity, destination)
            self.assertEqual(destination.read_bytes(), b"payload")
            with self.assertRaises(FileExistsError):
                store.export(identity, destination)

