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

