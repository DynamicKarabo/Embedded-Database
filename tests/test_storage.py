import unittest
import os
import shutil
import tempfile
from engine.storage import StorageEngine

class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.data_file = os.path.join(self.test_dir, "test.db")
        self.storage = StorageEngine(self.data_file)

    def tearDown(self):
        if self.storage:
            self.storage.close()
        shutil.rmtree(self.test_dir)

    def test_append_and_read(self):
        key = b"name"
        value = b"Antigravity"
        
        offset = self.storage.append(key, value)
        self.assertEqual(offset, 0)
        
        read_key, read_value = self.storage.read(offset)
        self.assertEqual(read_key, key)
        self.assertEqual(read_value, value)

    def test_multiple_appends(self):
        recs = [
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3")
        ]
        
        offsets = []
        for k, v in recs:
            offsets.append(self.storage.append(k, v))
            
        for i, (k, v) in enumerate(recs):
            read_key, read_value = self.storage.read(offsets[i])
            self.assertEqual(read_key, k)
            self.assertEqual(read_value, v)

    def test_persistence(self):
        # Write record
        offset = self.storage.append(b"persist", b"test")
        self.storage.close()
        self.storage = None
        
        # Re-open and read
        storage2 = StorageEngine(self.data_file)
        rk, rv = storage2.read(offset)
        self.assertEqual(rk, b"persist")
        self.assertEqual(rv, b"test")
        storage2.close()

if __name__ == "__main__":
    unittest.main()
# Commit 4: test: Initial unit tests for storage engine
# Commit 12: refactor: Optimize record packing in WALManager
# Commit 20: feat: Add index rebuilding from storage on startup
# Commit 28: refactor: Improve error handling in storage.read()
# Commit 36: perf: Optimize index lookups for high volume
# Commit 44: refactor: Standardize parameter names across classes
