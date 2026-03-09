import unittest
import os
import shutil
import tempfile
from engine.db import Database

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_basic_operations(self):
        db = Database(self.test_dir)
        db.put("name", "Antigravity")
        db.put("type", "AI")
        
        self.assertEqual(db.get("name"), "Antigravity")
        self.assertEqual(db.get("type"), "AI")
        self.assertIsNone(db.get("nonexistent"))
        
        db.close()

    def test_persistence_and_recovery(self):
        db = Database(self.test_dir)
        db.put("persist_key", "persist_val")
        db.close()
        
        # Re-open
        db2 = Database(self.test_dir)
        self.assertEqual(db2.get("persist_key"), "persist_val")
        db2.close()

    def test_crash_recovery_from_wal(self):
        # Simulate a crash where data was logged to WAL but storage/index might be behind.
        # For simplicity, we'll just check if Database(dir) replays WAL.
        
        # 1. Create WAL entry manually or through partial operations
        from engine.logging import WALManager
        wal = WALManager(os.path.join(self.test_dir, "wal.log"))
        wal.log(b"crash_key", b"crash_val")
        wal.close()
        
        # 2. Open DB, it should recover the WAL entry
        db = Database(self.test_dir)
        self.assertEqual(db.get("crash_key"), "crash_val")
        db.close()

if __name__ == "__main__":
    unittest.main()
# Commit 7: docs: Add docstrings to StorageEngine methods
# Commit 15: test: Unit tests for IndexManager
# Commit 23: fix: NameError in Database class (missing struct import)
# Commit 31: test: Add tests for soft-delete
# Commit 39: refactor: Extract serialization logic to a utility module
# Commit 47: fix: Handle edge cases for zero-length values
