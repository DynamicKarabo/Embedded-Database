import unittest
import os
import shutil
import tempfile
from engine.db import Database

class TestMilestone5(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_durable_deletion(self):
        db = Database(self.test_dir)
        db.put("key1", "val1")
        db.delete("key1")
        self.assertIsNone(db.get("key1"))
        db.close()
        
        # Re-open, should still be deleted (via tombstone)
        db2 = Database(self.test_dir)
        self.assertIsNone(db2.get("key1"))
        db2.close()

    def test_compaction(self):
        db = Database(self.test_dir)
        data_file = os.path.join(self.test_dir, "data.db")
        
        # 1. Write many updates to same key
        for i in range(100):
            db.put("key1", f"val{i}")
            
        initial_size = os.path.getsize(data_file)
        
        # 2. Compact
        db.compact()
        
        compacted_size = os.path.getsize(data_file)
        
        # 3. Size should be significantly smaller
        # Each record is roughly 12 + 4 + 4 = 20 bytes. 
        # 100 records = 2000 bytes. Compacted = 20 bytes.
        self.assertLess(compacted_size, initial_size)
        self.assertEqual(db.get("key1"), "val99")
        db.close()

    def test_compaction_with_deletions(self):
        db = Database(self.test_dir)
        db.put("keep", "this")
        db.put("delete", "me")
        db.delete("delete")
        
        db.compact()
        
        self.assertEqual(db.get("keep"), "this")
        self.assertIsNone(db.get("delete"))
        db.close()

if __name__ == "__main__":
    unittest.main()
# History 4: test: Unit tests for tombstone serialization
# History 10: refactor: Extract file swapping logic in Database.compact()
# History 16: feat: Add get_range support to IndexManager
# History 22: test: Test range queries with mixed deletions
# History 28: feat: Add basic logging for compaction starts/stops
# History 34: chore: Clean up temporary compaction files on failure
# History 40: docs: Update README with range query examples
