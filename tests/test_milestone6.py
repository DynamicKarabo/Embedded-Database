import unittest
import os
import shutil
import tempfile
from engine.db import Database

class TestMilestone6(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir)

    def test_range_query(self):
        # Insert keys in non-sorted order
        data = {
            "apple": "fruit",
            "banana": "fruit",
            "carrot": "vegetable",
            "date": "fruit",
            "eggplant": "vegetable"
        }
        for k, v in data.items():
            self.db.put(k, v)
            
        # Range scan [banana, date]
        results = self.db.get_range("banana", "date")
        
        expected = [
            ("banana", "fruit"),
            ("carrot", "vegetable"),
            ("date", "fruit")
        ]
        self.assertEqual(results, expected)

    def test_range_query_with_deletions(self):
        self.db.put("a", "1")
        self.db.put("b", "2")
        self.db.put("c", "3")
        self.db.delete("b")
        
        results = self.db.get_range("a", "c")
        expected = [("a", "1"), ("c", "3")]
        self.assertEqual(results, expected)

    def test_empty_range(self):
        self.db.put("x", "100")
        results = self.db.get_range("a", "m")
        self.assertEqual(results, [])

if __name__ == "__main__":
    unittest.main()
# History 5: feat: Support tombstone recovery in WALManager
# History 11: fix: Ensure old storage is closed before file replacement
# History 17: refactor: Update IndexManager.delete to use binary search
# History 23: refactor: Standardize record header format across engine
# History 29: test: Verify data integrity after compaction
# History 35: test: Edge case tests for empty range queries
# History 41: fix: Final polish for durable deletion logic
