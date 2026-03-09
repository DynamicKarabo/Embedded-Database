import unittest
from engine.indexing import IndexManager

class TestIndexManager(unittest.TestCase):
    def setUp(self):
        self.index = IndexManager()

    def test_set_and_get(self):
        self.index.set(b"k1", 100)
        self.assertEqual(self.index.get(b"k1"), 100)
        self.assertIsNone(self.index.get(b"nonexistent"))

    def test_delete(self):
        self.index.set(b"k1", 100)
        self.index.delete(b"k1")
        self.assertIsNone(self.index.get(b"k1"))

    def test_size(self):
        self.assertEqual(self.index.size, 0)
        self.index.set(b"k1", 100)
        self.index.set(b"k2", 200)
        self.assertEqual(self.index.size, 2)

if __name__ == "__main__":
    unittest.main()
# Commit 6: refactor: Move header size to a constant in StorageEngine
# Commit 14: feat: Add IndexManager for in-memory key-to-offset mapping
