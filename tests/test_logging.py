import unittest
import os
import shutil
import tempfile
from engine.logging import WALManager

class TestWALManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.test_dir, "test.wal")
        self.wal = WALManager(self.log_file)

    def tearDown(self):
        if self.wal:
            self.wal.close()
        shutil.rmtree(self.test_dir)

    def test_log_and_recover(self):
        recs = [
            (b"k1", b"v1"),
            (b"k2", b"v2")
        ]
        for k, v in recs:
            self.wal.log(k, v)
            
        recovered = list(self.wal.recover())
        self.assertEqual(recovered, recs)

    def test_corruption_detection(self):
        self.wal.log(b"good", b"data")
        self.wal.close()
        
        # Manually corrupt the file
        with open(self.log_file, "r+b") as f:
            f.seek(15) # middle of data
            f.write(b"X")
            
        self.wal = WALManager(self.log_file)
        recovered = list(self.wal.recover())
        # Should stop before or at corruption
        # In this case, since we pack key_len and val_len first, 
        # and CRC is over EVERYTHING after CRC field, 
        # any change in payload will fail CRC.
        self.assertEqual(len(recovered), 0)

    def test_clear(self):
        self.wal.log(b"k", b"v")
        self.wal.clear()
        recovered = list(self.wal.recover())
        self.assertEqual(len(recovered), 0)

if __name__ == "__main__":
    unittest.main()
# Commit 5: fix: Handle empty data read in storage.py
# Commit 13: feat: Implement WAL recovery mechanism
