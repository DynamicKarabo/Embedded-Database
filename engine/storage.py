import os
import struct
from typing import Tuple, Optional
import binascii

class StorageEngine:
    """
    Handles append-only storage for the database.
    Format: [key_len (4 bytes)][val_len (4 bytes)][key][value]
    """
    HEADER_SIZE = 12  # 4 (CRC) + 4 (key_len) + 4 (val_len)
    TOMBSTONE_LEN = 0xFFFFFFFF  # Special value for deleted records

    def __init__(self, data_file: str):
        self.data_file = data_file
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(data_file)), exist_ok=True)
        # Open in append+binary mode (creates if doesn't exist)
        self.file = open(self.data_file, "a+b")

    def append(self, key: bytes, value: bytes) -> int:
        """
        Appends a record to the file.
        Returns the offset of the new record.
        """
        self.file.seek(0, os.SEEK_END)
        offset = self.file.tell()
        
        key_len = len(key)
        if value is None:
            val_len = self.TOMBSTONE_LEN
            val_bytes = b""
        else:
            val_len = len(value)
            val_bytes = value
            
        record_header_and_data = struct.pack("!II", key_len, val_len) + key + val_bytes
        crc = binascii.crc32(record_header_and_data) & 0xffffffff
        
        self.file.write(struct.pack("!I", crc) + record_header_and_data)
        self.file.flush()
        # Ensure data is written to disk
        os.fsync(self.file.fileno())
        
        return offset

    def read(self, offset: int) -> Tuple[bytes, bytes]:
        """
        Reads a record from the given offset.
        """
        self.file.seek(offset)
        header = self.file.read(self.HEADER_SIZE)
        if not header or len(header) < self.HEADER_SIZE:
            raise EOFError("Could not read record header")
            
        crc_actual, key_len, val_len = struct.unpack("!III", header)
        
        read_len = key_len if val_len == self.TOMBSTONE_LEN else (key_len + val_len)
        data = self.file.read(read_len)
        if len(data) < read_len:
            raise EOFError("Could not read full record data")
            
        # Verify CRC
        record_payload = header[4:] + data
        if binascii.crc32(record_payload) & 0xffffffff != crc_actual:
            raise ValueError("Storage record CRC mismatch - data corruption detected")
            
        if val_len == self.TOMBSTONE_LEN:
            return data, None
            
        key = data[:key_len]
        value = data[key_len:]
        
        return key, value

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
# Commit 0: init: Initial project setup
# Commit 8: feat: Implement WALManager core logging logic
# Commit 16: feat: Implement basic Query Interface in Database class
# Commit 24: test: Fix failing persistence tests (improved teardown)
# Commit 32: refactor: Use constant for WAL header size
# Commit 40: test: Fix intermittent test failures in CI
# Commit 48: chore: Update .gitignore to exclude .pyc and __pycache__
# Commit 56: docs: Add troubleshooting section to README.md
# Commit 64: feat: Add support for multiple data files (sharding prep)
# History 0: feat: Define TOMBSTONE_LEN constant for durable deletes
# History 6: refactor: Update Database._rebuild_index_from_storage to handle deleted keys
# History 12: test: Add compaction stress test with 100 duplicate updates
# History 18: feat: Implement get_range in Database class
# History 24: fix: NameError in logging.py (missing Optional)
# History 30: refactor: Use IndexManager.keys() for safe iteration in compact()
# History 36: refactor: Improve error messages for CRC mismatches
