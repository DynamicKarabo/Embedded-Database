import os
import struct
from typing import Tuple, Optional

class StorageEngine:
    """
    Handles append-only storage for the database.
    Format: [key_len (4 bytes)][val_len (4 bytes)][key][value]
    """
    HEADER_SIZE = 8  # 4 bytes for key_len, 4 bytes for val_len

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
        val_len = len(value)
        header = struct.pack("!II", key_len, val_len)
        
        self.file.write(header + key + value)
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
            
        key_len, val_len = struct.unpack("!II", header)
        
        data = self.file.read(key_len + val_len)
        if len(data) < key_len + val_len:
            raise EOFError("Could not read full record data")
            
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
