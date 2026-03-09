import os
import struct
import binascii
from typing import Tuple, Iterator, Optional

class WALManager:
    """
    Manages the Write-Ahead Log (WAL).
    Log Entry Format: [CRC32 (4 bytes)][key_len (4 bytes)][val_len (4 bytes)][key][value]
    """
    HEADER_SIZE = 12  # 4 (CRC) + 4 (key_len) + 4 (val_len)
    TOMBSTONE_LEN = 0xFFFFFFFF

    def __init__(self, log_file: str):
        self.log_file = log_file
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        self.file = open(self.log_file, "a+b")

    def log(self, key: bytes, value: Optional[bytes]):
        """
        Records an operation in the WAL.
        """
        self.file.seek(0, os.SEEK_END)
        
        key_len = len(key)
        if value is None:
            val_len = self.TOMBSTONE_LEN
            val_bytes = b""
        else:
            val_len = len(value)
            val_bytes = value

        data = struct.pack("!II", key_len, val_len) + key + val_bytes
        crc = binascii.crc32(data) & 0xffffffff
        
        entry = struct.pack("!I", crc) + data
        self.file.write(entry)
        self.file.flush()
        os.fsync(self.file.fileno())

    def recover(self) -> Iterator[Tuple[bytes, Optional[bytes]]]:
        """
        Reads the log and yields valid (key, value) pairs.
        """
        self.file.seek(0)
        while True:
            header = self.file.read(self.HEADER_SIZE)
            if not header or len(header) < self.HEADER_SIZE:
                break
                
            expected_crc, key_len, val_len = struct.unpack("!III", header)
            
            read_len = key_len if val_len == self.TOMBSTONE_LEN else (key_len + val_len)
            payload = self.file.read(read_len)
            if len(payload) < read_len:
                break # Partial write or corruption
            
            # Verify CRC
            actual_payload = struct.pack("!II", key_len, val_len) + payload
            if binascii.crc32(actual_payload) & 0xffffffff != expected_crc:
                break
                
            key = payload[:key_len]
            value = None if val_len == self.TOMBSTONE_LEN else payload[key_len:]
            yield key, value

    def clear(self):
        """
        Clears the WAL after a successful checkpoint.
        """
        self.file.truncate(0)
        self.file.seek(0)
        self.file.flush()
        os.fsync(self.file.fileno())

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
# Commit 1: docs: Add prd.md with core requirements
# Commit 9: feat: Add CRC32 integrity check to WAL entries
# Commit 17: feat: Integrate StorageEngine and WALManager in Database
# Commit 25: docs: Add README.md with project overview
# Commit 33: style: Linting and formatting improvements
# Commit 41: feat: Add logging for database operations
# Commit 49: feat: Add support for key prefix searches
# Commit 57: feat: Add command-line tool for basic DB operations
# Commit 65: fix: Final polish for v1.0.0 release
