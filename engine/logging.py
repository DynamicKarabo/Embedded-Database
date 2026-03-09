import os
import struct
import binascii
from typing import Tuple, Iterator

class WALManager:
    """
    Manages the Write-Ahead Log (WAL).
    Log Entry Format: [CRC32 (4 bytes)][key_len (4 bytes)][val_len (4 bytes)][key][value]
    """
    HEADER_SIZE = 12  # 4 (CRC) + 4 (key_len) + 4 (val_len)

    def __init__(self, log_file: str):
        self.log_file = log_file
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        self.file = open(self.log_file, "a+b")

    def log(self, key: bytes, value: bytes):
        """
        Records an operation in the WAL.
        """
        self.file.seek(0, os.SEEK_END)
        
        key_len = len(key)
        val_len = len(value)
        data = struct.pack("!II", key_len, val_len) + key + value
        crc = binascii.crc32(data) & 0xffffffff
        
        entry = struct.pack("!I", crc) + data
        self.file.write(entry)
        self.file.flush()
        os.fsync(self.file.fileno())

    def recover(self) -> Iterator[Tuple[bytes, bytes]]:
        """
        Reads the log and yields valid (key, value) pairs.
        """
        self.file.seek(0)
        while True:
            header = self.file.read(self.HEADER_SIZE)
            if not header or len(header) < self.HEADER_SIZE:
                break
                
            expected_crc, key_len, val_len = struct.unpack("!III", header)
            
            payload = self.file.read(key_len + val_len)
            if len(payload) < key_len + val_len:
                break # Partial write or corruption
            
            # Verify CRC
            actual_payload = struct.pack("!II", key_len, val_len) + payload
            if binascii.crc32(actual_payload) & 0xffffffff != expected_crc:
                # Corruption detected, might want to stop or signal error
                # For now, we'll stop reading
                break
                
            key = payload[:key_len]
            value = payload[key_len:]
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
