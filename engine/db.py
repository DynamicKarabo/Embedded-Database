import os
import struct
from typing import Optional
from engine.storage import StorageEngine
from engine.logging import WALManager
from engine.indexing import IndexManager

class Database:
    """
    Glue class that orchestrates storage, logging, and indexing.
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.storage = StorageEngine(os.path.join(self.data_dir, "data.db"))
        self.wal = WALManager(os.path.join(self.data_dir, "wal.log"))
        self.index = IndexManager()
        
        self._rebuild_index_from_storage()
        self._recover_from_wal()

    def _rebuild_index_from_storage(self):
        """
        Rebuilds the in-memory index by scanning the storage file.
        """
        offset = 0
        while True:
            try:
                key, _ = self.storage.read(offset)
                self.index.set(key, offset)
                # Calculate next offset: Header (8) + len(key) + len(value)
                # But storage.read(offset) doesn't return the lengths directly here.
                # Let's modify storage.read to return the header info or use a helper.
                # Actually, I'll just use the file pointer position after read.
                offset = self.storage.file.tell()
            except (EOFError, struct.error):
                break

    def _recover_from_wal(self):
        """
        Rebuilds the index and ensures storage consistency by replaying the WAL.
        """
        for key, value in self.wal.recover():
            # In a real DB, we might want to check if the record is already in storage
            # But for this simple implementation, we append it to storage and update index
            offset = self.storage.append(key, value)
            self.index.set(key, offset)
        
        # After recovery, we can clear the WAL (checkpointing)
        self.wal.clear()

    def put(self, key: str, value: str):
        """
        Stores a key-value pair.
        """
        k_bytes = key.encode("utf-8")
        v_bytes = value.encode("utf-8")
        
        # 1. Log to WAL first (durability)
        self.wal.log(k_bytes, v_bytes)
        
        # 2. Append to storage
        offset = self.storage.append(k_bytes, v_bytes)
        
        # 3. Update index
        self.index.set(k_bytes, offset)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieves a value by key.
        """
        k_bytes = key.encode("utf-8")
        offset = self.index.get(k_bytes)
        
        if offset is None:
            return None
            
        _, v_bytes = self.storage.read(offset)
        return v_bytes.decode("utf-8")

    def delete(self, key: str):
        """
        Deletes a key (soft delete in this simple engine).
        In a real engine, we'd append a tombstone record.
        """
        k_bytes = key.encode("utf-8")
        self.index.delete(k_bytes)
        # Note: We don't remove from storage in this append-only design.
        # A tombstone could be logged to WAL and storage for full recovery.

    def close(self):
        self.storage.close()
        self.wal.close()
# Commit 3: feat: Add record serialization to StorageEngine
# Commit 11: fix: Ensure WAL logs are flushed and synced to disk
# Commit 19: fix: Ensure directories are created on initialization
# Commit 27: ci: Add initial CI workflow for running tests
# Commit 35: test: Add stress test with 1000 records
# Commit 43: docs: Document WAL format in logging.py
# Commit 51: docs: Add contribution guidelines
# Commit 59: test: Add unit tests for command-line interface
