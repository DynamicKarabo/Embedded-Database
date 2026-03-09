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
                key, value = self.storage.read(offset)
                if value is None:
                    self.index.delete(key)
                else:
                    self.index.set(key, offset)
                offset = self.storage.file.tell()
            except (EOFError, struct.error):
                break

    def _recover_from_wal(self):
        """
        Rebuilds the index and ensures storage consistency by replaying the WAL.
        """
        for key, value in self.wal.recover():
            offset = self.storage.append(key, value)
            if value is None:
                self.index.delete(key)
            else:
                self.index.set(key, offset)
        
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
        Deletes a key (durable delete via tombstone).
        """
        k_bytes = key.encode("utf-8")
        
        # 1. Log tombstone to WAL
        self.wal.log(k_bytes, None)
        
        # 2. Append tombstone to storage
        self.storage.append(k_bytes, None)
        
        # 3. Update index
        self.index.delete(k_bytes)

    def compact(self):
        """
        Reclaims space by writing only live records to a new storage file.
        """
        compact_file = os.path.join(self.data_dir, "data.db.compacted")
        new_storage = StorageEngine(compact_file)
        new_index = IndexManager()
        
        # Iterate over current live keys in the index
        for key in self.index.keys():
            old_offset = self.index.get(key)
            if old_offset is not None:
                _, value = self.storage.read(old_offset)
                if value is not None:
                    new_offset = new_storage.append(key, value)
                    new_index.set(key, new_offset)
        
        # Close current storage to allow file swap
        self.storage.close()
        new_storage.close()
        
        # Swap files
        data_file = os.path.join(self.data_dir, "data.db")
        os.replace(compact_file, data_file)
        
        # Re-open storage and update index
        self.storage = StorageEngine(data_file)
        self.index = new_index

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
# Final touch
