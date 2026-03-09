from typing import Dict, Optional

class IndexManager:
    """
    Maintains an in-memory hash index mapping keys to file offsets.
    """
    def __init__(self):
        self._index: Dict[bytes, int] = {}

    def set(self, key: bytes, offset: int):
        self._index[key] = offset

    def get(self, key: bytes) -> Optional[int]:
        return self._index.get(key)

    def delete(self, key: bytes):
        if key in self._index:
            del self._index[key]

    def clear(self):
        self._index.clear()

    @property
    def size(self) -> int:
        return len(self._index)
# Commit 2: feat: Implement base StorageEngine structure
# Commit 10: test: Add unit tests for WALManager
# Commit 18: feat: Integrate IndexManager in Database
# Commit 26: refactor: Clean up redundant code in db.py
# Commit 34: fix: Ensure file pointers are handled correctly during recovery
# Commit 42: fix: Potential memory leak in IndexManager
