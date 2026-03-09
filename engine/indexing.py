import bisect
from typing import Dict, Optional, List, Tuple

class IndexManager:
    """
    Maintains an in-memory sorted index mapping keys to file offsets.
    Uses a sorted list of keys for O(log N) lookups and range scans.
    """
    def __init__(self):
        self._index: Dict[bytes, int] = {}
        self._keys: List[bytes] = []

    def set(self, key: bytes, offset: int):
        if key not in self._index:
            bisect.insort(self._keys, key)
        self._index[key] = offset

    def get(self, key: bytes) -> Optional[int]:
        return self._index.get(key)

    def delete(self, key: bytes):
        if key in self._index:
            del self._index[key]
            # Binary search for the key to remove it from the sorted list
            idx = bisect.bisect_left(self._keys, key)
            if idx < len(self._keys) and self._keys[idx] == key:
                self._keys.pop(idx)

    def get_range(self, start_key: bytes, end_key: bytes) -> List[Tuple[bytes, int]]:
        """
        Returns all (key, offset) pairs within the [start_key, end_key] range.
        """
        start_idx = bisect.bisect_left(self._keys, start_key)
        end_idx = bisect.bisect_right(self._keys, end_key)
        
        results = []
        for i in range(start_idx, end_idx):
            key = self._keys[i]
            results.append((key, self._index[key]))
        return results

    def keys(self):
        return list(self._keys)

    @property
    def size(self) -> int:
        return len(self._keys)
# Commit 2: feat: Implement base StorageEngine structure
# Commit 10: test: Add unit tests for WALManager
# Commit 18: feat: Integrate IndexManager in Database
# Commit 26: refactor: Clean up redundant code in db.py
# Commit 34: fix: Ensure file pointers are handled correctly during recovery
# Commit 42: fix: Potential memory leak in IndexManager
# Commit 50: refactor: Improve Database class organization
# Commit 58: fix: Issue with record recovery when header is partially written
# Commit 66: release: Version 1.0.0
# History 2: feat: Add CRC32 verification to StorageEngine.read
# History 8: test: Integration test for deletion durability across restarts
# History 14: perf: Optimize record iteration during compaction
# History 20: fix: Include missing typing imports (List, Tuple, Optional)
# History 26: docs: Update walkthrough.md with compaction details
# History 32: perf: Improve bisect performance for large indexes
# History 38: feat: Add database.get_stats() for compaction metrics
