import logging
"""A2A Mesh Dedup — Message deduplication with TTL-based cache."""

import time
import threading
from collections import OrderedDict
from typing import Optional

log = logging.getLogger("a2a_mesh.dedup")



class DedupCache:
    """Thread-safe LRU deduplication cache with TTL.

    Prevents processing the same message twice (arriving via different transports).
    Uses OrderedDict for O(1) lookup and LRU eviction.
    """

    def __init__(self, max_size: int = 5000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl = ttl_seconds
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def check_and_add(self, message_id: str) -> bool:
        """Check if duplicate and add atomically. Returns True if duplicate.
        
        This is the primary entry point for dedup. The check and add are done
        under a single lock acquisition to prevent TOCTOU race conditions where
        two concurrent calls could both see is_duplicate=False before either adds.
        """
        with self._lock:
            if message_id in self._cache:
                ts = self._cache[message_id]
                if time.time() - ts < self.ttl:
                    # Move to end (most recently accessed)
                    self._cache.move_to_end(message_id)
                    self._hits += 1
                    return True
                else:
                    # Expired, remove it
                    del self._cache[message_id]

            # Not a duplicate — add it now
            self._cache[message_id] = time.time()
            self._misses += 1
            # Evict oldest if over max size
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
            return False

    def is_duplicate(self, message_id: str) -> bool:
        """Check if a message ID has been seen recently (does NOT add)."""
        with self._lock:
            if message_id in self._cache:
                ts = self._cache[message_id]
                if time.time() - ts < self.ttl:
                    return True
                else:
                    del self._cache[message_id]
            return False

    def add(self, message_id: str) -> None:
        """Add a message ID to the cache."""
        with self._lock:
            if message_id in self._cache:
                self._cache.move_to_end(message_id)
                self._cache[message_id] = time.time()
            else:
                self._cache[message_id] = time.time()
                while len(self._cache) > self.max_size:
                    self._cache.popitem(last=False)

    def remove(self, message_id: str) -> None:
        """Remove a message ID from the cache."""
        with self._lock:
            self._cache.pop(message_id, None)

    def clear(self) -> None:
        """Clear the entire cache."""
        with self._lock:
            self._cache.clear()

    def cleanup(self) -> int:
        """Remove expired entries. Returns number of entries removed."""
        now = time.time()
        removed = 0
        with self._lock:
            # Iterate from oldest to newest
            keys_to_remove = []
            for key, ts in self._cache.items():
                if now - ts > self.ttl:
                    keys_to_remove.append(key)
                else:
                    break  # OrderedDict is ordered by insertion time
            for key in keys_to_remove:
                del self._cache[key]
                removed += 1
        return removed

    @property
    def size(self) -> int:
        """Current cache size."""
        return len(self._cache)

    @property
    def stats(self) -> dict:
        """Dedup statistics including hit/miss ratio."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "ttl_seconds": self.ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(hit_rate, 4),
        }

    def __repr__(self):
        return f"DedupCache(size={self.size}, max={self.max_size}, hits={self._hits}, misses={self._misses})"