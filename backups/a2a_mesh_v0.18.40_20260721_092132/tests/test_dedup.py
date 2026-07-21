"""Test core.dedup — Deduplication cache"""
import pytest
from a2a_mesh.core.dedup import DedupCache


class TestDedupCache:
    """Test message deduplication."""

    def test_new_message_not_duplicate(self):
        cache = DedupCache(max_size=100, ttl_seconds=300)
        assert not cache.is_duplicate("msg_001")
        assert not cache.is_duplicate("msg_002")

    def test_cache_size_limit(self):
        cache = DedupCache(max_size=3, ttl_seconds=300)
        cache.is_duplicate("msg_001")
        cache.is_duplicate("msg_002")
        cache.is_duplicate("msg_003")
        # After 3 entries, oldest should be evicted
        assert not cache.is_duplicate("msg_004")
        # msg_001 should be evicted
        assert not cache.is_duplicate("msg_001")

    def test_different_ids_not_duplicate(self):
        cache = DedupCache(max_size=100, ttl_seconds=300)
        for i in range(50):
            assert not cache.is_duplicate(f"msg_{i:04d}")