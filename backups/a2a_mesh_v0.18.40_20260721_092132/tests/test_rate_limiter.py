"""Tests for core.rate_limiter — Sliding window rate limiting."""

import time
import pytest

from a2a_mesh.core.rate_limiter import RateLimiter, EndpointRateLimiter
from a2a_mesh.core.exceptions import RateLimitError


class TestRateLimiterBasic:
    def test_allow_within_limit(self):
        limiter = RateLimiter(max_requests=5, window_seconds=60)
        for _ in range(5):
            assert limiter.allow("user1") is True

    def test_block_over_limit(self):
        limiter = RateLimiter(max_requests=3, window_seconds=60)
        assert limiter.allow("user1") is True
        assert limiter.allow("user1") is True
        assert limiter.allow("user1") is True
        assert limiter.allow("user1") is False  # 4th request blocked

    def test_separate_keys_independent(self):
        limiter = RateLimiter(max_requests=2, window_seconds=60)
        assert limiter.allow("user1") is True
        assert limiter.allow("user1") is True
        assert limiter.allow("user1") is False  # user1 at limit
        # user2 is independent
        assert limiter.allow("user2") is True
        assert limiter.allow("user2") is True
        assert limiter.allow("user2") is False

    def test_remaining_count(self):
        limiter = RateLimiter(max_requests=5, window_seconds=60)
        assert limiter.remaining("user1") == 5
        limiter.allow("user1")
        assert limiter.remaining("user1") == 4
        limiter.allow("user1")
        assert limiter.remaining("user1") == 3

    def test_remaining_zero_at_limit(self):
        limiter = RateLimiter(max_requests=2, window_seconds=60)
        limiter.allow("user1")
        limiter.allow("user1")
        assert limiter.remaining("user1") == 0


class TestRateLimiterSlidingWindow:
    def test_window_expiry_allows_again(self):
        limiter = RateLimiter(max_requests=2, window_seconds=0.3)
        limiter.allow("user1")
        limiter.allow("user1")
        assert limiter.allow("user1") is False
        # Wait for window to expire
        time.sleep(0.35)
        assert limiter.allow("user1") is True

    def test_partial_window_expiry(self):
        limiter = RateLimiter(max_requests=3, window_seconds=0.5)
        limiter.allow("user1")
        time.sleep(0.3)
        limiter.allow("user1")
        # First request should be expired, still have room
        assert limiter.remaining("user1") >= 1


class TestRateLimiterCheck:
    def test_check_raises_rate_limit_error(self):
        limiter = RateLimiter(max_requests=1, window_seconds=60)
        limiter.check("user1")  # First is OK
        with pytest.raises(RateLimitError):
            limiter.check("user1")  # Second raises

    def test_check_error_has_retry_after(self):
        limiter = RateLimiter(max_requests=1, window_seconds=60)
        limiter.check("user1")
        try:
            limiter.check("user1")
            assert False, "Should have raised"
        except RateLimitError as e:
            assert e.retry_after > 0


class TestRateLimiterRetryAfter:
    def test_retry_after_within_window(self):
        limiter = RateLimiter(max_requests=2, window_seconds=60)
        limiter.allow("user1")
        retry = limiter.retry_after("user1")
        assert 0 < retry <= 60

    def test_retry_after_unknown_key(self):
        limiter = RateLimiter(max_requests=2, window_seconds=60)
        assert limiter.retry_after("unknown") == 0.0


class TestRateLimiterReset:
    def test_reset_specific_key(self):
        limiter = RateLimiter(max_requests=1, window_seconds=60)
        limiter.allow("user1")
        assert limiter.remaining("user1") == 0
        limiter.reset("user1")
        assert limiter.remaining("user1") == 1

    def test_reset_all(self):
        limiter = RateLimiter(max_requests=1, window_seconds=60)
        limiter.allow("user1")
        limiter.allow("user2")
        limiter.reset_all()
        assert limiter.remaining("user1") == 1
        assert limiter.remaining("user2") == 1


class TestRateLimiterLRUEviction:
    def test_lru_eviction_when_max_buckets_exceeded(self):
        limiter = RateLimiter(max_requests=5, window_seconds=60, max_buckets=3)
        limiter.allow("key1")
        limiter.allow("key2")
        limiter.allow("key3")
        # Adding key4 should evict key1 (oldest)
        limiter.allow("key4")
        # key1 was evicted, so it should have full remaining
        assert limiter.remaining("key1") == 5


class TestRateLimiterStats:
    def test_stats_structure(self):
        limiter = RateLimiter(max_requests=10, window_seconds=30, max_buckets=50)
        limiter.allow("user1")
        stats = limiter.stats()
        assert stats["active_keys"] == 1
        assert stats["max_requests"] == 10
        assert stats["window_seconds"] == 30
        assert stats["max_buckets"] == 50


class TestEndpointRateLimiter:
    def test_default_limiters_created(self):
        ep = EndpointRateLimiter()
        stats = ep.stats()
        assert "api" in stats
        assert "p2p" in stats
        assert "health" in stats
        assert "workflow" in stats
        assert "auth" in stats

    def test_check_allows_within_limit(self):
        ep = EndpointRateLimiter()
        # Should not raise for first request
        ep.check("api", "user1")

    def test_check_blocks_over_limit(self):
        # Create with very low limit
        custom = {"api": RateLimiter(max_requests=1, window_seconds=60)}
        ep = EndpointRateLimiter(limits=custom)
        ep.check("api", "user1")
        with pytest.raises(RateLimitError):
            ep.check("api", "user1")

    def test_allow_returns_bool(self):
        ep = EndpointRateLimiter()
        assert ep.allow("api", "user1") is True

    def test_unknown_endpoint_allways_allows(self):
        ep = EndpointRateLimiter()
        # Unknown endpoint type should allow by default
        assert ep.allow("unknown_type", "user1") is True

    def test_remaining_for_known_endpoint(self):
        ep = EndpointRateLimiter()
        remaining = ep.remaining("api", "user1")
        assert remaining is not None
        assert remaining > 0

    def test_remaining_for_unknown_endpoint(self):
        ep = EndpointRateLimiter()
        assert ep.remaining("nonexistent", "user1") is None
