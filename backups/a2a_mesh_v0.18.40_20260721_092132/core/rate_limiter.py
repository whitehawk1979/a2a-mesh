"""A2A Mesh Rate Limiter — Token bucket rate limiting for API and P2P endpoints.

Inspired by sushaan-k/a2a-mesh RateLimiter, adapted for our architecture.
Uses per-key sliding window with LRU eviction for memory efficiency.
"""

import time
import logging
from collections import OrderedDict
from typing import Dict, Optional, Tuple
from ..core.exceptions import RateLimitError

log = logging.getLogger("a2a_mesh.rate_limiter")


class RateLimiter:
    """Sliding window rate limiter with per-key buckets and LRU eviction.

    Usage:
        limiter = RateLimiter(max_requests=100, window_seconds=60)

        # Check rate limit
        if not limiter.allow("agent:nova"):
            raise RateLimitError(retry_after=limiter.retry_after("agent:nova"))

        # Or use as decorator-like check
        limiter.check("agent:nova")  # Raises RateLimitError if exceeded
    """

    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0,
                 max_buckets: int = 1000):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.max_buckets = max_buckets
        self._buckets: OrderedDict[str, list] = OrderedDict()

    def _cleanup_bucket(self, key: str, now: float) -> list:
        """Remove timestamps outside the sliding window."""
        if key not in self._buckets:
            return []
        cutoff = now - self.window_seconds
        self._buckets[key] = [
            ts for ts in self._buckets[key] if ts > cutoff
        ]
        return self._buckets[key]

    def _evict_if_needed(self):
        """LRU eviction when bucket count exceeds max_buckets."""
        while len(self._buckets) > self.max_buckets:
            self._buckets.popitem(last=False)  # Remove oldest

    def allow(self, key: str) -> bool:
        """Check if a request from key is allowed.

        Returns True if the request is within rate limits, False otherwise.
        """
        now = time.time()
        timestamps = self._cleanup_bucket(key, now)

        if len(timestamps) >= self.max_requests:
            # Move key to end (most recently accessed)
            self._buckets.move_to_end(key)
            log.warning(f"Rate limit exceeded for {key}: {len(timestamps)}/{self.max_requests}")
            return False

        # Record this request
        timestamps.append(now)
        self._buckets[key] = timestamps
        self._buckets.move_to_end(key)
        self._evict_if_needed()
        return True

    def check(self, key: str) -> None:
        """Check rate limit and raise RateLimitError if exceeded."""
        if not self.allow(key):
            remaining_time = self.retry_after(key)
            raise RateLimitError(
                message=f"Rate limit exceeded for '{key}'",
                retry_after=remaining_time,
                key=key,
                max_requests=self.max_requests,
                window_seconds=self.window_seconds,
            )

    def retry_after(self, key: str) -> float:
        """Get seconds until the oldest request in the window expires."""
        if key not in self._buckets or not self._buckets[key]:
            return 0.0
        now = time.time()
        timestamps = self._cleanup_bucket(key, now)
        if not timestamps:
            return 0.0
        oldest = timestamps[0]
        return max(0.0, self.window_seconds - (now - oldest))

    def remaining(self, key: str) -> int:
        """Get remaining requests allowed for this key in the current window."""
        now = time.time()
        timestamps = self._cleanup_bucket(key, now)
        return max(0, self.max_requests - len(timestamps))

    def reset(self, key: str) -> None:
        """Reset rate limit for a specific key."""
        self._buckets.pop(key, None)

    def reset_all(self) -> None:
        """Reset all rate limit buckets."""
        self._buckets.clear()

    def stats(self) -> Dict:
        """Get rate limiter statistics."""
        return {
            "active_keys": len(self._buckets),
            "max_requests": self.max_requests,
            "window_seconds": self.window_seconds,
            "max_buckets": self.max_buckets,
        }


class EndpointRateLimiter:
    """Per-endpoint rate limiter with different limits per endpoint type.

    Applies different rate limits to:
    - API endpoints (e.g., /api/*)
    - P2P message endpoints
    - Health check endpoints
    - Workflow endpoints
    """

    # Default rate limit configurations per endpoint type
    DEFAULT_LIMITS = {
        "api": RateLimiter(max_requests=100, window_seconds=60),       # 100/min
        "p2p": RateLimiter(max_requests=200, window_seconds=60),       # 200/min
        "health": RateLimiter(max_requests=30, window_seconds=60),     # 30/min
        "workflow": RateLimiter(max_requests=20, window_seconds=60),   # 20/min
        "auth": RateLimiter(max_requests=10, window_seconds=60),       # 10/min
    }

    def __init__(self, limits: Optional[Dict[str, RateLimiter]] = None):
        self._limiters = limits or dict(self.DEFAULT_LIMITS)

    def check(self, endpoint_type: str, key: str) -> None:
        """Check rate limit for an endpoint type and key.

        Raises RateLimitError if exceeded.
        """
        limiter = self._limiters.get(endpoint_type)
        if limiter:
            limiter.check(key)

    def allow(self, endpoint_type: str, key: str) -> bool:
        """Check if request is allowed without raising."""
        limiter = self._limiters.get(endpoint_type)
        if limiter:
            return limiter.allow(key)
        return True

    def remaining(self, endpoint_type: str, key: str) -> Optional[int]:
        """Get remaining requests for a key on an endpoint type."""
        limiter = self._limiters.get(endpoint_type)
        if limiter:
            return limiter.remaining(key)
        return None

    def stats(self) -> Dict:
        """Get statistics for all endpoint limiters."""
        return {
            endpoint: limiter.stats()
            for endpoint, limiter in self._limiters.items()
        }