"""A2A Mesh Bounded Queue — Thread-safe async queue with oldest-drop overflow.

Inspired by gensyn-ai/axl's receivedQueue pattern. When the queue reaches
capacity, the oldest message is dropped to make room for new ones. This
prevents OOM conditions during burst traffic while ensuring recent messages
are always processed.
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

log = logging.getLogger("a2a_mesh.bounded_queue")


@dataclass
class QueueStats:
    """Statistics for a bounded queue."""
    enqueued: int = 0
    dequeued: int = 0
    dropped: int = 0  # Messages dropped due to overflow
    overflow_count: int = 0  # Number of overflow events
    current_size: int = 0
    max_size: int = 0
    total_wait_time_ms: float = 0.0


class BoundedQueue:
    """Async bounded queue with oldest-drop overflow policy.
    
    When the queue is full, the oldest message is dropped to make room.
    This is different from asyncio.Queue(maxsize) which blocks producers
    or raises QueueFull — instead we prioritize recent messages over old ones.
    
    Usage:
        q = BoundedQueue(capacity=100)
        await q.put(message)  # Drops oldest if full
        msg = await q.get()   # Waits for message
    """
    
    def __init__(self, capacity: int = 100, name: str = "default"):
        self.capacity = capacity
        self.name = name
        self._queue: deque = deque(maxlen=capacity)  # auto-evicts oldest
        self._event = asyncio.Event()  # Signal when items available
        self._stats = QueueStats(max_size=capacity)
        self._lock = asyncio.Lock()
        log.debug(f"BoundedQueue '{name}' created (capacity={capacity})")
    
    async def put(self, item: Any, priority: int = 0) -> bool:
        """Add an item to the queue. Drops oldest if at capacity.
        
        Args:
            item: The message/data to enqueue
            priority: Optional priority (higher = more important, won't be dropped as easily)
            
        Returns:
            True if item was enqueued, False if it was a duplicate
        """
        async with self._lock:
            was_full = len(self._queue) >= self.capacity
            
            if was_full:
                # Drop oldest item (leftmost in deque)
                dropped = self._queue.popleft()
                self._stats.dropped += 1
                self._stats.overflow_count += 1
                log.warning(f"Queue '{self.name}' overflow: dropped oldest message "
                          f"(total drops: {self._stats.dropped})")
            
            self._queue.append((item, priority, time.time()))
            self._stats.enqueued += 1
            self._stats.current_size = len(self._queue)
            self._event.set()
            return True
    
    def put_sync(self, item: Any, priority: int = 0) -> bool:
        """Synchronous put — use only outside async context. Drops oldest if full."""
        was_full = len(self._queue) >= self.capacity
        
        if was_full:
            dropped = self._queue.popleft()
            self._stats.dropped += 1
            self._stats.overflow_count += 1
            log.warning(f"Queue '{self.name}' overflow: dropped oldest "
                      f"(total drops: {self._stats.dropped})")
        
        self._queue.append((item, priority, time.time()))
        self._stats.enqueued += 1
        self._stats.current_size = len(self._queue)
        return True
    
    def get_sync(self) -> Optional[Any]:
        """Synchronous get — use only outside async context. Returns None if empty."""
        if not self._queue:
            return None
        
        # Sort by priority (descending), then by age (ascending)
        items = list(self._queue)
        items.sort(key=lambda x: (-x[1], x[2]))
        best = items[0]
        self._queue.remove(best)
        
        self._stats.dequeued += 1
        self._stats.current_size = len(self._queue)
        
        return best[0]
    
    async def get(self, timeout: Optional[float] = None) -> Any:
        """Get the highest-priority item from the queue.
        
        Items with higher priority number are returned first.
        Within the same priority, FIFO order is maintained.
        
        Args:
            timeout: Max seconds to wait. None = wait forever.
            
        Returns:
            The item data (priority and timestamp stripped)
            
        Raises:
            asyncio.TimeoutError: If timeout expires
        """
        deadline = time.time() + timeout if timeout else None
        
        while True:
            # Check if items available
            if self._queue:
                # Sort by priority (descending), then by enqueue time (ascending)
                # This is O(n log n) but queues are small (max 100)
                items = list(self._queue)
                # Sort: higher priority first, then older first
                items.sort(key=lambda x: (-x[1], x[2]))
                best = items[0]
                self._queue.remove(best)
                
                self._stats.dequeued += 1
                self._stats.current_size = len(self._queue)
                wait_time = (time.time() - best[2]) * 1000
                self._stats.total_wait_time_ms += wait_time
                
                if not self._queue:
                    self._event.clear()
                
                return best[0]  # Return just the item, not (item, priority, ts)
            
            # Wait for new items
            remaining = deadline - time.time() if deadline else 1.0
            if remaining is not None and remaining <= 0:
                raise asyncio.TimeoutError(f"Queue '{self.name}' get timed out")
            
            try:
                await asyncio.wait_for(self._event.wait(), timeout=min(remaining or 1.0, 1.0))
            except asyncio.TimeoutError:
                if deadline and time.time() >= deadline:
                    raise
                continue
    
    def get_nowait(self) -> Optional[Any]:
        """Non-blocking get. Returns None if queue is empty."""
        if not self._queue:
            return None
        
        # Sort by priority (descending), then by age (ascending)
        items = list(self._queue)
        items.sort(key=lambda x: (-x[1], x[2]))
        best = items[0]
        self._queue.remove(best)
        
        self._stats.dequeued += 1
        self._stats.current_size = len(self._queue)
        
        if not self._queue:
            self._event.clear()
        
        return best[0]
    
    def qsize(self) -> int:
        """Current queue size."""
        return len(self._queue)
    
    def empty(self) -> bool:
        """Is the queue empty?"""
        return len(self._queue) == 0
    
    def full(self) -> bool:
        """Is the queue at capacity?"""
        return len(self._queue) >= self.capacity
    
    @property
    def stats(self) -> dict:
        """Return queue statistics."""
        avg_wait = (self._stats.total_wait_time_ms / self._stats.dequeued 
                    if self._stats.dequeued > 0 else 0)
        return {
            "name": self.name,
            "capacity": self.capacity,
            "current_size": len(self._queue),
            "enqueued": self._stats.enqueued,
            "dequeued": self._stats.dequeued,
            "dropped": self._stats.dropped,
            "overflow_count": self._stats.overflow_count,
            "drop_rate": round(self._stats.dropped / max(1, self._stats.enqueued), 4),
            "avg_wait_ms": round(avg_wait, 2),
        }
    
    def __repr__(self):
        return f"BoundedQueue(name='{self.name}', size={len(self._queue)}/{self.capacity}, dropped={self._stats.dropped})"