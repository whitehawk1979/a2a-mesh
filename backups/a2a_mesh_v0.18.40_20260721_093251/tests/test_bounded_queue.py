"""Tests for core.bounded_queue — BoundedQueue with oldest-drop overflow."""

import asyncio
import pytest

from a2a_mesh.core.bounded_queue import BoundedQueue, QueueStats


class TestBoundedQueueCreation:
    def test_default_capacity(self):
        q = BoundedQueue()
        assert q.capacity == 100
        assert q.name == "default"

    def test_custom_capacity_and_name(self):
        q = BoundedQueue(capacity=10, name="test_q")
        assert q.capacity == 10
        assert q.name == "test_q"

    def test_empty_on_creation(self):
        q = BoundedQueue(capacity=5)
        assert q.empty()
        assert q.qsize() == 0


class TestBoundedQueuePutAndGet:
    @pytest.mark.asyncio
    async def test_put_and_get_single(self):
        q = BoundedQueue(capacity=10)
        await q.put("hello")
        assert q.qsize() == 1
        result = await q.get(timeout=1.0)
        assert result == "hello"
        assert q.empty()

    @pytest.mark.asyncio
    async def test_put_and_get_fifo_order(self):
        q = BoundedQueue(capacity=10)
        await q.put("first")
        await q.put("second")
        await q.put("third")
        assert await q.get(timeout=1.0) == "first"
        assert await q.get(timeout=1.0) == "second"
        assert await q.get(timeout=1.0) == "third"

    @pytest.mark.asyncio
    async def test_priority_ordering(self):
        q = BoundedQueue(capacity=10)
        await q.put("low", priority=1)
        await q.put("high", priority=10)
        await q.put("mid", priority=5)
        # Higher priority should come out first
        assert await q.get(timeout=1.0) == "high"
        assert await q.get(timeout=1.0) == "mid"
        assert await q.get(timeout=1.0) == "low"

    @pytest.mark.asyncio
    async def test_priority_with_same_priority_is_fifo(self):
        q = BoundedQueue(capacity=10)
        await q.put("a", priority=5)
        await q.put("b", priority=5)
        await q.put("c", priority=5)
        assert await q.get(timeout=1.0) == "a"
        assert await q.get(timeout=1.0) == "b"
        assert await q.get(timeout=1.0) == "c"


class TestBoundedQueueOverflow:
    @pytest.mark.asyncio
    async def test_overflow_drops_oldest(self):
        q = BoundedQueue(capacity=3)
        await q.put("old1")
        await q.put("old2")
        await q.put("old3")
        assert q.qsize() == 3
        # Adding 4th item should drop oldest
        await q.put("new4")
        assert q.qsize() == 3
        # "old1" should be dropped
        result = await q.get(timeout=1.0)
        assert result == "old2"

    @pytest.mark.asyncio
    async def test_overflow_increments_drop_count(self):
        q = BoundedQueue(capacity=2)
        await q.put("a")
        await q.put("b")
        await q.put("c")  # overflow: drops "a"
        await q.put("d")  # overflow: drops "b"
        stats = q.stats
        assert stats["dropped"] == 2
        assert stats["overflow_count"] == 2

    @pytest.mark.asyncio
    async def test_multiple_overflow_drops(self):
        q = BoundedQueue(capacity=2)
        for i in range(10):
            await q.put(f"item_{i}")
        assert q.qsize() == 2
        # Only last 2 items remain
        items = []
        while not q.empty():
            items.append(await q.get(timeout=1.0))
        assert len(items) == 2
        assert items[0] == "item_8"
        assert items[1] == "item_9"


class TestBoundedQueueSyncMethods:
    def test_put_sync_and_get_sync(self):
        q = BoundedQueue(capacity=5)
        q.put_sync("hello")
        q.put_sync("world")
        assert q.qsize() == 2
        result = q.get_sync()
        assert result == "hello"

    def test_get_sync_empty(self):
        q = BoundedQueue(capacity=5)
        result = q.get_sync()
        assert result is None

    def test_put_sync_overflow(self):
        q = BoundedQueue(capacity=2)
        q.put_sync("a")
        q.put_sync("b")
        q.put_sync("c")  # overflow
        assert q.stats["dropped"] == 1

    def test_get_nowait(self):
        q = BoundedQueue(capacity=5)
        assert q.get_nowait() is None
        q.put_sync("item")
        assert q.get_nowait() == "item"

    @pytest.mark.asyncio
    async def test_mixed_sync_async(self):
        q = BoundedQueue(capacity=5)
        q.put_sync("sync_item")
        await q.put("async_item")
        assert q.qsize() == 2
        assert q.get_nowait() == "sync_item"
        assert await q.get(timeout=1.0) == "async_item"


class TestBoundedQueueStats:
    @pytest.mark.asyncio
    async def test_stats_tracking(self):
        q = BoundedQueue(capacity=5, name="stats_test")
        await q.put("a")
        await q.put("b")
        await q.get(timeout=1.0)
        stats = q.stats
        assert stats["name"] == "stats_test"
        assert stats["capacity"] == 5
        assert stats["enqueued"] == 2
        assert stats["dequeued"] == 1
        assert stats["current_size"] == 1

    @pytest.mark.asyncio
    async def test_drop_rate_calculation(self):
        q = BoundedQueue(capacity=2)
        await q.put("a")
        await q.put("b")
        await q.put("c")  # 1 drop
        stats = q.stats
        assert stats["enqueued"] == 3
        assert stats["dropped"] == 1
        assert stats["drop_rate"] == pytest.approx(1/3, abs=0.01)


class TestBoundedQueueTimeout:
    @pytest.mark.asyncio
    async def test_get_timeout_raises(self):
        q = BoundedQueue(capacity=5)
        with pytest.raises(asyncio.TimeoutError):
            await q.get(timeout=0.1)

    @pytest.mark.asyncio
    async def test_full_and_empty_checks(self):
        q = BoundedQueue(capacity=2)
        assert q.empty()
        assert not q.full()
        await q.put("a")
        await q.put("b")
        assert q.full()
        assert not q.empty()


class TestBoundedQueueRepr:
    def test_repr(self):
        q = BoundedQueue(capacity=10, name="test_repr")
        assert "test_repr" in repr(q)
        assert "0/10" in repr(q)
