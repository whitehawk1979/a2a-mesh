"""Test core.auto_steer — Priority-based message dispatch and steer processing"""
import pytest
from a2a_mesh.core.auto_steer import AutoSteerProcessor, SteerDirective
from a2a_mesh.core.message import A2AMessage, MSG_TYPE_DIRECTIVE, MSG_TYPE_STEER


class TestAutoSteerClassification:
    """Test message priority classification."""

    def setup_method(self):
        self.processor = AutoSteerProcessor(node_name="test_node")

    def test_p10_is_interrupt(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_DIRECTIVE, priority=10, payload={"action": "restart"})
        assert self.processor.classify_message(msg) == "interrupt"

    def test_p7_is_high(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_DIRECTIVE, priority=7, payload={})
        assert self.processor.classify_message(msg) == "high"

    def test_p5_is_normal(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_DIRECTIVE, priority=5, payload={})
        assert self.processor.classify_message(msg) == "normal"

    def test_p1_is_normal(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_DIRECTIVE, priority=1, payload={})
        assert self.processor.classify_message(msg) == "normal"


class TestAutoSteerProcessing:
    """Test message dispatch processing."""

    def setup_method(self):
        self.processor = AutoSteerProcessor(node_name="test_node")

    @pytest.mark.asyncio
    async def test_normal_priority_queues(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_DIRECTIVE, priority=3, payload={"text": "hello"})
        action = await self.processor.process_message(msg)
        assert action == "normal"
        assert self.processor._stats["normal_priority_queued"] == 1

    @pytest.mark.asyncio
    async def test_high_priority_dispatches(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_DIRECTIVE, priority=8, payload={"text": "urgent"})
        action = await self.processor.process_message(msg)
        assert action == "high"
        assert self.processor._stats["high_priority_queued"] == 1

    @pytest.mark.asyncio
    async def test_steer_directive(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_STEER, priority=5, payload={"action": "reboot", "params": {"delay": 60}})
        action = await self.processor.process_message(msg)
        assert action == "steer_queued"
        assert self.processor._stats["steers_received"] == 1

    @pytest.mark.asyncio
    async def test_steer_interrupt(self):
        msg = A2AMessage.create(sender="morzsa", recipient="nova", msg_type=MSG_TYPE_STEER, priority=10, payload={"action": "shutdown", "params": {}})
        action = await self.processor.process_message(msg)
        assert action == "steer_interrupt"
        assert self.processor._stats["steers_received"] == 1
        assert self.processor._stats["interrupts_triggered"] == 1


class TestSteerDirective:
    """Test steer directive tracking."""

    def setup_method(self):
        self.processor = AutoSteerProcessor(node_name="test_node")

    def test_steer_status_update(self):
        self.processor._active_steers["test-1"] = SteerDirective(
            id="test-1", sender="morzsa", action="reboot", status="executing"
        )
        self.processor.update_steer_status("test-1", "completed", result="OK")
        assert self.processor._active_steers["test-1"].status == "completed"
        assert self.processor._active_steers["test-1"].result == "OK"
        assert self.processor._stats["steers_completed"] == 1

    def test_steer_failure_tracking(self):
        self.processor._active_steers["test-2"] = SteerDirective(
            id="test-2", sender="morzsa", action="deploy", status="executing"
        )
        self.processor.update_steer_status("test-2", "failed", result="timeout")
        assert self.processor._stats["steers_failed"] == 1

    def test_cleanup_old_steers(self):
        import time
        old = SteerDirective(id="old-1", sender="morzsa", action="test", status="completed")
        old.received_at = time.time() - 7200  # 2h ago
        recent = SteerDirective(id="recent-1", sender="morzsa", action="test", status="pending")
        recent.received_at = time.time()
        self.processor._active_steers["old-1"] = old
        self.processor._active_steers["recent-1"] = recent
        self.processor.cleanup_old_steers(max_age_seconds=3600)
        assert "old-1" not in self.processor._active_steers
        assert "recent-1" in self.processor._active_steers

    def test_get_stats(self):
        stats = self.processor.get_stats()
        assert "interrupts_triggered" in stats
        assert "steers_received" in stats
        assert "steers_completed" in stats
        assert "steers_failed" in stats
        assert "high_priority_queued" in stats
        assert "normal_priority_queued" in stats
        assert "interrupt_threshold" in stats
        assert "active_steers" in stats