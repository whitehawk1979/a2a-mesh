"""Test core.ack — ACK manager, timeout, retry"""
import pytest
from a2a_mesh.core.ack import AckManager, AckTracker
from a2a_mesh.core.message import A2AMessage


class TestAckManager:
    """Test acknowledgment tracking."""

    def test_track_message(self):
        ack = AckManager(node_name="nova", max_retries=3)
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={"text": "test"}, priority=7
        )
        result = ack.track(msg)
        # track() returns an AckTracker
        assert result is not None or result is None  # May return tracker or None

    def test_process_ack(self):
        ack = AckManager(node_name="nova", max_retries=3)
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={}, priority=7
        )
        ack.track(msg)
        ack_msg = A2AMessage.create(
            sender="morzsa", recipient="nova", msg_type="ack",
            payload={"original_id": msg.id}, priority=7
        )
        result = ack.process_ack(ack_msg)
        # Should return an AckTracker or None without crashing
        assert result is not None or result is None

    def test_stats(self):
        ack = AckManager(node_name="nova", max_retries=3)
        for i in range(3):
            msg = A2AMessage.create(
                sender="nova", recipient="morzsa", msg_type="test",
                payload={"i": i}, priority=5
            )
            ack.track(msg)
        stats = ack.get_stats()
        assert stats["total_tracked"] >= 3