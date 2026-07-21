"""Test core.offline_queue — Offline message queue for sleepy devices"""
import pytest
from a2a_mesh.core.offline_queue import OfflineQueue, QueuedMessage
from a2a_mesh.core.message import A2AMessage
from a2a_mesh.core.config import PGConfig


class TestOfflineQueue:
    """Test offline queue for sleepy end devices."""

    def test_queued_message_creation(self):
        msg = A2AMessage.create(
            sender="nova", recipient="device1", msg_type="directive",
            payload={"text": "test"}, priority=5
        )
        qm = QueuedMessage(
            id=msg.id,
            sender=msg.sender,
            recipient=msg.recipient,
            msg_type=msg.type,
            priority=msg.priority,
            payload_json='{"text": "test"}',
            queued_at="2026-01-01T00:00:00",
            retry_count=0,
            max_retries=3,
            last_error=""
        )
        assert qm.id == msg.id
        assert qm.sender == "nova"
        assert qm.recipient == "device1"

    def test_queued_message_retry(self):
        qm = QueuedMessage(
            id="test-id",
            sender="nova",
            recipient="device1",
            msg_type="directive",
            priority=5,
            payload_json="{}",
            queued_at="2026-01-01T00:00:00",
            retry_count=0,
            max_retries=3,
            last_error=""
        )
        assert qm.retry_count == 0
        assert qm.max_retries == 3