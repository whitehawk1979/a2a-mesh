"""Test core.message — A2AMessage creation, serialization, validation"""
import pytest
import json
from a2a_mesh.core.message import (
    A2AMessage, MSG_TYPE_DIRECTIVE, MSG_TYPE_HEARTBEAT,
    MAX_MESSAGE_SIZE, COMPRESSION_THRESHOLD
)


class TestA2AMessage:
    """Test A2AMessage creation and serialization."""

    def test_create_basic_message(self):
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={"text": "hello"}, priority=7,
        )
        assert msg.sender == "nova"
        assert msg.recipient == "morzsa"
        assert msg.type == "directive"
        assert msg.priority == 7
        assert msg.id is not None

    def test_create_broadcast_message(self):
        msg = A2AMessage.create(
            sender="nova", recipient="broadcast", msg_type="heartbeat",
            payload={"status": "alive"}, priority=1,
        )
        assert msg.recipient == "broadcast"
        assert msg.priority == 1
        assert msg.is_broadcast()

    def test_direct_message_is_not_broadcast(self):
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={}, priority=5,
        )
        assert not msg.is_broadcast()

    def test_message_serialization_json(self):
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={"key": "value"}, priority=5,
        )
        data = msg.to_dict()
        assert data["sender"] == "nova"
        assert data["recipient"] == "morzsa"
        msg2 = A2AMessage.from_dict(data)
        assert msg2.sender == "nova"
        assert msg2.id == msg.id

    def test_message_serialization_msgpack(self):
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={"key": "value"}, priority=5,
        )
        packed = msg.to_bytes()
        assert isinstance(packed, bytes)
        assert len(packed) > 0

    def test_message_priority_range(self):
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={}, priority=10,
        )
        assert msg.priority == 10
        msg_low = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={}, priority=1,
        )
        assert msg_low.priority == 1

    def test_message_with_nested_payload(self):
        payload = {
            "action": "deploy",
            "params": {"target": "lxc", "version": "1.0"},
            "metadata": {"timestamp": 1234567890},
        }
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload=payload, priority=8,
        )
        data = msg.to_dict()
        msg2 = A2AMessage.from_dict(data)
        assert msg2.payload["action"] == "deploy"
        assert msg2.payload["params"]["target"] == "lxc"

    def test_message_validate_size(self):
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={}, priority=5,
        )
        assert msg.validate_size()

    def test_message_ttl(self):
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload={}, priority=5,
        )
        assert msg.ttl > 0
        msg.decrement_ttl()
        assert msg.ttl >= 0

    def test_message_compression(self):
        large_payload = {"data": "x" * COMPRESSION_THRESHOLD}
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa", msg_type="directive",
            payload=large_payload, priority=5,
        )
        result = msg.compress_payload()
        if result:
            assert len(msg.to_dict()["payload"]) < len(json.dumps(large_payload))