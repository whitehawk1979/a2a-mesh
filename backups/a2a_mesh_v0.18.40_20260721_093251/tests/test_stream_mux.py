"""Tests for core.stream_mux — Content-based message stream routing."""

import json
import pytest

from a2a_mesh.core.stream_mux import (
    StreamMultiplexer, MessageStream,
    A2AStream, MeshControlStream, FileTransferStream, DashboardStream,
    create_default_mux,
)


class TestA2AStream:
    def test_matches_directive(self):
        s = A2AStream()
        data = json.dumps({"type": "directive", "content": "test"}).encode()
        assert s.matches(data) is True

    def test_matches_task(self):
        s = A2AStream()
        data = json.dumps({"type": "task", "payload": "do stuff"}).encode()
        assert s.matches(data) is True

    def test_matches_a2a_message(self):
        s = A2AStream()
        data = json.dumps({"type": "a2a_message"}).encode()
        assert s.matches(data) is True

    def test_matches_delegation(self):
        s = A2AStream()
        data = json.dumps({"type": "delegation", "task_id": "123"}).encode()
        assert s.matches(data) is True

    def test_matches_a2a_flag(self):
        s = A2AStream()
        data = json.dumps({"a2a": True, "content": "test"}).encode()
        assert s.matches(data) is True

    def test_no_match_heartbeat(self):
        s = A2AStream()
        data = json.dumps({"type": "heartbeat"}).encode()
        assert s.matches(data) is False

    def test_no_match_invalid_json(self):
        s = A2AStream()
        assert s.matches(b"\x00\x01\x02") is False

    def test_stream_id(self):
        assert A2AStream().stream_id == "a2a"

    @pytest.mark.asyncio
    async def test_forward_returns_none(self):
        s = A2AStream()
        result = await s.forward({"type": "directive"}, "peer1")
        assert result is None


class TestMeshControlStream:
    def test_matches_heartbeat(self):
        s = MeshControlStream()
        data = json.dumps({"type": "heartbeat"}).encode()
        assert s.matches(data) is True

    def test_matches_discovery(self):
        s = MeshControlStream()
        data = json.dumps({"type": "discovery"}).encode()
        assert s.matches(data) is True

    def test_matches_election(self):
        s = MeshControlStream()
        data = json.dumps({"type": "election"}).encode()
        assert s.matches(data) is True

    def test_matches_ack(self):
        s = MeshControlStream()
        data = json.dumps({"type": "ack"}).encode()
        assert s.matches(data) is True

    def test_matches_ping_pong(self):
        s = MeshControlStream()
        assert s.matches(json.dumps({"type": "ping"}).encode()) is True
        assert s.matches(json.dumps({"type": "pong"}).encode()) is True

    def test_no_match_task(self):
        s = MeshControlStream()
        data = json.dumps({"type": "task"}).encode()
        assert s.matches(data) is False

    def test_stream_id(self):
        assert MeshControlStream().stream_id == "mesh_control"


class TestFileTransferStream:
    def test_matches_file_type(self):
        s = FileTransferStream()
        data = json.dumps({"type": "file", "name": "test.bin"}).encode()
        assert s.matches(data) is True

    def test_matches_file_transfer_flag(self):
        s = FileTransferStream()
        data = json.dumps({"file_transfer": True, "content": "data"}).encode()
        assert s.matches(data) is True

    def test_no_match_task(self):
        s = FileTransferStream()
        data = json.dumps({"type": "task"}).encode()
        assert s.matches(data) is False

    def test_stream_id(self):
        assert FileTransferStream().stream_id == "file_transfer"


class TestDashboardStream:
    def test_matches_dashboard_source(self):
        s = DashboardStream()
        data = json.dumps({"payload": json.dumps({"source": "web_dashboard"})}).encode()
        assert s.matches(data) is True

    def test_matches_dict_payload(self):
        s = DashboardStream()
        data = json.dumps({"payload": {"source": "web_dashboard"}}).encode()
        assert s.matches(data) is True

    def test_no_match_non_dashboard(self):
        s = DashboardStream()
        data = json.dumps({"payload": {"source": "other"}}).encode()
        assert s.matches(data) is False

    def test_stream_id(self):
        assert DashboardStream().stream_id == "dashboard"


class TestStreamMultiplexer:
    def test_register_and_match(self):
        mux = StreamMultiplexer()
        mux.register(A2AStream())
        mux.register(MeshControlStream())
        data = json.dumps({"type": "heartbeat"}).encode()
        stream = mux.match(data)
        assert stream is not None
        assert stream.stream_id == "mesh_control"

    def test_match_priority_order(self):
        mux = StreamMultiplexer()
        mux.register(A2AStream(), priority=1)
        mux.register(MeshControlStream(), priority=2)
        # "directive" matches A2A
        data = json.dumps({"type": "directive"}).encode()
        stream = mux.match(data)
        assert stream.stream_id == "a2a"

    def test_no_match_returns_none(self):
        mux = StreamMultiplexer()
        data = json.dumps({"type": "unknown_type"}).encode()
        assert mux.match(data) is None

    def test_unregister_stream(self):
        mux = StreamMultiplexer()
        mux.register(A2AStream())
        mux.register(MeshControlStream())
        mux.unregister("a2a")
        data = json.dumps({"type": "directive"}).encode()
        stream = mux.match(data)
        assert stream is None  # A2A was unregistered

    def test_match_updates_stats(self):
        mux = StreamMultiplexer()
        mux.register(A2AStream())
        data = json.dumps({"type": "task"}).encode()
        mux.match(data)
        stats = mux.get_stats()
        assert stats["routed"] == 1
        assert stats["by_stream"]["a2a"] == 1

    def test_unmatched_updates_stats(self):
        mux = StreamMultiplexer()
        data = json.dumps({"type": "unknown"}).encode()
        mux.match(data)
        stats = mux.get_stats()
        assert stats["unmatched"] == 1

    @pytest.mark.asyncio
    async def test_route_to_stream(self):
        mux = StreamMultiplexer()
        mux.register(A2AStream())
        data = json.dumps({"type": "directive"}).encode()
        result = await mux.route({"type": "directive"}, data, "peer1")
        # A2AStream.forward returns None
        assert result is None

    @pytest.mark.asyncio
    async def test_route_to_default_handler(self):
        mux = StreamMultiplexer()
        async def default_handler(msg, peer, meta):
            return f"default:{peer}"
        mux.set_default_handler(default_handler)
        data = json.dumps({"type": "unknown"}).encode()
        result = await mux.route({"type": "unknown"}, data, "test_peer")
        assert result == "default:test_peer"

    @pytest.mark.asyncio
    async def test_route_no_handler_returns_none(self):
        mux = StreamMultiplexer()
        data = json.dumps({"type": "unknown"}).encode()
        result = await mux.route({"type": "unknown"}, data, "peer1")
        assert result is None

    def test_stats_after_multiple_routes(self):
        mux = StreamMultiplexer()
        mux.register(A2AStream())
        mux.register(MeshControlStream())
        mux.register(FileTransferStream())

        mux.match(json.dumps({"type": "task"}).encode())
        mux.match(json.dumps({"type": "heartbeat"}).encode())
        mux.match(json.dumps({"type": "file"}).encode())
        mux.match(json.dumps({"type": "directive"}).encode())

        stats = mux.get_stats()
        assert stats["routed"] == 4
        assert stats["total_streams"] == 3


class TestCreateDefaultMux:
    def test_creates_mux_with_builtin_streams(self):
        mux = create_default_mux()
        stats = mux.get_stats()
        assert "mesh_control" in stats["streams"]
        assert "a2a" in stats["streams"]
        assert "file_transfer" in stats["streams"]
        assert "dashboard" in stats["streams"]
        assert stats["total_streams"] == 4

    def test_default_mux_routes_mesh_control(self):
        mux = create_default_mux()
        data = json.dumps({"type": "heartbeat"}).encode()
        stream = mux.match(data)
        assert stream.stream_id == "mesh_control"

    def test_default_mux_routes_a2a(self):
        mux = create_default_mux()
        data = json.dumps({"type": "directive"}).encode()
        stream = mux.match(data)
        assert stream.stream_id == "a2a"

    def test_default_mux_routes_file(self):
        mux = create_default_mux()
        data = json.dumps({"type": "file"}).encode()
        stream = mux.match(data)
        assert stream.stream_id == "file_transfer"
