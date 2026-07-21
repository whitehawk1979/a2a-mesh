"""Test core.router — MeshRouter, priority queue, routing"""
import pytest
from a2a_mesh.core.router import MeshRouter, ProcessResult
from a2a_mesh.core.message import A2AMessage


class TestMeshRouter:
    """Test message routing logic."""

    def test_router_creation(self):
        router = MeshRouter(node_name="nova")
        assert router.node_name == "nova"
        assert len(router.transports) == 0

    def test_register_transport(self):
        from a2a_mesh.transports.base import TransportStatus
        router = MeshRouter(node_name="nova")

        class MockTransport:
            def __init__(self):
                self.name = "mock"
                self._available = True
            def is_available(self):
                return self._available
            def get_status(self):
                return TransportStatus(available=True, latency_ms=1.0)
            async def start(self):
                return True
            async def stop(self):
                return True
            async def send(self, msg):
                return True
            async def receive(self):
                return []

        transport = MockTransport()
        router.register_transport("mock", transport)
        assert "mock" in router.transports

    def test_process_result(self):
        result = ProcessResult(status="processed", message="OK")
        assert result.status == "processed"
        assert result.message == "OK"