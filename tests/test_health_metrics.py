"""Tests for the public health and Prometheus metrics endpoints.

These tests use aiohttp's test utilities to verify the dashboard's
public endpoints return correct data without authentication.
"""

import asyncio
import json
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from aiohttp.test_utils import TestClient, TestServer
from aiohttp import web


class MockPeerDiscovery:
    """Mock peer discovery for testing."""
    def get_stats(self):
        return {"known_peers": 2, "connected_peers": 2, "available_peers": 2}


class MockRouter:
    """Mock router for testing."""
    def get_stats(self):
        return {
            "sent": 100,
            "received": 200,
            "forwarded": 50,
            "duplicates": 10,
            "errors": 0,
            "dedup": {"size": 500, "max_size": 5000, "ttl_seconds": 300},
        }


class MockNode:
    """Mock mesh node for testing."""
    def __init__(self):
        self.node_name = "test-node"
        self._running = True
        self._start_time = None
        self._resolved_version = "0.20.0"
        self.peer_discovery = MockPeerDiscovery()
        self.router = MockRouter()
        self.config = Mock()
        self.config.health_port = 8650
        
    def _get_uptime(self):
        import time
        if self._start_time:
            return int(time.time() - self._start_time)
        return 0


@pytest.fixture
def mock_node():
    return MockNode()


@pytest.fixture
def dashboard_handler(mock_node):
    """Create a DashboardHandler with mock node."""
    from a2a_mesh.core.dashboard import DashboardHandler
    handler = DashboardHandler(mock_node)
    return handler


class TestHealthEndpoint:
    """Test the /api/health endpoint."""

    @pytest.mark.asyncio
    async def test_health_returns_json(self, dashboard_handler):
        """Health endpoint returns valid JSON with required fields."""
        request = Mock()
        response = await dashboard_handler._api_public_health(request)
        data = json.loads(response.text)
        
        assert data["status"] == "healthy"
        assert data["node"] == "test-node"
        assert data["running"] is True
        assert "uptime" in data
        assert "peers" in data
        assert data["peers"]["known"] == 2
        assert data["peers"]["connected"] == 2
        assert data["version"] == "0.20.0"
        assert "timestamp" in data

    @pytest.mark.asyncio
    async def test_health_unhealthy_when_not_running(self, dashboard_handler):
        """Health endpoint reports unhealthy when node is not running."""
        dashboard_handler.node._running = False
        request = Mock()
        response = await dashboard_handler._api_public_health(request)
        data = json.loads(response.text)
        
        assert data["status"] == "unhealthy"
        assert data["running"] is False

    @pytest.mark.asyncio
    async def test_health_no_auth_required(self, dashboard_handler):
        """Health endpoint does not require authentication."""
        request = Mock()
        response = await dashboard_handler._api_public_health(request)
        # Should not return 401
        assert response.status != 401
        assert response.status == 200


class TestMetricsEndpoint:
    """Test the /metrics (Prometheus) endpoint."""

    @pytest.mark.asyncio
    async def test_metrics_returns_text(self, dashboard_handler):
        """Metrics endpoint returns text/plain content."""
        request = Mock()
        response = await dashboard_handler._api_prometheus_metrics(request)
        assert response.content_type == "text/plain"
        assert response.status == 200

    @pytest.mark.asyncio
    async def test_metrics_contains_required_metrics(self, dashboard_handler):
        """Metrics endpoint returns all required Prometheus metrics."""
        request = Mock()
        response = await dashboard_handler._api_prometheus_metrics(request)
        text = response.text
        
        # Check for required metrics
        required_metrics = [
            "a2a_mesh_node_up",
            "a2a_mesh_uptime_seconds",
            "a2a_mesh_peers_connected",
            "a2a_mesh_peers_known",
            "a2a_mesh_messages_sent_total",
            "a2a_mesh_messages_received_total",
            "a2a_mesh_messages_forwarded_total",
            "a2a_mesh_messages_duplicated_total",
            "a2a_mesh_transport_errors_total",
            "a2a_mesh_dedup_cache_size",
        ]
        for metric in required_metrics:
            assert metric in text, f"Missing metric: {metric}"

    @pytest.mark.asyncio
    async def test_metrics_contains_node_label(self, dashboard_handler):
        """Metrics include node label."""
        request = Mock()
        response = await dashboard_handler._api_prometheus_metrics(request)
        text = response.text
        
        assert 'node="test-node"' in text

    @pytest.mark.asyncio
    async def test_metrics_node_up_value(self, dashboard_handler):
        """Node up metric is 1 when running."""
        request = Mock()
        response = await dashboard_handler._api_prometheus_metrics(request)
        text = response.text
        
        # Find the node_up line
        for line in text.split("\n"):
            if line.startswith("a2a_mesh_node_up{"):
                assert "} 1" in line
                return
        pytest.fail("a2a_mesh_node_up metric not found")

    @pytest.mark.asyncio
    async def test_metrics_node_down_value(self, dashboard_handler):
        """Node up metric is 0 when not running."""
        dashboard_handler.node._running = False
        request = Mock()
        response = await dashboard_handler._api_prometheus_metrics(request)
        text = response.text
        
        for line in text.split("\n"):
            if line.startswith("a2a_mesh_node_up{"):
                assert "} 0" in line
                return
        pytest.fail("a2a_mesh_node_up metric not found")

    @pytest.mark.asyncio
    async def test_metrics_no_auth_required(self, dashboard_handler):
        """Metrics endpoint does not require authentication."""
        request = Mock()
        response = await dashboard_handler._api_prometheus_metrics(request)
        assert response.status != 401
        assert response.status == 200

    @pytest.mark.asyncio
    async def test_metrics_contains_help_and_type(self, dashboard_handler):
        """Metrics include HELP and TYPE annotations per Prometheus spec."""
        request = Mock()
        response = await dashboard_handler._api_prometheus_metrics(request)
        text = response.text
        
        assert "# HELP" in text
        assert "# TYPE" in text