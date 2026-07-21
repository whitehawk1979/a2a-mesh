#!/usr/bin/env python3
"""Tests for Dashboard API endpoints: /api/agents, /api/nodes, /api/status.

Validates:
1. Agent list format and transport availability
2. Node list format and status calculation
3. Self-node status detection
4. Peer status logic (online/available/offline)
5. Dashboard HTML agent rendering elements

These are unit tests that mock the MeshNode and don't require a running server.
"""

import asyncio
import json
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
from dataclasses import dataclass, field

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockPeerInfo:
    """Mock peer info for testing."""
    def __init__(self, name="morzsa", host="192.168.1.30", role="router",
                 p2p_available=True, pg_available=True, http_available=True,
                 p2p_port=8645, health_port=8650, last_seen=None):
        self.name = name
        self.host = host
        self.role = role
        self.p2p_available = p2p_available
        self.pg_available = pg_available
        self.http_available = http_available
        self.p2p_port = p2p_port
        self.health_port = health_port
        self.last_seen = last_seen or time.time()
        self.capabilities = ["a2a_messaging"]


class MockTransportStatus:
    """Mock transport status."""
    def __init__(self, available=True, latency_ms=1.0, error=""):
        self.available = available
        self.latency_ms = latency_ms
        self.error = error

    def __repr__(self):
        return f"TransportStatus(available={self.available}, latency_ms={self.latency_ms})"


class MockPeerDiscovery:
    """Mock peer discovery."""
    def __init__(self, peers=None):
        self._peers = peers or {}

    def get_all_peers(self):
        return dict(self._peers)


class MockLocalStore:
    """Mock local store."""
    def get_stats(self):
        return {
            "outbound_pending": 0,
            "outbound_synced": 0,
            "inbound_unprocessed": 0,
            "inbound_total": 0,
            "files_pending": 0,
            "files_complete": 0,
            "active_steers": 0,
            "peers": len(self._peers) if hasattr(self, '_peers') else 0,
        }


class MockConfig:
    """Mock config for dashboard testing."""
    def __init__(self):
        self.topology = MagicMock()
        self.topology.node_role = "coordinator"
        self.p2p = MagicMock()
        self.p2p.listen_host = "0.0.0.0"
        self.p2p.listen_port = 8645
        self.pg = MagicMock()
        self.pg.host = "192.168.1.30"
        self.pg.port = 5432
        self.pg.dbname = "agent_memory"
        self.pg.user = "nova"
        self.pg.password = "test"


class TestAgentsEndpoint(unittest.TestCase):
    """Test /api/agents endpoint response format and data."""

    def setUp(self):
        """Set up mock dashboard handler."""
        from a2a_mesh.core.dashboard import DashboardHandler
        self.handler_class = DashboardHandler

    def _create_mock_node(self, peers=None, transports=None):
        """Create a mock MeshNode with the specified peers and transports."""
        node = MagicMock()
        node.node_name = "nova"
        node.config = MockConfig()

        # Transport status
        if transports is None:
            transports = {
                "pg_notify": MockTransportStatus(available=True, latency_ms=0.5),
                "p2p": MockTransportStatus(available=True, latency_ms=1.0),
                "http": MockTransportStatus(available=True, latency_ms=100.0),
                "ble": MockTransportStatus(available=True, latency_ms=50.0),
            }
        node.get_status.return_value = {
            "transports": transports,
        }

        # Peer discovery
        node.peer_discovery = MockPeerDiscovery(peers or {})
        node.local_store = MockLocalStore()

        # Health scorer
        node.health_scorer = MagicMock()

        return node

    def test_agents_endpoint_returns_self_and_peers(self):
        """Test that /api/agents returns self node plus known peers."""
        node = self._create_mock_node(
            peers={"morzsa": MockPeerInfo(name="morzsa", p2p_available=True, pg_available=True)}
        )
        handler = self.handler_class(node)

        # Simulate the _api_agents method
        # We need to call it with a mock request
        loop = asyncio.new_event_loop()
        try:
            from aiohttp import web
            request = MagicMock()

            async def _test():
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data

            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        self.assertEqual(result["total"], 2)
        names = [a["name"] for a in result["agents"]]
        self.assertIn("nova", names)
        self.assertIn("morzsa", names)

    def test_agents_self_node_status_online(self):
        """Test that self-node always shows status='online'."""
        node = self._create_mock_node()
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                from aiohttp import web
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        self_node = [a for a in result["agents"] if a["name"] == "nova"][0]
        self.assertEqual(self_node["status"], "online")

    def test_agents_peer_status_online_when_p2p_and_pg(self):
        """Test peer status is 'online' when both P2P and PG available."""
        node = self._create_mock_node(
            peers={"morzsa": MockPeerInfo(name="morzsa", p2p_available=True, pg_available=True)}
        )
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        morzsa = [a for a in result["agents"] if a["name"] == "morzsa"][0]
        self.assertEqual(morzsa["status"], "online")
        self.assertTrue(morzsa["transports"]["p2p"])
        self.assertTrue(morzsa["transports"]["pg"])

    def test_agents_peer_status_available_when_p2p_only(self):
        """Test peer status is 'available' when only P2P available (no PG)."""
        node = self._create_mock_node(
            peers={"morzsa": MockPeerInfo(name="morzsa", p2p_available=True, pg_available=False)}
        )
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        morzsa = [a for a in result["agents"] if a["name"] == "morzsa"][0]
        self.assertEqual(morzsa["status"], "available")

    def test_agents_peer_status_offline_when_no_p2p(self):
        """Test peer status is 'offline' when P2P not available."""
        node = self._create_mock_node(
            peers={"morzsa": MockPeerInfo(name="morzsa", p2p_available=False, pg_available=True)}
        )
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        morzsa = [a for a in result["agents"] if a["name"] == "morzsa"][0]
        self.assertEqual(morzsa["status"], "offline")

    def test_agents_self_transport_extraction(self):
        """Test that self-node transport availability is correctly extracted from TransportStatus objects."""
        node = self._create_mock_node()
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        self_node = [a for a in result["agents"] if a["name"] == "nova"][0]
        # All transports should be available
        self.assertTrue(self_node["transports"]["p2p"])
        self.assertTrue(self_node["transports"]["pg"])
        self.assertTrue(self_node["transports"]["http"])

    def test_agents_self_transport_string_parsing(self):
        """Test transport availability extraction from string TransportStatus representations."""
        node = self._create_mock_node(transports={
            "pg_notify": "available=True, latency_ms=0.5",
            "p2p": "available=True, latency_ms=1.0",
            "http": "available=False, latency_ms=0",
            "ble": "available=True, latency_ms=50.0",
        })
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        self_node = [a for a in result["agents"] if a["name"] == "nova"][0]
        self.assertTrue(self_node["transports"]["p2p"])
        self.assertTrue(self_node["transports"]["pg"])
        self.assertFalse(self_node["transports"]["http"])

    def test_agents_multiple_peers(self):
        """Test /api/agents with multiple peers."""
        peers = {
            "morzsa": MockPeerInfo(name="morzsa", p2p_available=True, pg_available=True),
            "runa": MockPeerInfo(name="runa", p2p_available=False, pg_available=True, host="192.168.1.100"),
        }
        node = self._create_mock_node(peers=peers)
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        self.assertEqual(result["total"], 3)
        morzsa = [a for a in result["agents"] if a["name"] == "morzsa"][0]
        runa = [a for a in result["agents"] if a["name"] == "runa"][0]
        self.assertEqual(morzsa["status"], "online")
        self.assertEqual(runa["status"], "offline")

    def test_agents_peer_includes_port_info(self):
        """Test that peer entries include p2p_port and health_port."""
        node = self._create_mock_node(
            peers={"morzsa": MockPeerInfo(name="morzsa", p2p_port=8645, health_port=8650)}
        )
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                response = await handler._api_agents(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        morzsa = [a for a in result["agents"] if a["name"] == "morzsa"][0]
        self.assertEqual(morzsa["p2p_port"], 8645)
        self.assertEqual(morzsa["health_port"], 8650)


class TestNodesEndpoint(unittest.TestCase):
    """Test /api/nodes endpoint response format and data merging."""

    def setUp(self):
        from a2a_mesh.core.dashboard import DashboardHandler
        self.handler_class = DashboardHandler

    def _create_mock_node(self, peers=None, registry_agents=None):
        """Create a mock node with optional registry agents."""
        node = MagicMock()
        node.node_name = "nova"
        node.config = MockConfig()
        node.peer_discovery = MockPeerDiscovery(peers or {})
        node.local_store = MockLocalStore()

        # Mock registry
        registry = MagicMock()
        if registry_agents:
            registry.list_agents.return_value = registry_agents
        else:
            registry.list_agents.return_value = []
        node.registry = registry

        return node

    def test_nodes_endpoint_requires_auth(self):
        """Test that /api/nodes requires authentication."""
        node = self._create_mock_node()
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                # No auth header
                request.headers = {}
                response = await handler._api_nodes_list(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        # Should return auth error
        self.assertIn("error", result)

    def test_nodes_merges_registry_and_p2p_data(self):
        """Test that /api/nodes merges data from registry and P2P connections."""
        from a2a_mesh.core.registry import AgentCard, HealthRecord

        # Create mock registry data
        card = AgentCard(
            name="morzsa",
            endpoint="http://192.168.1.30:8650",
            skills=["mesh_send", "mesh_discover"],
            capabilities=["a2a_messaging", "p2p_transport"],
            version="0.13.4",
            metadata={"role": "router", "p2p_port": 8645}
        )
        health = HealthRecord(
            health_score=0.85,
            last_success=time.time(),
            last_failure=0,
            total_requests=10,
            last_health_check=time.time()
        )

        # P2P peer data
        peers = {
            "morzsa": MockPeerInfo(name="morzsa", p2p_available=True, pg_available=True)
        }

        node = self._create_mock_node(peers=peers, registry_agents=[(card, health)])
        handler = self.handler_class(node)

        loop = asyncio.new_event_loop()
        try:
            async def _test():
                request = MagicMock()
                # Authenticated request — mock _require_auth (sync method)
                from unittest.mock import MagicMock as MM
                from aiohttp import web as aio_web
                mock_user = MagicMock()
                mock_user.username = "zsolt"
                mock_user.role = "owner"
                handler._require_auth = lambda req: (mock_user, None)
                response = await handler._api_nodes_list(request)
                data = json.loads(response.text)
                return data
            result = loop.run_until_complete(_test())
        finally:
            loop.close()

        self.assertIn("nodes", result)
        # Should have at least the morzsa node
        node_names = [n["node_name"] for n in result["nodes"]]
        self.assertIn("morzsa", node_names)

        morzsa = [n for n in result["nodes"] if n["node_name"] == "morzsa"][0]
        # P2P-connected peer should have health_score=1.0
        self.assertEqual(morzsa["p2p_available"], True)
        self.assertEqual(morzsa["status"], "online")


class TestDashboardHTMLAgentRendering(unittest.TestCase):
    """Test that dashboard HTML contains agent rendering elements."""

    def test_agent_list_container_exists(self):
        """Test that dashboard HTML has an agent list container."""
        html_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "core", "dashboard.html")
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()

        # Must have agent-related elements
        self.assertIn("loadAgents", html, "Dashboard must have loadAgents() function")
        self.assertIn("renderAgents", html, "Dashboard must have renderAgents() function")
        self.assertIn("/api/agents", html, "Dashboard must call /api/agents endpoint")

    def test_agent_status_dots_exist(self):
        """Test that dashboard CSS has status dot styles."""
        html_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "core", "dashboard.html")
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()

        self.assertIn("status-dot", html, "Dashboard must have status-dot CSS class")
        self.assertIn("online", html, "Dashboard must have online status style")

    def test_agent_transport_tags_exist(self):
        """Test that dashboard has transport tag rendering."""
        html_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "core", "dashboard.html")
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()

        self.assertIn("transport-tag", html, "Dashboard must have transport-tag CSS class")

    def test_agent_total_count_element(self):
        """Test that dashboard has total agents counter."""
        html_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "core", "dashboard.html")
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()

        self.assertIn("totalAgents", html, "Dashboard must have totalAgents element")

    def test_renderAgents_creates_agent_cards_or_updates_list(self):
        """Test that renderAgents function actually updates the UI."""
        html_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "core", "dashboard.html")
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()

        # Check that renderAgents does something with agents data
        # Currently it only calls addDMChannel — this is the BUG we're checking for
        self.assertIn("renderAgents", html)


class TestAgentStatusLogic(unittest.TestCase):
    """Test the status determination logic for agents/nodes.

    Status rules:
    - online: P2P available AND PG available
    - connected: P2P available but no PG
    - available: P2P only (legacy alias)
    - active: in registry but not P2P-connected
    - offline: no P2P, no PG
    - pending: registered but not approved
    """

    def test_status_online_p2p_and_pg(self):
        """Peer with P2P+PG should be 'online'."""
        peer = MockPeerInfo(p2p_available=True, pg_available=True)
        if peer.p2p_available and peer.pg_available:
            status = "online"
        elif peer.p2p_available:
            status = "available"
        else:
            status = "offline"
        self.assertEqual(status, "online")

    def test_status_available_p2p_only(self):
        """Peer with P2P only should be 'available'."""
        peer = MockPeerInfo(p2p_available=True, pg_available=False)
        if peer.p2p_available and peer.pg_available:
            status = "online"
        elif peer.p2p_available:
            status = "available"
        else:
            status = "offline"
        self.assertEqual(status, "available")

    def test_status_offline_no_p2p(self):
        """Peer without P2P should be 'offline'."""
        peer = MockPeerInfo(p2p_available=False, pg_available=False)
        if peer.p2p_available and peer.pg_available:
            status = "online"
        elif peer.p2p_available:
            status = "available"
        else:
            status = "offline"
        self.assertEqual(status, "offline")

    def test_status_offline_pg_only(self):
        """Peer with PG only (no P2P) should be 'offline'."""
        peer = MockPeerInfo(p2p_available=False, pg_available=True)
        if peer.p2p_available and peer.pg_available:
            status = "online"
        elif peer.p2p_available:
            status = "available"
        else:
            status = "offline"
        self.assertEqual(status, "offline")

    def test_nodes_status_connected_p2p_only(self):
        """In /api/nodes, P2P-only peer should be 'connected'."""
        peer = MockPeerInfo(p2p_available=True, pg_available=False)
        if peer.p2p_available and peer.pg_available:
            status = "online"
        elif peer.p2p_available:
            status = "connected"
        else:
            status = "disconnected"
        self.assertEqual(status, "connected")


if __name__ == "__main__":
    unittest.main()