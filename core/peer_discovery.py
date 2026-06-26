import asyncio
import json
import logging
import time
import uuid
from typing import Dict, List, Optional, Any

from .agent_card import AgentCard
from .config import MeshConfig

log = logging.getLogger("a2a_mesh.peer_discovery")


class PeerInfo:
    """Information about a discovered peer."""
    def __init__(self, name: str, host: str = "", port: int = 0,
                 p2p_port: int = 0, role: str = "agent",
                 last_seen: float = 0, capabilities: Optional[List[str]] = None,
                 http_port: int = 0, metadata: Optional[Dict] = None):
        self.name = name
        self.host = host
        self.port = port
        self.p2p_port = p2p_port
        self.http_port = http_port
        self.role = role
        self.last_seen = last_seen or time.time()
        self.capabilities = capabilities or []
        self.metadata = metadata or {}


class PeerDiscovery:
    """Dynamic peer discovery for A2A mesh.

    Discovery sources (in priority order):
    1. Static config (discovery.static_nodes)
    2. PG mesh_nodes table (if PG available)
    3. mDNS zeroconf (if enabled)
    4. Health-based peer monitoring

    When a peer is discovered, it's added to the local_store.peer_status
    table and a P2P connection is attempted.
    """

    def __init__(self, node_name: str, config, local_store=None, p2p_transport=None, pg_conn=None, registry=None):
        self.node_name = node_name
        self.config = config
        self.local_store = local_store
        self.p2p_transport = p2p_transport
        self._pg_conn = pg_conn
        self.registry = registry  # AgentRegistry for auto-registration
        self._on_peer_discovered = None  # Callback: async fn(peer_name) called when peer auto-approved

        # Known peers: name → PeerInfo
        self._peers: Dict[str, PeerInfo] = {}

        # Load static peers from config
        self._load_static_peers()

        # Start PG discovery if available
        self._pg_discovery_started = False

        # Start mDNS discovery if enabled
        self._mdns_started = False

        # P2P connection callback
        self._connect_callback = None

    def _load_static_peers(self):
        """Load static peers from config."""
        static_peers = getattr(self.config, 'static_nodes', [])
        if not static_peers:
            # Try from discovery config
            discovery = getattr(self.config, 'discovery', {})
            if isinstance(discovery, dict):
                static_peers = discovery.get('static_peers', [])
        
        for peer_config in static_peers:
            name = peer_config.get('name', '')
            if not name or name == self.node_name:
                continue
            peer = PeerInfo(
                name=name,
                host=peer_config.get('host', ''),
                port=peer_config.get('port', 0),
                p2p_port=peer_config.get('p2p_port', 0),
                http_port=peer_config.get('http_port', 0),
                role=peer_config.get('role', 'agent'),
                capabilities=peer_config.get('capabilities', []),
                metadata=peer_config.get('metadata', {}),
            )
            self._peers[name] = peer
            log.info(f"Loaded static peer: {name} at {peer.host}:{peer.p2p_port}")

    async def start_discovery(self):
        """Start all discovery mechanisms."""
        # Start PG discovery
        if self._pg_conn:
            self._pg_discovery_started = True
            await self._discover_from_pg()

        # Start mDNS discovery
        mdns_enabled = getattr(self.config, 'mdns_enabled', False)
        if mdns_enabled:
            self._mdns_started = True
            # mDNS discovery would be implemented here

        log.info(f"Peer discovery started: {len(self._peers)} static peers, "
                 f"PG={'enabled' if self._pg_conn else 'disabled'}, "
                 f"mDNS={'enabled' if mdns_enabled else 'disabled'}")

    async def _discover_from_pg(self):
        """Discover peers from PG mesh_nodes table."""
        if not self._pg_conn:
            return
        try:
            cur = self._pg_conn.cursor()
            cur.execute("""
                SELECT node_name, host, p2p_port, http_port, capabilities, status
                FROM mesh.mesh_nodes
                WHERE status = 'active' AND node_name != %s
            """, (self.node_name,))
            rows = cur.fetchall()
            for row in rows:
                name = row[0]
                host = row[1]
                p2p_port = row[2]
                http_port = row[3]
                capabilities = row[4] if row[4] else []
                if isinstance(capabilities, str):
                    try:
                        capabilities = json.loads(capabilities)
                    except:
                        capabilities = []
                peer = PeerInfo(
                    name=name,
                    host=host or '',
                    p2p_port=p2p_port or 0,
                    http_port=http_port or 0,
                    capabilities=capabilities if isinstance(capabilities, list) else [],
                )
                if name not in self._peers:
                    self._peers[name] = peer
                    log.info(f"Discovered peer from PG: {name} at {host}:{p2p_port}")
                    self._register_discovered_peer(peer)
                else:
                    # Update existing peer info
                    self._peers[name].host = host or self._peers[name].host
                    self._peers[name].p2p_port = p2p_port or self._peers[name].p2p_port
                    self._peers[name].capabilities = capabilities if isinstance(capabilities, list) else self._peers[name].capabilities
                    self._register_discovered_peer(peer)
            cur.close()
        except Exception as e:
            log.debug(f"PG peer discovery failed: {e}")

    def _register_discovered_peer(self, peer) -> None:
        """Register a discovered peer in the agent registry and trigger P2P connection."""
        # Determine capabilities
        capabilities = list(peer.capabilities or [])
        if self._pg_conn:
            try:
                cur = self._pg_conn.cursor()
                cur.execute("SELECT capabilities FROM mesh.mesh_nodes WHERE node_name = %s", (peer.name,))
                row = cur.fetchone()
                if row:
                    caps = row[0] if isinstance(row[0], list) else (json.loads(row[0]) if isinstance(row[0], str) else [])
                    if isinstance(caps, list) and len(caps) > 0:
                        capabilities = caps
            except Exception as e:
                log.debug(f"Could not load capabilities from PG for {peer.name}: {e}")
        
        card = AgentCard(
            name=peer.name,
            capabilities=capabilities,
            endpoint=f"{peer.host}:{peer.p2p_port}",
            description=f"P2P discovered peer ({peer.role})",
        )
        status = self.registry.request_registration(card)
        if status == "approved":
            log.info(f"Auto-approved discovered peer: {peer.name} caps={capabilities}")
            # Notify node that a new peer was discovered (triggers skills announcement)
            if self._on_peer_discovered:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._on_peer_discovered(peer.name))
                    log.info(f"Triggered on_peer_discovered callback for {peer.name}")
                except RuntimeError:
                    # No running loop
                    log.warning(f"No running event loop for on_peer_discovered callback for {peer.name}")
                except Exception as e:
                    log.warning(f"Could not trigger on_peer_discovered callback: {e}")
        else:
            log.info(f"Discovered peer {peer.name} pending approval (auto_approve=False)")

    def handle_pg_notification(self, action: str, node_name: str, data: Optional[Dict] = None):
        """Handle PG NOTIFY for peer discovery."""
        if action == "register" and node_name != self.node_name:
            host = data.get('host', '') if data else ''
            p2p_port = data.get('p2p_port', 0) if data else 0
            capabilities = data.get('capabilities', []) if data else []
            peer = PeerInfo(name=node_name, host=host, p2p_port=p2p_port, capabilities=capabilities)
            self._peers[node_name] = peer
            self._register_discovered_peer(peer)
        elif action == "deregister" and node_name in self._peers:
            del self._peers[node_name]
            log.info(f"Peer {node_name} deregistered")

    def add_peer(self, name: str, host: str = "", port: int = 0,
                p2p_port: int = 0, capabilities: Optional[List[str]] = None,
                role: str = "agent", metadata: Optional[Dict] = None):
        """Manually add a peer."""
        peer = PeerInfo(name=name, host=host, port=port, p2p_port=p2p_port,
                       capabilities=capabilities, role=role, metadata=metadata)
        self._peers[name] = peer
        self._register_discovered_peer(peer)
        return peer

    def get_peer(self, name: str) -> Optional[PeerInfo]:
        """Get peer info by name."""
        return self._peers.get(name)

    def get_all_peers(self) -> Dict[str, PeerInfo]:
        """Get all known peers."""
        return dict(self._peers)

    async def discover_and_connect(self):
        """Discover peers and attempt P2P connections."""
        await self._discover_from_pg()
        # P2P connection attempts would happen here

    def update_peer_status(self, name: str, status: str):
        """Update a peer's status."""
        if name in self._peers:
            self._peers[name].last_seen = time.time()
            log.debug(f"Updated peer {name} status: {status}")