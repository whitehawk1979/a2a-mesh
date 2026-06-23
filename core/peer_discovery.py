"""A2A Mesh Peer Discovery — Dynamic peer discovery and connection management.

Supports:
- Static node configuration (from YAML)
- mDNS service discovery (zeroconf)
- PG-based discovery (query mesh_nodes table)
- Health-based peer monitoring
- Auto-connect to discovered peers

When a new agent joins the mesh:
1. It registers in the mesh_nodes table in PG
2. Other agents discover it via PG polling or NOTIFY
3. They establish P2P connections directly
4. File transfer and messages work over P2P directly
"""
import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

log = logging.getLogger("a2a_mesh.peer_discovery")


@dataclass
class PeerInfo:
    """Information about a discovered peer."""
    name: str
    host: str
    port: int
    role: str = "router"
    p2p_port: int = 8651
    health_port: int = 8650
    last_seen: float = 0.0
    p2p_available: bool = False
    pg_available: bool = False
    http_available: bool = False
    capabilities: Optional[list] = None  # Agent capabilities from PG

    def __post_init__(self):
        if self.capabilities is None:
            self.capabilities = []

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "role": self.role,
            "p2p_port": self.p2p_port,
            "health_port": self.health_port,
            "last_seen": self.last_seen,
            "p2p_available": self.p2p_available,
            "pg_available": self.pg_available,
            "http_available": self.http_available,
            "capabilities": self.capabilities,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'PeerInfo':
        return cls(
            name=d.get("name", ""),
            host=d.get("host", d.get("ip", "")),
            port=d.get("port", 8651),
            role=d.get("role", "router"),
            p2p_port=d.get("p2p_port", d.get("port", 8651)),
            health_port=d.get("health_port", 8650),
            last_seen=d.get("last_seen", 0.0),
            p2p_available=d.get("p2p_available", False),
            pg_available=d.get("pg_available", False),
            http_available=d.get("http_available", False),
        )


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

        # Known peers: name → PeerInfo
        self._peers: Dict[str, PeerInfo] = {}

        # Load static peers from config
        for node in config.discovery.static_nodes:
            name = node.get("name", "")
            if name and name != node_name:
                peer = PeerInfo(
                    name=name,
                    host=node.get("host", node.get("ip", "")),
                    port=node.get("p2p_port", 8651),
                    role=node.get("role", "router"),
                    p2p_port=node.get("p2p_port", 8651),
                    health_port=node.get("health_port", 8650),
                )
                self._peers[name] = peer
                log.info(f"Loaded static peer: {name} at {peer.host}:{peer.p2p_port}")

        # Discovery state
        self._running = False
        self._discover_task: Optional[asyncio.Task] = None
        self._discover_interval = 30  # seconds

        # Shared HTTP session for health checks (P0: connection pooling)
        self._http_session = None  # type: Optional[aiohttp.ClientSession]

        # PG NOTIFY listener for real-time discovery (P0: replaces polling)
        self._notify_listener_task: Optional[asyncio.Task] = None

    def _register_discovered_peer(self, peer) -> None:
        """Register a discovered peer with the agent registry.

        If auto_approve is True, the peer is registered immediately.
        Otherwise, it goes into pending state for admin approval.
        
        Uses capabilities from: peer.capabilities (PG discovery) > PG mesh_nodes > default.
        """
        from .registry import AgentCard
        
        # Use peer capabilities if available (from PG discovery), else try PG query, else default
        capabilities = list(peer.capabilities) if peer.capabilities else ["a2a_messaging"]
        
        if not peer.capabilities and self._pg_conn and not self._pg_conn.closed:
            try:
                cur = self._pg_conn.cursor()
                cur.execute("""
                    SELECT capabilities FROM mesh.mesh_nodes 
                    WHERE node_name = %s
                """, (peer.name,))
                row = cur.fetchone()
                cur.close()
                if row and row[0]:
                    import json
                    caps = json.loads(row[0]) if isinstance(row[0], str) else row[0]
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
        else:
            log.info(f"Discovered peer {peer.name} pending approval (auto_approve=False)")

    def add_peer(self, name: str, host: str, p2p_port: int = 8651,
                 role: str = "router", health_port: int = 8650) -> PeerInfo:
        """Add or update a peer."""
        peer = PeerInfo(
            name=name, host=host, port=p2p_port, role=role,
            p2p_port=p2p_port, health_port=health_port,
            last_seen=time.time(),
        )
        self._peers[name] = peer

        # Update local store
        if self.local_store:
            self.local_store.update_peer_status(
                node_name=name, role=role, address=f"{host}:{p2p_port}",
                pg_available=True, p2p_available=True,
            )

        log.info(f"Discovered peer: {name} at {host}:{p2p_port}")
        return peer

    def remove_peer(self, name: str):
        """Remove a peer (e.g., on graceful departure)."""
        if name in self._peers:
            del self._peers[name]
            log.info(f"Peer removed: {name}")

    def get_peer(self, name: str) -> Optional[PeerInfo]:
        """Get a peer by name."""
        return self._peers.get(name)

    def get_all_peers(self) -> Dict[str, PeerInfo]:
        """Get all known peers."""
        return self._peers.copy()

    def get_available_peers(self) -> List[PeerInfo]:
        """Get peers that are likely available (seen in last 5 minutes)."""
        cutoff = time.time() - 300  # 5 minutes
        return [
            peer for peer in self._peers.values()
            if peer.last_seen > cutoff
        ]

    async def discover_from_pg(self, pg_conn=None) -> List[PeerInfo]:
        """Discover peers from the mesh_nodes PG table.

        This is the primary discovery mechanism for multi-agent meshes.
        Every agent registers itself in mesh_nodes, and other agents
        discover peers by querying this table.
        """
        if not pg_conn:
            return []

        try:
            cur = pg_conn.cursor()
            cur.execute("""
                SELECT node_name, role, host, p2p_port, health_port,
                       pg_available, p2p_available, http_available,
                       last_heartbeat, capabilities
                FROM mesh.mesh_nodes
                WHERE node_name != %s
                  AND status = 'active'
                  AND last_heartbeat > NOW() - INTERVAL '10 minutes'
                ORDER BY last_heartbeat DESC
            """, (self.node_name,))

            discovered = []
            for row in cur.fetchall():
                name, role, host, p2p_port, health_port = row[0], row[1], row[2], row[3], row[4]
                pg_avail = row[5] if len(row) > 5 else False
                p2p_avail = row[6] if len(row) > 6 else False
                http_avail = row[7] if len(row) > 7 else False
                capabilities = row[9] if len(row) > 9 else None

                if not host:
                    log.debug(f"Skipping peer {name}: no host address")
                    continue

                # Parse capabilities from PG (JSONB)
                import json as _json
                caps = []
                if capabilities:
                    caps = _json.loads(capabilities) if isinstance(capabilities, str) else capabilities
                    if not isinstance(caps, list):
                        caps = []

                if name in self._peers:
                    # Update existing peer
                    self._peers[name].host = host
                    self._peers[name].p2p_port = p2p_port
                    self._peers[name].health_port = health_port
                    self._peers[name].last_seen = time.time()
                    self._peers[name].pg_available = pg_avail
                    self._peers[name].p2p_available = p2p_avail
                    self._peers[name].http_available = http_avail
                    self._peers[name].capabilities = caps
                else:
                    peer = self.add_peer(name, host, p2p_port, role, health_port)
                    peer.pg_available = pg_avail
                    peer.p2p_available = p2p_avail
                    peer.http_available = http_avail
                    peer.capabilities = caps
                    discovered.append(peer)

            cur.close()
            return discovered

        except Exception as e:
            log.warning(f"PG peer discovery failed: {e}")
            return []

    async def check_peer_health(self, peer: PeerInfo) -> bool:
        """Check if a peer is healthy by hitting its health endpoint.
        Uses shared HTTP session for connection pooling (P0 optimization)."""
        import aiohttp
        url = f"http://{peer.host}:{peer.health_port}/health"
        try:
            # P0: Reuse shared session instead of creating new one per call
            if self._http_session is None or self._http_session.closed:
                self._http_session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=3),
                    connector=aiohttp.TCPConnector(limit=10, limit_per_host=5),
                )
            async with self._http_session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    peer.last_seen = time.time()
                    peer.p2p_available = data.get("transports", {}).get("p2p", False)
                    peer.pg_available = data.get("transports", {}).get("pg", False)
                    peer.http_available = data.get("transports", {}).get("http", False)

                    # Update local store
                    if self.local_store:
                        self.local_store.update_peer_status(
                            node_name=peer.name, role=peer.role,
                            address=f"{peer.host}:{peer.p2p_port}",
                            pg_available=peer.pg_available,
                            p2p_available=peer.p2p_available,
                        )
                    return True
        except Exception:
            peer.p2p_available = False
            peer.pg_available = False
            return False

    async def connect_to_peer(self, peer: PeerInfo) -> bool:
        """Establish P2P connection to a peer.

        If the P2P transport is available and the peer isn't already connected,
        attempt a direct connection. Also register the peer with the agent registry.
        """
        if not self.p2p_transport or not self.p2p_transport.is_available():
            log.debug(f"P2P not available, skipping connection to {peer.name}")
            return False

        if peer.name in self.p2p_transport._peers:
            log.debug(f"Already connected to {peer.name}")
            return True  # Already connected

        try:
            log.info(f"P2P: Attempting connection to {peer.name} at {peer.host}:{peer.p2p_port}")
            await self.p2p_transport._connect_to_peer(
                peer.name, peer.host, peer.p2p_port
            )
            log.info(f"P2P connected to peer {peer.name} at {peer.host}:{peer.p2p_port}")
            # Register connected peer with agent registry
            if self.registry:
                self._register_discovered_peer(peer)
            return True
        except Exception as e:
            log.warning(f"Failed to connect to {peer.name}: {e}")
            return False

    async def discover_and_connect(self):
        """Full discovery cycle: find peers and connect.

        1. Discover from PG (primary source)
        2. Check health of known peers
        3. Connect to newly discovered peers (skip already connected)
        """
        # 1. Discover from PG
        pg_conn = self._pg_conn
        if not pg_conn and self.local_store and hasattr(self.local_store, '_pg_conn'):
            pg_conn = self.local_store._pg_conn
        if pg_conn:
            try:
                new_peers = await self.discover_from_pg(pg_conn)
                if new_peers:
                    log.info(f"Discovered {len(new_peers)} new peers from PG: {[p.name for p in new_peers]}")
                    # Auto-register discovered peers with registry
                    if self.registry:
                        for peer in new_peers:
                            self._register_discovered_peer(peer)
            except Exception as e:
                log.warning(f"PG peer discovery failed: {e}")

        # 2. Health check known peers (but don't reconnect if already connected)
        for peer in list(self._peers.values()):
            # Skip health check for already-connected peers — P2P transport handles keepalive
            if self.p2p_transport and peer.name in self.p2p_transport._peers:
                continue
            await self.check_peer_health(peer)

        # 3. Connect to peers that aren't connected yet (skip already-connected)
        connected = 0
        already_connected = 0
        for peer in list(self._peers.values()):
            if not peer.host:
                continue
            # CRITICAL: Skip peers that already have an active P2P connection
            if self.p2p_transport and peer.name in self.p2p_transport._peers:
                already_connected += 1
                continue
            # Skip peers still in backoff (on the P2P transport)
            if self.p2p_transport and peer.name in self.p2p_transport._peer_backoff:
                if time.time() < self.p2p_transport._peer_backoff[peer.name]:
                    continue
            if await self.connect_to_peer(peer):
                connected += 1

        if connected > 0 or already_connected > 0:
            log.debug(f"Discovery: {connected} new, {already_connected} already connected")
        return connected

    async def start(self):
        """Start periodic peer discovery and PG NOTIFY listener (P0: real-time discovery)."""
        self._running = True
        self._discover_task = asyncio.create_task(self._discover_loop())

        # P0: PG NOTIFY for near-instant peer discovery (replaces 30s polling)
        if self._pg_conn:
            try:
                self._notify_listener_task = asyncio.create_task(self._listen_pg_notifications())
                log.info("PG NOTIFY peer discovery listener started")
            except Exception as e:
                log.warning(f"Could not start PG NOTIFY listener: {e}")

        log.info(f"Peer discovery started (interval: {self._discover_interval}s)")

    async def stop(self):
        """Stop peer discovery and cleanup resources."""
        self._running = False
        if self._discover_task:
            self._discover_task.cancel()
            try:
                await self._discover_task
            except asyncio.CancelledError:
                pass
        if self._notify_listener_task:
            self._notify_listener_task.cancel()
            try:
                await self._notify_listener_task
            except asyncio.CancelledError:
                pass
        # P0: Cleanup shared HTTP session
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        log.info("Peer discovery stopped")

    async def _listen_pg_notifications(self):
        """Listen for PG NOTIFY on mesh_node_update channel for real-time peer discovery.
        P0 optimization: Instead of waiting 30s for the next poll cycle,
        immediately discover peers when they register/deregister.
        
        Uses a separate psycopg2 connection in a background thread to avoid
        blocking the asyncio event loop and to ensure thread safety."""
        import asyncio as _asyncio
        import select as _select
        import threading
        
        # Get PG connection config from config (not from DSN, which omits password)
        config = self.config
        if not config or not hasattr(config, 'pg') or not config.pg:
            log.debug("PG NOTIFY: no PG config available, skipping listener")
            return
        
        pg_host = getattr(config.pg, 'host', '192.168.1.30')
        pg_port = getattr(config.pg, 'port', 5432)
        pg_dbname = getattr(config.pg, 'dbname', 'agent_memory')
        pg_user = getattr(config.pg, 'user', 'nova')
        pg_password = getattr(config.pg, 'password', '')
        
        def _listen_sync():
            """Sync PG NOTIFY listener running in a background thread with its own connection."""
            listener_conn = None
            try:
                import psycopg2 as _pg
                listener_conn = _pg.connect(
                    host=pg_host, port=int(pg_port),
                    dbname=pg_dbname, user=pg_user,
                    password=pg_password, sslmode='prefer'
                )
                listener_conn.autocommit = True
                cur = listener_conn.cursor()
                cur.execute("LISTEN mesh_node_update")
                cur.close()
                log.info("Listening for mesh_node_update PG NOTIFY (dedicated thread)")
                
                while self._running:
                    try:
                        # Use select() to poll for PG notifications (1s timeout)
                        if _select.select([listener_conn], [], [], 1.0)[0]:
                            listener_conn.poll()
                        while listener_conn.notifies:
                            msg = listener_conn.notifies.pop(0)
                            if msg.channel == "mesh_node_update":
                                import json as _json
                                try:
                                    data = _json.loads(msg.payload)
                                    node_name = data.get("node", data.get("name", ""))
                                    action = data.get("action", "register")
                                    log.info(f"PG NOTIFY: peer {node_name} action={action}")
                                    if action in ("register", "update", "heartbeat"):
                                        _asyncio.ensure_future(self.discover_and_connect())
                                    elif action in ("deregister", "offline"):
                                        if node_name in self._peers:
                                            del self._peers[node_name]
                                            log.info(f"Removed offline peer: {node_name}")
                                except _json.JSONDecodeError:
                                    log.warning(f"Invalid PG NOTIFY payload: {msg.payload}")
                    except Exception as e:
                        if self._running:
                            log.debug(f"PG NOTIFY listener error: {e}")
                            import time; time.sleep(1)
            except Exception as e:
                log.warning(f"PG NOTIFY listener thread stopped: {e}")
            finally:
                try:
                    if listener_conn is not None:
                        listener_conn.close()
                except Exception:
                    pass

        # Start listener in a background thread (non-blocking for asyncio)
        listener_thread = threading.Thread(target=_listen_sync, daemon=True, name="pg-notify-listener")
        listener_thread.start()
        log.info("PG NOTIFY listener thread started")

    async def _discover_loop(self):
        """Periodic peer discovery loop."""
        while self._running:
            try:
                await self.discover_and_connect()
            except Exception as e:
                log.error(f"Discovery loop error: {e}")
            await asyncio.sleep(self._discover_interval)

    def get_stats(self) -> dict:
        """Return discovery statistics."""
        return {
            "known_peers": len(self._peers),
            "connected_peers": len([p for p in self._peers.values() if p.p2p_available]),
            "available_peers": len(self.get_available_peers()),
            "peers": {name: peer.to_dict() for name, peer in self._peers.items()},
        }