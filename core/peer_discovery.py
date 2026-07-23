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
    p2p_port: int = 8645
    health_port: int = 8650
    last_seen: float = 0.0
    p2p_available: bool = False
    pg_available: bool = False
    http_available: bool = False
    capabilities: Optional[list] = None  # Agent capabilities from PG
    version: str | None = None  # Mesh version — set from PG or P2P handshake

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
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'PeerInfo':
        return cls(
            name=d.get("name", ""),
            host=d.get("host", d.get("ip", "")),
            port=d.get("port", 8645),
            role=d.get("role", "router"),
            p2p_port=d.get("p2p_port", d.get("port", 8645)),
            health_port=d.get("health_port", 8650),
            last_seen=d.get("last_seen", 0.0),
            p2p_available=d.get("p2p_available", False),
            pg_available=d.get("pg_available", False),
            http_available=d.get("http_available", False),
            version=d.get("version"),  # None if missing — resolved from PG later
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
        self._on_peer_discovered = None  # Callback: async fn(peer_name) called when peer auto-approved

        # Known peers: name → PeerInfo
        self._peers: Dict[str, PeerInfo] = {}
        # Thread-safe event queue for PG NOTIFY → async loop communication
        self._pg_event_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

        # Load static peers from config
        for node in config.discovery.static_nodes:
            name = node.get("name", "")
            if name and name != node_name:
                peer = PeerInfo(
                    name=name,
                    host=node.get("host", node.get("ip", "")),
                    port=node.get("p2p_port", 8645),
                    role=node.get("role", "router"),
                    p2p_port=node.get("p2p_port", 8645),
                    health_port=node.get("health_port", 8650),
                    version=node.get("version"),  # None if missing
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
        # Filter out non-hashable items (dicts) from capabilities to prevent TypeError
        _raw_caps = list(peer.capabilities) if peer.capabilities else ["a2a_messaging"]
        capabilities = [c for c in _raw_caps if isinstance(c, (str, int, float, tuple))]
        if not capabilities:
            capabilities = ["a2a_messaging"]
        
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
                        # Filter out non-hashable items (dicts) from PG capabilities
                        capabilities = [c for c in caps if isinstance(c, (str, int, float, tuple))]
                        if not capabilities:
                            capabilities = ["a2a_messaging"]
            except Exception as e:
                log.debug(f"Could not load capabilities from PG for {peer.name}: {e}")
        
        # Determine version — prefer PG over stale "1.0.0" default
        peer_version = getattr(peer, 'version', None)
        pg_skills_list = None  # Skills loaded from PG
        # Always try PG for the real version (peer.version may be stale "1.0.0" default)
        if self._pg_conn and not self._pg_conn.closed:
            try:
                cur2 = self._pg_conn.cursor()
                cur2.execute("SELECT version, skills FROM mesh.mesh_nodes WHERE node_name = %s", (peer.name,))
                vrow = cur2.fetchone()
                cur2.close()
                if vrow and vrow[0] and vrow[0] != '1.0.0':
                    peer_version = vrow[0]
                # Also load skills from PG if available
                if vrow and vrow[1]:
                    import json
                    pg_skills = json.loads(vrow[1]) if isinstance(vrow[1], str) else vrow[1]
                    if isinstance(pg_skills, list) and len(pg_skills) > 0:
                        # Convert string skill IDs to minimal skill dicts for AgentCard
                        skills_list = []
                        for s in pg_skills:
                            if isinstance(s, str):
                                skills_list.append({"id": s, "name": s, "description": f"Skill {s}"})
                            elif isinstance(s, dict):
                                skills_list.append(s)
                        if skills_list:
                            pg_skills_list = skills_list
            except Exception:
                pass
        # Fallback: use peer version if available, else use the mesh_nodes PG version
        if not peer_version:
            # Try PG one more time for this specific node
            try:
                if self._pg_conn and not self._pg_conn.closed:
                    with self._pg_conn.cursor() as cur:
                        cur.execute("SELECT version FROM mesh.mesh_nodes WHERE node_name = %s", (peer.name,))
                        row = cur.fetchone()
                        if row and row[0]:
                            peer_version = row[0]
            except Exception:
                pass
        if not peer_version:
            peer_version = getattr(peer, 'version', None) or 'unknown'
        
        # Update PeerInfo version
        peer.version = peer_version

        card = AgentCard(
            name=peer.name,
            capabilities=capabilities,
            version=peer_version,
            endpoint=f"{peer.host}:{peer.p2p_port}",
            description=f"P2P discovered peer ({peer.role})",
            skills=pg_skills_list if pg_skills_list else None,
        )
        status = self.registry.request_registration(card)
        if status == "approved":
            log.info(f"Auto-approved discovered peer: {peer.name} caps={capabilities}")
            # Notify node that a new peer was discovered (triggers skills announcement)
            if self._on_peer_discovered:
                try:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(self._on_peer_discovered(peer.name))
                    except RuntimeError:
                        log.warning("No running event loop for on_peer_discovered callback")
                    log.info(f"Triggered on_peer_discovered callback for {peer.name}")
                except Exception as e:
                    log.warning(f"Could not trigger on_peer_discovered callback: {e}")
        else:
            log.info(f"Discovered peer {peer.name} pending approval (auto_approve=False)")

    def add_peer(self, name: str, host: str, p2p_port: int = 8645,
                 role: str = "router", health_port: int = 8650,
                 capabilities: Optional[list] = None,
                 version: Optional[str] = None,
                 port: int = 0) -> PeerInfo:
        """Add or update a peer and register it with the agent registry."""
        peer = PeerInfo(
            name=name, host=host, port=port or p2p_port, role=role,
            p2p_port=p2p_port, health_port=health_port,
            last_seen=time.time(),
            capabilities=capabilities or [],
            version=version,
        )
        self._peers[name] = peer

        # Update local store
        if self.local_store:
            self.local_store.update_peer_status(
                node_name=name, role=role, address=f"{host}:{p2p_port}",
                pg_available=True, p2p_available=True,
            )

        # Register with agent registry so peers appear in dashboard
        self._register_discovered_peer(peer)

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

    @staticmethod
    def _detect_local_ip() -> str:
        """Detect the primary LAN IP address robustly.
        
        Uses UDP socket trick: create a connection to a public IP without sending data,
        then read the local address. This returns the actual LAN IP instead of
        127.0.1.1 which gethostbyname(gethostname()) returns on many Linux systems.
        """
        import socket
        try:
            # Create a UDP socket and "connect" to a public IP (no data sent)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(2)
            # Use Google's public DNS as target — we don't actually send anything
            s.connect(('8.8.8.8', 80))
            local_ip = s.getsockname()[0]
            s.close()
            if local_ip and not local_ip.startswith('127.'):
                return local_ip
        except (OSError, socket.timeout):
            pass
        # Fallback: try gethostname
        try:
            ip = socket.gethostbyname(socket.gethostname())
            if ip and not ip.startswith('127.'):
                return ip
        except (socket.gaierror, OSError):
            pass
        # Last resort
        return '0.0.0.0'

    def resolve_peer_address(self, name: str) -> Optional[Tuple[str, int]]:
        """Resolve a peer name to (host, p2p_port) for direct P2P connection.

        P2: Used by P2PTransport.send() to dynamically connect to a peer
        that is not currently connected but known via peer_discovery.

        Returns:
            (host, p2p_port) if peer is known and available, None otherwise.
        """
        peer = self._peers.get(name)
        if peer is None:
            log.debug(f"Peer address resolver: peer '{name}' not found in discovery")
            return None

        # Check if peer was seen recently (within 10 minutes)
        if peer.last_seen > 0 and (time.time() - peer.last_seen) > 600:
            log.debug(f"Peer address resolver: peer '{name}' last seen {time.time() - peer.last_seen:.0f}s ago (stale)")
            return None

        host = peer.host
        port = peer.p2p_port
        # P2 SECURITY: Do NOT fall back to health_port for P2P connections
        # P2P (binary TCP framing) and health (HTTP) are different protocols
        # Connecting P2P client to an HTTP port causes protocol mismatch/hang

        if not host or not port:
            log.debug(f"Peer address resolver: peer '{name}' has no address (host={host}, port={port})")
            return None

        log.info(f"Peer address resolver: {name} → {host}:{port}")
        return (host, port)

    def approve_peer(self, name: str) -> bool:
        """Auto-approve a discovered peer for P2P connection."""
        peer = self._peers.get(name)
        if not peer:
            return False
        log.info(f"Auto-approved discovered peer: {name} caps={peer.capabilities or ['a2a_messaging']}")
        self._register_discovered_peer(peer)
        return True

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
                       last_heartbeat, capabilities, version
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
                version = row[10] if len(row) > 10 else None

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
                    # Filter out non-hashable items (dicts) from capabilities
                    caps = [c for c in caps if isinstance(c, (str, int, float, tuple))]

                if name in self._peers:
                    # Update existing peer — but preserve live status if peer is connected
                    # PG data may be stale; live P2P connection is ground truth for p2p_available
                    self._peers[name].host = host
                    self._peers[name].port = p2p_port  # Keep port in sync with p2p_port
                    self._peers[name].p2p_port = p2p_port
                    self._peers[name].health_port = health_port
                    self._peers[name].last_seen = time.time()
                    # Only downgrade PG status from PG data if we don't have a live P2P connection
                    # If peer is P2P-connected, PG is reachable (we share the same PG)
                    if self.p2p_transport and name in self.p2p_transport._peers:
                        self._peers[name].p2p_available = True
                        self._peers[name].pg_available = True  # Same PG = PG available
                    elif self._peers[name].p2p_available and not p2p_avail:
                        # P2 FIX: Don't downgrade p2p_available from PG data if health check
                        # already confirmed it. PG data can be stale; health check is live truth.
                        # Only update pg_available from PG data (which reflects actual PG reachability)
                        log.debug(f"Discovery: {name} p2p_available=True preserved (PG says False, but health check confirmed)")
                        self._peers[name].pg_available = pg_avail
                    else:
                        self._peers[name].pg_available = pg_avail
                        self._peers[name].p2p_available = p2p_avail
                    self._peers[name].http_available = http_avail
                    self._peers[name].capabilities = caps
                    if version:
                        self._peers[name].version = version
                    # Re-register with registry to keep dashboard in sync
                    self._register_discovered_peer(self._peers[name])
                else:
                    peer = self.add_peer(name, host, p2p_port, role, health_port, capabilities=caps)
                    peer.pg_available = pg_avail
                    peer.p2p_available = p2p_avail
                    peer.http_available = http_avail
                    if version:
                        peer.version = version
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
                    # P2 FIX: If peer is P2P-connected to us, PG is available (shared mesh)
                    # Don't downgrade pg_available from True based on peer's internal health
                    p2p_from_health = data.get("transports", {}).get("p2p", False)
                    pg_from_health = data.get("transports", {}).get("pg", False)
                    is_p2p_connected = self.p2p_transport and peer.name in self.p2p_transport._peers
                    peer.p2p_available = bool(p2p_from_health or (is_p2p_connected and peer.p2p_available))
                    peer.pg_available = bool(pg_from_health or (is_p2p_connected and peer.pg_available) or peer.pg_available)
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
        except Exception as e:
            log.warning(f"Health check failed for {peer.name} at {url}: {e}")
            # P2 FIX: Don't immediately mark all transports as False on health check failure.
            # The peer might be temporarily unreachable (network blip, firewall).
            # Only mark p2p_available as False if we had it as True (was connected, now can't reach)
            # Keep pg_available from PG data (already set by discover_from_pg)
            peer.p2p_available = False
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
                log.info(f"PG discovery cycle complete: {len(new_peers)} new peers, {len(self._peers)} known total")
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
            if self.p2p_transport and peer.name in self.p2p_transport._peers:
                # Already connected via P2P — update status from local knowledge
                # P2P keepalive is sufficient; no need for HTTP health check
                peer.p2p_available = True
                peer.last_seen = time.time()
                # If peer is P2P-connected, they share our PG instance
                # (all mesh nodes use the same PG for transport)
                peer.pg_available = True
                log.info(f"Discovery: {peer.name} P2P-connected, set pg_available=True (shared mesh)")
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
                peer.p2p_available = True
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
        """Start periodic peer discovery, PG NOTIFY listener, and mDNS discovery."""
        self._running = True
        self._discover_task = asyncio.create_task(self._discover_loop())

        # P2 FIX: Process PG NOTIFY events in the async loop (thread-safe)
        self._pg_event_task = asyncio.create_task(self._pg_event_processor())

        # P0: PG NOTIFY for near-instant peer discovery (replaces 30s polling)
        if self._pg_conn:
            try:
                self._notify_listener_task = asyncio.create_task(self._listen_pg_notifications())
                log.info("PG NOTIFY peer discovery listener started")
            except Exception as e:
                log.warning(f"Could not start PG NOTIFY listener: {e}")

        # mDNS service discovery (if enabled in config)
        mdns_config = getattr(self.config, 'discovery', None)
        mdns_enabled = False
        if mdns_config:
            mdns_data = getattr(mdns_config, 'mdns', None)
            if isinstance(mdns_data, dict):
                mdns_enabled = mdns_data.get('enabled', False)
            elif hasattr(mdns_data, 'enabled'):
                mdns_enabled = mdns_data.enabled
        if mdns_enabled:
            try:
                self._start_mdns()
                log.info("mDNS peer discovery started")
            except Exception as e:
                log.warning(f"Could not start mDNS discovery: {e}")

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
        # mDNS cleanup
        if hasattr(self, '_mdns_zeroconf') and self._mdns_zeroconf:
            try:
                self._mdns_zeroconf.unregister_service(self._mdns_service_info)
            except Exception:
                pass
            self._mdns_zeroconf.close()
            self._mdns_zeroconf = None
        log.info("Peer discovery stopped")

    # ─── mDNS Service Discovery ────────────────────────────────────────

    def _start_mdns(self):
        """Register this node as an mDNS service and browse for peers.

        Advertises _a2a._tcp on the local network so other mesh nodes
        can discover each other automatically without static config or PG.
        """
        try:
            from zeroconf import Zeroconf, ServiceInfo, ServiceBrowser
        except ImportError:
            log.warning("zeroconf not installed — mDNS discovery disabled. Run: pip install zeroconf")
            return

        mdns_config = getattr(self.config, 'discovery', None)
        mdns_data = getattr(mdns_config, 'mdns', None) if mdns_config else {}
        if isinstance(mdns_data, dict):
            service_type = mdns_data.get('service', '_a2a._tcp')
            mdns_port = mdns_data.get('port', 8645)
        elif hasattr(mdns_data, 'service'):
            service_type = getattr(mdns_data, 'service', '_a2a._tcp')
            mdns_port = getattr(mdns_data, 'port', 8645)
        else:
            service_type = '_a2a._tcp'
            mdns_port = 8645

        # Use the actual P2P listen port, not the config mdns port
        if hasattr(self.config, 'p2p') and hasattr(self.config.p2p, 'listen_port'):
            mdns_port = self.config.p2p.listen_port

        self._mdns_zeroconf = Zeroconf()
        self._mdns_service_type = service_type

        # Register this node
        import socket
        # P2 FIX: Robust IP detection — gethostbyname(gethostname()) often returns 127.0.1.1 on Linux
        local_ip = self._detect_local_ip()
        # Prefer the configured host if available
        if hasattr(self.config, 'p2p') and hasattr(self.config.p2p, 'listen_host'):
            host = self.config.p2p.listen_host
            if host and host != '0.0.0.0':
                local_ip = host

        # Build TXT records with node metadata
        txt_records = {
            'node_name': self.node_name.encode('utf-8'),
            'role': getattr(self.config, 'node_role', 'router').encode('utf-8'),
            'health_port': str(getattr(self.config, 'health_port', 8650)).encode('utf-8'),
        }

        self._mdns_service_info = ServiceInfo(
            service_type,
            name=f"{self.node_name}.{service_type}",
            addresses=[socket.inet_aton(local_ip)],
            port=mdns_port,
            properties=txt_records,
        )

        self._mdns_zeroconf.register_service(self._mdns_service_info)
        log.info(f"mDNS: Registered {self.node_name} at {local_ip}:{mdns_port} as {service_type}")

        # Browse for other mesh nodes
        self._mdns_browser = ServiceBrowser(self._mdns_zeroconf, service_type, handlers=[self._on_mdns_service_state_change])
        log.info(f"mDNS: Browsing for {service_type} services on local network")

    def _on_mdns_service_state_change(self, zeroconf, service_type, name, state_change):
        """Handle mDNS service discovery events.

        When a new A2A mesh node appears on the network, add it as a peer
        and attempt P2P connection.
        """
        from zeroconf import ServiceStateChange
        if state_change == ServiceStateChange.Added:
            # Resolve service info to get IP and port
            info = zeroconf.get_service_info(service_type, name)
            if info is None:
                return

            # Extract node name from service name (e.g., "morzsa._a2a._tcp.local.")
            peer_name = name.split('.')[0]
            if peer_name == self.node_name:
                return  # Skip self

            # Get IP address
            addresses = info.addresses
            if not addresses:
                return
            import socket
            peer_host = socket.inet_ntoa(addresses[0])

            # Get port and metadata from TXT records
            peer_port = info.port
            peer_role = 'router'
            peer_health_port = 8650
            if info.properties:
                peer_role = info.properties.get(b'role', b'router').decode('utf-8', errors='ignore')
                peer_health_port = int(info.properties.get(b'health_port', b'8650').decode('utf-8', errors='ignore'))

            # Add as peer if not already known
            if peer_name not in self._peers:
                peer = self.add_peer(
                    name=peer_name,
                    host=peer_host,
                    p2p_port=peer_port,
                    role=peer_role,
                    health_port=peer_health_port,
                )
                log.info(f"mDNS: Discovered new peer {peer_name} at {peer_host}:{peer_port} (role={peer_role})")
            else:
                # Update existing peer address if changed
                existing = self._peers[peer_name]
                if existing.host != peer_host or existing.p2p_port != peer_port:
                    existing.host = peer_host
                    existing.p2p_port = peer_port
                    existing.port = peer_port
                    log.info(f"mDNS: Updated peer {peer_name} address to {peer_host}:{peer_port}")

        elif state_change == ServiceStateChange.Removed:
            peer_name = name.split('.')[0]
            if peer_name != self.node_name and peer_name in self._peers:
                log.info(f"mDNS: Peer {peer_name} removed from network")
                # Don't remove — let health check handle it

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
                cur.execute("LISTEN mesh_node_joined")
                cur.close()
                log.info("Listening for mesh_node_update and mesh_node_joined PG NOTIFY (dedicated thread)")
                
                while self._running:
                    try:
                        # Use select() to poll for PG notifications (1s timeout)
                        if _select.select([listener_conn], [], [], 1.0)[0]:
                            listener_conn.poll()
                        while listener_conn.notifies:
                            msg = listener_conn.notifies.pop(0)
                            if msg.channel == "mesh_node_joined":
                                # New node joined the mesh — trigger immediate discovery
                                node_name = msg.payload.strip() if msg.payload else ""
                                log.info(f"PG NOTIFY: mesh_node_joined — node '{node_name}' joined, triggering discovery")
                                try:
                                    loop = _asyncio.get_event_loop()
                                    if loop.is_running():
                                        _asyncio.run_coroutine_threadsafe(
                                            self.discover_and_connect(), loop
                                        )
                                    else:
                                        loop.run_until_complete(self.discover_and_connect())
                                except RuntimeError:
                                    try:
                                        loop = _asyncio.get_event_loop()
                                        _asyncio.run_coroutine_threadsafe(
                                            self.discover_and_connect(), loop
                                        )
                                    except Exception:
                                        pass
                            elif msg.channel == "mesh_node_update":
                                import json as _json
                                try:
                                    data = _json.loads(msg.payload)
                                    node_name = data.get("node", data.get("name", ""))
                                    action = data.get("action", "register")
                                    log.info(f"PG NOTIFY: peer {node_name} action={action}")
                                    if action in ("register", "update", "heartbeat"):
                                        try:
                                            loop = _asyncio.get_event_loop()
                                            if loop.is_running():
                                                # Thread-safe coroutine scheduling from sync thread
                                                _asyncio.run_coroutine_threadsafe(
                                                    self.discover_and_connect(), loop
                                                )
                                            else:
                                                loop.run_until_complete(self.discover_and_connect())
                                        except RuntimeError:
                                            # No running loop — schedule thread-safe
                                            try:
                                                loop = _asyncio.get_event_loop()
                                                _asyncio.run_coroutine_threadsafe(
                                                    self.discover_and_connect(), loop
                                                )
                                            except Exception:
                                                pass  # Will be discovered on next periodic cycle
                                    elif action in ("deregister", "offline"):
                                        # P2 FIX: Thread-safe event instead of direct dict mutation
                                        # Old code: del self._peers[node_name] (race condition!)
                                        try:
                                            self._pg_event_queue.put_nowait(("deregister", node_name))
                                        except Exception:
                                            log.debug(f"PG event queue full, skipping deregister for {node_name}")
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

    async def _pg_event_processor(self):
        """Process PG NOTIFY events from the thread-safe queue.
        
        This replaces direct _peers mutations from the PG thread,
        eliminating race conditions with the async event loop.
        """
        while self._running:
            try:
                event_type, node_name = await asyncio.wait_for(
                    self._pg_event_queue.get(), timeout=5.0
                )
                if event_type == "deregister":
                    if node_name in self._peers:
                        del self._peers[node_name]
                        log.info(f"Removed offline peer (PG event): {node_name}")
                # Future: add more event types (register, update, etc.)
            except asyncio.TimeoutError:
                continue  # No events, loop again
            except Exception as e:
                log.error(f"PG event processor error: {e}")
                await asyncio.sleep(1)

    async def _discover_loop(self):
        """Periodic peer discovery loop."""
        while self._running:
            try:
                await self.discover_and_connect()
                # Prune stale peers not seen in 30 minutes
                cutoff = time.time() - 1800
                stale = [name for name, p in self._peers.items() if p.last_seen > 0 and p.last_seen < cutoff]
                for name in stale:
                    log.info(f"Pruning stale peer: {name} (last seen {int(time.time() - self._peers[name].last_seen)}s ago)")
                    del self._peers[name]
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