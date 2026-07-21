"""A2A Mesh Node — Main mesh node that ties everything together.

MeshNode is the central orchestrator that:
- Manages all transports (PG, P2P, HTTP)
- Routes messages via the MeshRouter
- Handles discovery via mDNS
- Runs coordinator election & failover
- Provides CLI interface
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Optional, Dict, List, Callable, Tuple

from .core.message import A2AMessage, SendResult, ProcessResult, MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK, MAX_MESSAGE_SIZE
from .core.config import MeshConfig
from .core.router import MeshRouter
from .core.encryption import MeshEncryption
from .core.topology import NodeRole, MeshAddress, AddressManager
from .core.tree_router import TreeRouter
from .core.election import CoordinatorElection, ElectionConfig, CoordinatorState
from .core.ack import AckManager, AckType, AckStatus
from .core.offline_queue import OfflineQueue
from .core.auth import NodeAuthenticator, AuthConfig, JoinRequest, AuthMode
from .core.async_db import AsyncDBPool, validate_message_payload, MessageValidationError
from .core.auto_steer import AutoSteerProcessor
from .core.local_store import LocalStore
from .core.file_transfer import P2PFileTransfer, FILE_OFFER, FILE_ACCEPT, FILE_REJECT, FILE_CHUNK, FILE_COMPLETE, FILE_ACK
from .core.peer_discovery import PeerDiscovery
from .core.memory_sync import MemorySync
from .core.dashboard import DashboardHandler
from .transports.pg_transport import PGTransport
from .transports.p2p_transport import P2PTransport
from .transports.http_transport import HTTPTransport
from .transports.ble_transport import BLETransport
from .discovery.mdns import MeshDiscovery
from .discovery.udp_broadcast import UDPBroadcastDiscovery
from .core.plugin_loader import PluginLoader
from .core.topology_tuner import TopologyTuner
from .core.delegation import DelegationManager, _safe_ascii
from .core.exceptions import ConfigurationError

log = logging.getLogger("a2a_mesh.node")


class MeshNode:
    """Main mesh node — orchestrates all transports and routing.

    Usage:
        config = MeshConfig.from_yaml("mesh_config.yaml")
        node = MeshNode(config)
        await node.start()

        # Send a message
        msg = A2AMessage.create(
            sender="nova",
            recipient="morzsa",
            msg_type="directive",
            payload={"action": "ping"},
            priority=5
        )
        result = await node.send(msg)

        # Add a handler for incoming messages
        async def handle_message(message):
            print(f"Got: {message}")

        node.add_handler(handle_message)

        # Run until stopped
        await node.run_forever()
    """

    def __init__(self, config: Optional[MeshConfig] = None):
        self.config = config or MeshConfig()
        self.node_name = self.config.node_name
        # Resolve version from git tag (auto-updates on deploy)
        self._resolved_version = self.config._resolve_version()

        # Initialize encryption
        self.encryption: Optional[MeshEncryption] = None
        if self.config.security.signing_key:
            try:
                self.encryption = MeshEncryption(self.config.security.signing_key)
            except Exception as e:
                log.warning(f"Encryption init failed: {e}")
        if not self.config.security.signing_key:
            try:
                self.encryption = MeshEncryption()
                self.config.security.signing_key = self.encryption.signing_key_hex
                log.info(f"Generated new signing key: {self.encryption.verify_key_hex[:16]}...")
            except ImportError:
                log.warning("pynacl not installed, message signing disabled")

        # Initialize router
        self.local_store = LocalStore(node_name=self.node_name)
        self.router = MeshRouter(self.node_name, self.config, local_store=self.local_store)

        # Initialize topology (Zigbee-inspired)
        topo = self.config.topology
        self.role = NodeRole(topo.node_role)
        self.mesh_address: Optional[MeshAddress] = None
        self.address_manager: Optional[AddressManager] = None
        self.tree_router: Optional[TreeRouter] = None

        if self.role == NodeRole.COORDINATOR:
            # Coordinator assigns addresses and manages the tree
            self.address_manager = AddressManager(
                max_children=topo.max_children,
                max_routers=topo.max_routers,
                max_depth=topo.max_depth,
            )
            self.mesh_address = self.address_manager.assign_address(
                self.node_name, NodeRole.COORDINATOR
            )
            # Use deterministic short_addr for coordinator too (avoid short_addr=0 conflict)
            import hashlib
            name_hash = int(hashlib.md5(self.node_name.encode()).hexdigest(), 16)
            deterministic_addr = (name_hash % 0xFFFE) + 1  # 1-65535, avoid 0
            self.mesh_address.short = deterministic_addr
            log.info(f"Coordinator mode: address={self.mesh_address} (deterministic short={deterministic_addr})")
            self.tree_router = TreeRouter(self.mesh_address, self.address_manager)
        elif self.role == NodeRole.ROUTER:
            # Router joins network, gets address from coordinator
            # If coordinator not reachable, assign self as first router
            # Use deterministic short_addr based on node name to avoid conflicts
            self.address_manager = AddressManager(
                max_children=topo.max_children,
                max_routers=topo.max_routers,
                max_depth=topo.max_depth,
            )
            # Generate deterministic short_addr from node name hash
            import hashlib
            name_hash = int(hashlib.md5(self.node_name.encode()).hexdigest(), 16)
            deterministic_addr = (name_hash % 0xFFFE) + 1  # 1-65535, avoid 0 (coordinator)
            self.mesh_address = self.address_manager.assign_address(
                self.node_name, NodeRole.ROUTER
            )
            # Override the sequential short_addr with deterministic one
            self.mesh_address.short = deterministic_addr
            self.tree_router = TreeRouter(self.mesh_address, self.address_manager)
            log.info(f"Router mode: address={self.mesh_address}")
        else:
            # End device — lightweight, connects via parent
            log.info(f"End device mode: will join network via parent router")

        # Initialize coordinator election
        self.election = CoordinatorElection(
            self_name=self.node_name,
            self_addr=self.mesh_address.short if self.mesh_address else 0xFFFF,
            self_role=topo.node_role,
            config=ElectionConfig(
                heartbeat_interval=self.config.heartbeat.interval,
                suspect_threshold=self.config.heartbeat.warning_threshold,
                down_threshold=self.config.heartbeat.critical_threshold,
            ),
        )

        # Initialize ACK manager
        self.ack_manager = AckManager(node_name=self.node_name)

        # Initialize offline queue
        self.offline_queue = OfflineQueue(
            pg_config=self.config.pg,
            node_name=self.node_name,
        )

        # Initialize auto-steer processor
        self.auto_steer = AutoSteerProcessor(
            node_name=self.node_name,
            config=self.config,
        )

        # Initialize delegation manager (task delegation between nodes)
        self.delegation = DelegationManager(
            pg_pool=None,  # Will be set after PG connection is established
            node_name=self.node_name,
        )

        # Initialize P2P file transfer
        self.file_transfer = P2PFileTransfer(
            node_name=self.node_name,
            local_store=self.local_store,
        )

        # Initialize peer discovery (P2P transport set after start)
        self.peer_discovery = PeerDiscovery(
            node_name=self.node_name,
            config=self.config,
            local_store=self.local_store,
            pg_conn=None,  # Set later after PG connection established
            registry=None,  # Set after dashboard init
        )

        # Initialize mesh memory sync
        self.memory_sync = MemorySync(self)

        # Initialize web dashboard
        self.dashboard = DashboardHandler(self)

        # Link registry to peer discovery (after dashboard init)
        self.peer_discovery.registry = self.dashboard.registry
        
        # Set callback for peer discovery → triggers skills announcement via PG broadcast
        self.peer_discovery._on_peer_discovered = self._on_peer_discovered

        # Initialize plugin loader
        self.plugin_loader = PluginLoader(self)

        # Initialize topology tuner (health-score-based auto-tuning)
        self.topology_tuner = TopologyTuner(self, config=self.config)
        # Debounce peer offline broadcasts — prevent broadcast storms during P2P flapping
        self._peer_offline_debounce: dict[str, float] = {}  # peer_name -> last_broadcast_time
        # Grace period tasks — delayed offline broadcasts cancelled on reconnect
        self._peer_offline_grace_tasks: dict[str, asyncio.Task] = {}  # peer_name -> pending broadcast task
        self._peer_offline_grace_seconds: int = 30  # wait before declaring peer offline

        # Rate limit skills announcements — min 60s between announcements to prevent flooding
        self._last_skills_announcement: float = 0

        # Initialize node authenticator
        auth_config = AuthConfig(
            mode=getattr(self.config, 'auth_mode', 'open'),
            trust_center=self.node_name if self.role == NodeRole.COORDINATOR else "",
            whitelist=set(getattr(self.config, 'auth_whitelist', [])),
        )
        self.authenticator = NodeAuthenticator(auth_config)

        # Health endpoint
        self._health_server: Optional[asyncio.AbstractServer] = None
        self._health_port = getattr(self.config, 'health_port', 8650)
        # Safety: ensure health_port differs from p2p_port to avoid bind conflict
        if self._health_port == self.config.p2p.listen_port:
            self._health_port = self.config.p2p.listen_port + 5
            log.warning(f"health_port == p2p_port ({self.config.p2p.listen_port}), auto-corrected to {self._health_port}")

        # Initialize transports
        self._pg_transport = PGTransport(self.config)
        self._p2p_transport = P2PTransport(self.config)
        self._http_transport = HTTPTransport(self.config)
        self._ble_transport = BLETransport(self.config)

        # Register transports with router
        self.router.register_transport("pg_notify", self._pg_transport)
        self.router.register_transport("p2p", self._p2p_transport)
        self.router.register_transport("http", self._http_transport)
        self.router.register_transport("ble", self._ble_transport)

        # Initialize discovery
        self._discovery = MeshDiscovery(
            node_name=self.node_name,
            port=self.config.p2p.listen_port,
        )

        # Message handlers
        self._handlers: List[Callable] = []

        # State
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._start_time = 0
        self._pg_pool: Optional[AsyncDBPool] = None  # asyncpg connection pool for all DB ops

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging to file and console."""
        log_dir = os.path.dirname(self.config.log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        handlers = []
        if self.config.log_file:
            handlers.append(logging.FileHandler(self.config.log_file))
        handlers.append(logging.StreamHandler())

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            handlers=handlers,
        )

    def add_handler(self, handler: Callable):
        """Add a message handler."""
        self._handlers.append(handler)

    async def _dispatch_to_handlers(self, message: A2AMessage):
        """Dispatch incoming message to all registered handlers.

        Special handling for file_transfer, memory_sync messages, ACK.
        Note: Dashboard notification is now in _receive_loop for ALL processed messages.
        """
        # Handle ACK messages — process via ack_manager
        if message.type == MSG_TYPE_ACK:
            self.ack_manager.process_ack(message)
            return

        log.debug(f"_dispatch_to_handlers: msg id={message.id[:8]} type={message.type} sender={message.sender}")

        # Handle file transfer messages
        if message.type == "file_transfer":
            # Parse payload
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}

            transfer_type = payload.get("transfer_type", "")

            # Let P2PFileTransfer handle the message
            response = self.file_transfer.handle_incoming(message)
            if response and isinstance(response, A2AMessage):
                # Send response back via P2P (or best transport)
                asyncio.create_task(self.router.send(response))

            # If we received FILE_ACCEPT, we are the sender — start sending chunks
            if transfer_type == FILE_ACCEPT:
                file_id = payload.get("file_id", "")
                if file_id:
                    log.info(f"FILE_ACCEPT received for {file_id}, starting chunk transfer")
                    asyncio.create_task(self._send_file_chunks(file_id, message.sender))

            return

        # Handle memory sync messages
        if message.type == "memory_sync":
            payload = message.payload if isinstance(message.payload, dict) else {}
            self.memory_sync.handle_incoming_memory(payload)
            return

        # Handle skills announcement — P2P auto-discovery of agent skills
        if message.type == "skills_announcement":
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}
            peer_skills = payload.get("skills", [])
            peer_capabilities = payload.get("capabilities", [])
            peer_name = message.sender
            log.info(f"Skills announcement from {peer_name}: skills={[s.get('id','?') if isinstance(s, dict) else s for s in peer_skills]}, caps={peer_capabilities}")
            # Update the peer's AgentCard in registry with their skills
            merged_skills = list(peer_skills)
            merged_caps = [c for c in peer_capabilities if isinstance(c, (str, int, float, tuple))]
            if not merged_caps:
                merged_caps = ["a2a_messaging"]
            if hasattr(self, 'dashboard') and hasattr(self.dashboard, 'registry'):
                card = self.dashboard.registry.get(peer_name)
                if card:
                    # Merge skills: keep existing + add new ones (by id)
                    existing_ids = set()
                    for s in (card.skills or []):
                        sid = s.get('id') if isinstance(s, dict) else s
                        if isinstance(sid, (str, int, float, tuple)):
                            existing_ids.add(sid)
                    merged_skills = list(card.skills or [])
                    for skill in peer_skills:
                        skill_id = skill.get('id') if isinstance(skill, dict) else skill
                        if skill_id not in existing_ids and isinstance(skill_id, (str, int, float, tuple)):
                            merged_skills.append(skill)
                            existing_ids.add(skill_id)
                    card.skills = merged_skills
                    # Also merge capabilities (union) — filter non-hashable items
                    if peer_capabilities:
                        existing_caps = set(c for c in (card.capabilities or []) if isinstance(c, (str, int, float, tuple)))
                        new_caps = set(c for c in peer_capabilities if isinstance(c, (str, int, float, tuple)))
                        merged_caps = list(existing_caps | new_caps)
                        if not merged_caps:
                            merged_caps = ["a2a_messaging"]
                        card.capabilities = merged_caps
                    log.info(f"Updated {peer_name} in registry: {len(merged_skills)} skills, {len(card.capabilities)} caps")
                else:
                    # Peer not in registry yet — create a new AgentCard
                    from .core.registry import AgentCard
                    new_card = AgentCard(
                        name=peer_name,
                        endpoint=f"http://{getattr(self, '_last_peer_host', '')}:8650",
                        skills=merged_skills,
                        capabilities=merged_caps,
                    )
                    self.dashboard.registry.register(new_card)
                    log.info(f"Created registry card for {peer_name}: {len(merged_skills)} skills, {len(merged_caps)} caps")
            # Sync skills & capabilities to DB
            # Convert dict skills to string IDs for SQL_ASCII compatibility
            try:
                if self._pg_pool and self._pg_pool.is_connected():
                    # Normalize skills: convert dicts to their id string, keep strings as-is
                    db_skills = []
                    for s in (merged_skills or []):
                        if isinstance(s, dict):
                            db_skills.append(s.get('id', str(s)))
                        elif isinstance(s, str):
                            db_skills.append(s)
                        elif isinstance(s, (int, float)):
                            db_skills.append(str(s))
                    # Normalize caps: only keep hashable types
                    db_caps = [c for c in (merged_caps or []) if isinstance(c, (str, int, float, tuple))]
                    if not db_caps:
                        db_caps = ["a2a_messaging"]
                    await self._pg_pool.execute("""
                        UPDATE mesh.mesh_nodes SET skills = $1, capabilities = $2 WHERE node_name = $3
                    """,
                        json.dumps(db_skills, ensure_ascii=True),
                        json.dumps(db_caps, ensure_ascii=True),
                        peer_name,
                    )
                    log.info(f"Synced {peer_name} skills/caps to DB")
            except Exception as e:
                log.warning(f"Failed to sync {peer_name} skills to DB: {e}")
            return

        # Dispatch to plugins first (they can intercept/transform messages)
        if hasattr(self, 'plugin_loader') and self.plugin_loader.plugins:
            plugin_response = await self.plugin_loader.dispatch_message_received(message)
            if plugin_response is not None:
                # Plugin handled the message, optionally send response
                if isinstance(plugin_response, A2AMessage):
                    asyncio.create_task(self.router.send(plugin_response))
                return  # Plugin consumed the message

        for handler in self._handlers:
            try:
                result = handler(message)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                log.error(f"Handler error: {e}")

    async def start(self) -> bool:
        """Start all transports and discovery."""
        # Validate node_name
        if not self.node_name or self.node_name.strip() == '':
            log.error("node_name is empty — refusing to start with invalid config")
            raise ConfigurationError("node_name cannot be empty")
        if self.node_name != self.config.node_name:
            log.warning(f"node_name mismatch: self.node_name={self.node_name} vs config.node_name={self.config.node_name} — using {self.node_name}")

        # Sanity check: node_name should not be a common default or another node's name
        # This catches copy-paste config errors
        if hasattr(self.config, 'discovery') and hasattr(self.config.discovery, 'static_nodes'):
            for node in self.config.discovery.static_nodes:
                static_name = node.get('name', '')
                if static_name == self.node_name and static_name != self.config.node_name:
                    log.warning(f"node_name '{self.node_name}' matches a static node entry — this may be correct if this IS that node")

        log.info(f"Starting mesh node '{self.node_name}' (role={self.role.value})")
        self._start_time = time.time()

        # Initialize direct PG connection for writes
        if not await self._init_pg_write_conn():
            log.warning("PG write connection failed — will retry")

        # Register self in mesh.mesh_nodes
        await self._register_node()

        # Start transports in priority order
        # P2P is now primary — PG is optional fallback
        results = {}

        # 1. P2P TCP (primary — always try first)
        results["p2p"] = await self._p2p_transport.start()
        if results["p2p"]:
            log.info("✅ P2P TCP transport started (primary)")
            # Wire up P2P ACK callback — updates PG message status when ACK received
            self._p2p_transport.set_ack_callback(self._on_p2p_ack)
            # Wire up P2P peer connected callback — registers peer with agent registry on connect/reconnect
            self._p2p_transport.set_peer_connected_callback(self._on_p2p_peer_connected)
            # Wire up P2P peer disconnect callback — broadcasts offline notification to mesh
            self._p2p_transport.set_peer_disconnect_callback(self._on_p2p_peer_disconnected)
            log.info("✅ P2P callbacks registered (ACK + peer_connected + peer_disconnected)")
        else:
            log.warning("❌ P2P TCP transport failed")
            await self.debug_log("ERROR", "transport", "P2P TCP transport failed to start")

        # 2. PG NOTIFY (optional fallback — gracefully degrades if unavailable)
        results["pg_notify"] = await self._pg_transport.start()
        if results["pg_notify"]:
            log.info("✅ PG NOTIFY transport started (fallback)")
        else:
            log.warning("⚠️ PG NOTIFY transport unavailable — running in P2P-only mode")
            await self.debug_log("WARNING", "transport", "PG NOTIFY transport unavailable — running in P2P-only mode")

        # 3. HTTP/MCP (tertiary)
        results["http"] = await self._http_transport.start()
        if results["http"]:
            log.info("✅ HTTP/MCP transport started")
        else:
            log.warning("❌ HTTP/MCP transport failed")
            await self.debug_log("ERROR", "transport", "HTTP/MCP transport failed to start")

        # Start BLE transport
        results["ble"] = await self._ble_transport.start()
        if results["ble"]:
            log.info("✅ BLE transport started")
        else:
            log.warning("❌ BLE transport failed (non-critical)")
            await self.debug_log("WARNING", "transport", "BLE transport failed (non-critical, bleak not installed)")

        # 4. mDNS discovery (linked to peer_discovery for auto-connect)
        if self.config.discovery.mdns_enabled:
            host_ip = self._get_local_ip()
            # Link mDNS discovery to peer_discovery so discovered nodes auto-connect
            self._discovery.on_discover(self._on_mdns_discover)
            disc_ok = await self._discovery.start(host_ip=host_ip)
            if disc_ok:
                log.info("✅ mDNS discovery started")
            else:
                log.warning("❌ mDNS discovery failed")
                await self.debug_log("WARNING", "transport", "mDNS discovery failed (zeroconf not installed or multicast unavailable)")

        # 5. UDP broadcast discovery (works on local network + Tailscale)
        tailscale_if = self.config.discovery.tailscale_interface
        udp_interfaces = [tailscale_if] if tailscale_if else None
        self._udp_discovery = UDPBroadcastDiscovery(
            node_name=self.node_name,
            p2p_port=self.config.p2p.listen_port,
            health_port=self.config.health_port or 8650,
            discovery_port=self.config.discovery.udp_broadcast_port,
            interfaces=udp_interfaces,
        )
        self._udp_discovery.on_discover(self._on_mdns_discover)  # Same handler for both
        udp_ok = await self._udp_discovery.start()
        if udp_ok:
            log.info("✅ UDP broadcast discovery started")
        else:
            log.warning("❌ UDP broadcast discovery failed")

        # Start background loops
        self._running = True
        self._tasks.append(asyncio.create_task(self._receive_loop()))
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))
        self._tasks.append(asyncio.create_task(self._election_monitor_loop()))
        self._tasks.append(asyncio.create_task(self._health_monitor_loop()))
        self._tasks.append(asyncio.create_task(self._stats_update_loop()))

        # Auto-update: check for new versions periodically
        auto_update_cfg = getattr(self.config, 'auto_update', None)
        if auto_update_cfg and getattr(auto_update_cfg, 'enabled', False):
            check_interval = getattr(auto_update_cfg, 'check_interval', 300)
            self._tasks.append(asyncio.create_task(self._auto_update_loop(check_interval)))

        # Auto-update state (shared with health endpoint)
        self._updater_state = {"state": "idle", "last_check": None, "last_update": None, "current_version": self._resolved_version}

        # Start priority queue processor
        self.router.start_priority_queue()

        # Start peer discovery (link P2P transport and PG pool for auto-connect)
        self.peer_discovery.p2p_transport = self._p2p_transport
        self.peer_discovery._pg_pool = self._pg_pool
        # P2: Wire up peer address resolver — P2P transport can now dynamically
        # connect to peers it hasn't connected to yet, using peer_discovery data
        self._p2p_transport._peer_address_resolver = self.peer_discovery.resolve_peer_address
        self.memory_sync._pg_pool = self._pg_pool
        # Wire up delegation manager with PG pool and start polling
        self.delegation.pg_pool = self._pg_pool
        # Register built-in task handlers
        self.delegation.register_handler("monitoring", self._handle_monitoring_task)
        self.delegation.register_handler("generic", self._handle_generic_task)
        self.delegation.register_handler("research", self._handle_generic_task)
        self.delegation.register_handler("code", self._handle_generic_task)
        self.delegation.register_handler("analysis", self._handle_generic_task)
        await self.delegation.start()
        await self.peer_discovery.start()

        # Start ACK manager
        await self.ack_manager.start()

        # Start health endpoint
        self._tasks.append(asyncio.create_task(self._run_health_server()))

        # Load and start plugins
        plugin_configs = getattr(self.config, 'plugins', {}) or {}
        try:
            loaded = await self.plugin_loader.load_all(config=plugin_configs)
            if loaded:
                log.info(f"✅ {len(loaded)} plugin(s) loaded: {list(loaded.keys())}")
            else:
                log.info("No plugins loaded")
        except Exception as e:
            log.warning(f"Plugin loading failed (non-fatal): {e}")

        # Auto-register self in the agent registry
        self._auto_register_self()

        # Start topology tuner (health-score-based auto-tuning)
        try:
            await self.topology_tuner.start()
        except Exception as e:
            log.warning(f"Topology tuner start failed (non-fatal): {e}")

        # At least one transport must be working
        any_ok = any(results.values())
        if any_ok:
            log.info(f"Mesh node '{self.node_name}' started ({sum(results.values())}/3 transports)")
        else:
            log.error("All transports failed!")

        return any_ok

    # ── Delegation task handlers ──

    async def _handle_monitoring_task(self, task: dict, context: dict) -> str:
        """Handle monitoring-type delegated tasks. Returns dict with result, files, context_updates."""
        import platform
        from datetime import datetime, timezone
        
        uptime = datetime.now(timezone.utc).isoformat()
        
        try:
            import psutil
            cpu_pct = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else (0, 0, 0)
            net_io = psutil.net_io_counters()
            
            result_text = (
                f"[{self.node_name}] Health check at {uptime}\n"
                f"  CPU: {cpu_pct}%\n"
                f"  Memory: {mem.percent}% ({mem.available // 1024 // 1024}MB free)\n"
                f"  Disk: {disk.percent}% ({disk.free // 1024 // 1024 // 1024}GB free)\n"
                f"  Load: {load_avg[0]:.1f}, {load_avg[1]:.1f}, {load_avg[2]:.1f}\n"
                f"  Network: ↑{net_io.bytes_sent // 1024 // 1024}MB ↓{net_io.bytes_recv // 1024 // 1024}MB\n"
                f"  Platform: {platform.system()} {platform.release()}\n"
                f"  Python: {platform.python_version()}"
            )
            
            # Build detailed JSON report as file
            import json
            report = {
                "agent": self.node_name,
                "timestamp": uptime,
                "cpu_pct": cpu_pct,
                "memory_pct": mem.percent,
                "memory_available_mb": mem.available // 1024 // 1024,
                "disk_pct": disk.percent,
                "disk_free_gb": disk.free // 1024 // 1024 // 1024,
                "load_avg": list(load_avg),
                "net_sent_mb": net_io.bytes_sent // 1024 // 1024,
                "net_recv_mb": net_io.bytes_recv // 1024 // 1024,
                "platform": platform.system(),
                "platform_release": platform.release(),
                "python_version": platform.python_version(),
            }
            file_content = json.dumps(report, indent=2)
            
            return {
                "result": result_text,
                "files": [{
                    "filename": f"health_{self.node_name}_{uptime[:10]}.json",
                    "content_type": "application/json",
                    "content": file_content,
                    "size": len(file_content),
                }],
                "context_updates": {
                    "cpu_pct": str(cpu_pct),
                    "memory_pct": str(mem.percent),
                    "disk_pct": str(disk.percent),
                    "last_health_check": uptime,
                },
            }
        except ImportError:
            result_text = (
                f"[{self.node_name}] Health check at {uptime}\n"
                f"  Platform: {platform.system()} {platform.release()}\n"
                f"  Python: {platform.python_version()}\n"
                f"  (psutil not available — basic report only)"
            )
            return {
                "result": result_text,
                "files": [],
                "context_updates": {
                    "last_health_check": uptime,
                },
            }

    async def _handle_generic_task(self, task: dict, context: dict) -> dict:
        """Handle generic delegated tasks. Parses description for instructions
        and dispatches to specialized sub-handlers based on keywords."""
        import platform
        import subprocess
        import json as _json
        from datetime import datetime, timezone

        subject = task.get("subject", "unknown")
        desc_raw = task.get("description", "")
        now = datetime.now(timezone.utc)
        node = self.node_name

        # Parse description — may be plain text or JSON
        desc_text = ""
        desc_ctx = {}
        try:
            d = _json.loads(desc_raw)
            desc_text = d.get("description", "")
            desc_ctx = d.get("context", {})
        except (ValueError, TypeError, AttributeError):
            desc_text = desc_raw if desc_raw else subject

        # ── Task dispatcher based on keywords ────────────────────────
        # Normalize: remove diacritics for matching (írj -> irj, fájl -> fajl)
        import unicodedata
        lower_raw = (subject + " " + desc_text).lower()
        # Also create an ASCII-normalized version for matching
        nfkd = unicodedata.normalize('NFKD', lower_raw)
        lower = ''.join(c for c in nfkd if not unicodedata.combining(c))

        # --- Code generation (check BEFORE file/network/diagnostic) ---
        if any(kw in lower for kw in ("kod", "code", "script", "python", "bash", "javascript", "generalj", "generate", "irj", "write", "szamold", "szamol", "oldd", "hatarozd", "keszits", "csinalj", "compute", "calcul")):
            return await self._task_code_generation(node, now, subject, desc_text)

        # --- Generate HTML status page ---
        if any(kw in lower for kw in ("html", "weboldal", "web oldal", "statuszoldal", "status page")):
            return await self._task_html_status(node, now)

        # --- System analysis / diagnostics ---
        if any(kw in lower for kw in ("diagnosztika", "diagnostico", "diagnostic", "analysis", "elemzes", "rendszer", "system info", "bench", "benchmark")):
            return await self._task_system_analysis(node, now)

        # --- File operations (only explicit file/list commands) ---
        if any(kw in lower for kw in ("fajl", "file ops", "konyvtar", "directory listing", "ls -", "cat /", "head /", "read file", "show files", "list dir", "list files")):
            return await self._task_file_ops(node, now, desc_text)

        # --- Network check ---
        if any(kw in lower for kw in ("ping", "halozat", "network", "dns", "ip", "port", "curl", "wget", "connect")):
            return await self._task_network_check(node, now, desc_text)

        # --- Fallback heuristic: if no keyword matched, try to guess from subject ---
        # Verbs suggesting action → code generation, nouns suggesting data → diagnostics
        verb_hints = ("create", "make", "build", "write", "generate", "comput", "calcul", "process", "irj", "generalj", "keszits", "csinalj", "szamol", "szamold", "oldd", "hatarozd")
        data_hints = ("check", "test", "status", "info", "show", "list", "get", "read", "ell", "vizsgal", "mutat", "listaz", "keres", "monitor", "diag")
        if any(h in lower for h in verb_hints):
            return await self._task_code_generation(node, now, subject, desc_text)
        if any(h in lower for h in data_hints):
            return await self._task_system_analysis(node, now)

        # --- Default: acknowledge ---
        return {
            "result": f"[{node}] Acknowledged task '{subject}' at {now.isoformat()}",
            "files": [],
            "context_updates": {"generic_ack": "true"},
        }

    # ── Sub-handlers ──────────────────────────────────────────────────

    async def _task_html_status(self, node: str, now) -> dict:
        """Generate an elegant HTML status page."""
        import platform
        cpu_pct = mem_pct = disk_pct = load_avg = "N/A"
        try:
            import psutil
            cpu_pct = f"{psutil.cpu_percent(interval=0.5):.1f}%"
            mem_pct = f"{psutil.virtual_memory().percent:.1f}%"
            disk_pct = f"{psutil.disk_usage('/').percent:.1f}%"
            load_avg = ", ".join(f"{x:.1f}" for x in psutil.getloadavg())
        except Exception:
            pass

        html = f"""<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{node} Status</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
               color: #e0e0e0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
        .card {{ background: rgba(255,255,255,0.05); backdrop-filter: blur(10px);
                 border: 1px solid rgba(255,255,255,0.1); border-radius: 16px;
                 padding: 2rem; max-width: 500px; width: 90%; box-shadow: 0 8px 32px rgba(0,0,0,0.3); }}
        h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; color: #7f5af0; }}
        .agent {{ font-size: 0.9rem; color: #a0a0a0; margin-bottom: 1.5rem; }}
        .stat {{ display: flex; justify-content: space-between; padding: 0.6rem 0;
                 border-bottom: 1px solid rgba(255,255,255,0.05); }}
        .stat:last-child {{ border-bottom: none; }}
        .label {{ color: #a0a0a0; }}
        .value {{ color: #7f5af0; font-weight: 600; }}
        .time {{ margin-top: 1.5rem; font-size: 0.8rem; color: #666; text-align: center; }}
        .badge {{ display: inline-block; background: #7f5af0; color: white; padding: 0.2rem 0.6rem;
                  border-radius: 12px; font-size: 0.75rem; margin-left: 0.5rem; }}
    </style>
</head>
<body>
    <div class="card">
        <h1>{node} <span class="badge">A2A Mesh</span></h1>
        <div class="agent">{platform.node()} &middot; {platform.system()} {platform.release()}</div>
        <div class="stat"><span class="label">CPU</span><span class="value">{cpu_pct}</span></div>
        <div class="stat"><span class="label">Memória</span><span class="value">{mem_pct}</span></div>
        <div class="stat"><span class="label">Lemez</span><span class="value">{disk_pct}</span></div>
        <div class="stat"><span class="label">Load</span><span class="value">{load_avg}</span></div>
        <div class="stat"><span class="label">Idő</span><span class="value">{now.strftime('%Y-%m-%d %H:%M:%S UTC')}</span></div>
        <div class="time">A2A Mesh v0.14.3 &middot; Task delegation</div>
    </div>
</body>
</html>"""

        save_path = f"/tmp/agent-status-{node}.html"
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            save_path = ""

        return {
            "result": f"[{node}] Generated HTML status page at {now.isoformat()}",
            "files": [{"filename": f"status_{node}_{now.strftime('%Y%m%d_%H%M%S')}.html",
                        "content_type": "text/html", "content": html,
                        "size": len(html.encode("utf-8"))}],
            "context_updates": {"task_type": "html_status", "cpu": str(cpu_pct),
                                "memory": str(mem_pct), "disk": str(disk_pct), "save_path": save_path},
        }

    async def _task_system_analysis(self, node: str, now) -> dict:
        """Collect detailed system diagnostics."""
        import platform
        import subprocess

        info = {"node": node, "hostname": platform.node(), "system": platform.system(),
                "release": platform.release(), "python": platform.python_version()}

        try:
            import psutil
            info["cpu_pct"] = f"{psutil.cpu_percent(interval=0.5):.1f}%"
            mem = psutil.virtual_memory()
            info["memory_pct"] = f"{mem.percent:.1f}%"
            info["memory_available_mb"] = int(mem.available / 1024 / 1024)
            info["disk_pct"] = f"{psutil.disk_usage('/').percent:.1f}%"
            info["disk_free_gb"] = round(psutil.disk_usage('/').free / 1024 / 1024 / 1024, 1)
            info["load_avg"] = [round(x, 2) for x in psutil.getloadavg()]
            info["uptime_hours"] = round(psutil.boot_time() / 3600, 1) if hasattr(psutil, 'boot_time') else "N/A"
            # Top 5 processes by CPU
            procs = sorted(psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']),
                           key=lambda p: p.info.get('cpu_percent', 0) or 0, reverse=True)[:5]
            info["top_processes"] = [{"name": p.info['name'], "cpu": f"{p.info.get('cpu_percent', 0):.1f}%",
                                      "mem": f"{p.info.get('memory_percent', 0):.1f}%"} for p in procs]
        except Exception as e:
            info["psutil_error"] = str(e)

        # Network interfaces
        try:
            result = subprocess.run(["ip", "addr", "show"], capture_output=True, text=True, timeout=5)
            net_lines = [l.strip() for l in result.stdout.split("\n") if "inet " in l][:10]
            info["network_ips"] = net_lines
        except Exception:
            pass

        # Docker containers
        try:
            result = subprocess.run(["docker", "ps", "--format", "{{.Names}} {{.Status}}"],
                                    capture_output=True, text=True, timeout=5)
            containers = result.stdout.strip().split("\n")[:10] if result.stdout.strip() else []
            info["docker_containers"] = containers
        except Exception:
            info["docker_containers"] = []

        report = _json.dumps(info, indent=2, ensure_ascii=False) if 'json' in dir() else str(info)
        import json as _jj
        report = _jj.dumps(info, indent=2, ensure_ascii=False)

        save_path = f"/tmp/analysis-{node}-{now.strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(report)
        except Exception:
            save_path = ""

        return {
            "result": f"[{node}] System analysis completed at {now.isoformat()}",
            "files": [{"filename": f"analysis_{node}_{now.strftime('%Y%m%d_%H%M%S')}.json",
                        "content_type": "application/json", "content": report,
                        "size": len(report.encode("utf-8"))}],
            "context_updates": {"task_type": "system_analysis", **{k: str(v) for k, v in info.items()
                                if isinstance(v, (str, int, float))}},
        }

    async def _task_file_ops(self, node: str, now, desc_text: str) -> dict:
        """List files or read file contents."""
        import subprocess

        # Determine path from description
        path = "/tmp"
        for word in desc_text.split():
            if word.startswith("/") or word.startswith("~/"):
                path = word
                break

        # Expand ~
        path = path.replace("~", "/home" + ("/" + os.environ.get("USER", "user")) if "USER" in os.environ else "")

        result_lines = []
        try:
            if "list" in desc_text.lower() or "ls" in desc_text.lower() or "könyvtár" in desc_text.lower():
                result = subprocess.run(["ls", "-la", path], capture_output=True, text=True, timeout=5)
                result_lines.append(f"=== Listing {path} ===")
                result_lines.append(result.stdout[:3000] if result.stdout else result.stderr[:500])
            else:
                # Read file
                result = subprocess.run(["head", "-100", path], capture_output=True, text=True, timeout=5)
                result_lines.append(f"=== {path} (first 100 lines) ===")
                result_lines.append(result.stdout[:3000] if result.stdout else result.stderr[:500])
        except Exception as e:
            result_lines.append(f"Error: {e}")

        content = "\n".join(result_lines)
        return {
            "result": f"[{node}] File ops: {desc_text[:100]} at {now.isoformat()}",
            "files": [{"filename": f"fileops_{node}_{now.strftime('%Y%m%d_%H%M%S')}.txt",
                        "content_type": "text/plain", "content": content,
                        "size": len(content.encode("utf-8"))}],
            "context_updates": {"task_type": "file_ops", "path": path},
        }

    async def _task_llm_generate(self, node: str, now, subject: str, desc_text: str) -> dict | None:
        """Try to generate code using Ollama LLM. Returns result dict or None."""
        import aiohttp
        import json as _json

        # Ollama API config — same host, default port
        ollama_url = getattr(self, '_ollama_url', None)
        if not ollama_url:
            # Check common Ollama URLs
            for url in ["http://localhost:11434", "http://127.0.0.1:11434"]:
                try:
                    import urllib.request
                    urllib.request.urlopen(f"{url}/api/tags", timeout=2)
                    ollama_url = url
                    self._ollama_url = url
                    break
                except Exception:
                    continue
        if not ollama_url:
            log.debug(f"[{node}] No Ollama available, using template fallback")
            return None

        # Pick model — prefer large code-capable models over small ones
        preferred_models = ["glm-5.2", "glm-5.1", "glm-4.7", "gemma4:31b", "kimi-k2.5", "qwen2.5:7b", "qwen2.5:3b", "qwen2.5:1.5b"]
        model = None
        try:
            import urllib.request
            resp = urllib.request.urlopen(f"{ollama_url}/api/tags", timeout=3)
            models_data = _json.loads(resp.read())
            available = [m["name"] for m in models_data.get("models", [])]
            for pref in preferred_models:
                for avail in available:
                    if pref in avail:
                        model = avail
                        break
                if model:
                    break
            if not model and available:
                model = available[0]  # fallback to first available
        except Exception:
            pass
        if not model:
            log.warning(f"[{node}] No Ollama models found")
            return None

        log.info(f"[{node}] LLM generation using {model} at {ollama_url}")

        # Build prompt
        prompt = f"""You are an A2A Mesh agent named {node}. Generate complete, working code for the following task.

Task: {subject}
Description: {desc_text}

Requirements:
- Generate COMPLETE, WORKING code — no placeholders, no stubs
- Include all necessary imports
- Code must run as-is
- If HTML/CSS/JS: single file, inline everything, no external dependencies
- If Python: include if __name__ == "__main__" block
- Add a comment header: "Generated by {node} via A2A Mesh delegation"

Output ONLY the code, no explanations. Start with the appropriate shebang or DOCTYPE."""

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.3, "num_predict": 8192}
                }
                async with session.post(f"{ollama_url}/api/generate", json=payload) as resp:
                    if resp.status != 200:
                        log.warning(f"[{node}] LLM generate failed: HTTP {resp.status}")
                        return None
                    result = await resp.json()
                    generated = result.get("response", "")
                    if not generated or len(generated) < 20:
                        log.warning(f"[{node}] LLM generated empty/short response")
                        return None

            # Detect language/extension from generated code
            ext = "txt"
            if generated.strip().startswith("<!DOCTYPE") or generated.strip().startswith("<html"):
                ext = "html"
            elif generated.strip().startswith("#!/bin/bash") or generated.strip().startswith("#!/bin/sh"):
                ext = "bash"
            elif generated.strip().startswith("#!/usr/bin/env python") or "import " in generated[:200]:
                ext = "py"
            elif "function " in generated[:200] or "const " in generated[:200] or "=>" in generated[:200]:
                ext = "js"

            filename = f"generated_{ext}_{node}_{now.strftime('%Y%m%d_%H%M%S')}.{ext}"
            log.info(f"[{node}] LLM generated {len(generated)} chars, saved as {filename}")

            return {
                "result": f"[{node}] LLM ({model}) generated code for '{subject[:50]}' — {len(generated)} chars",
                "files": [{"filename": filename,
                            "content_type": "text/plain",
                            "content": generated,
                            "size": len(generated.encode("utf-8"))}],
                "context_updates": {"task_type": "code_generation", "language": ext, "model": model, "llm": True},
            }
        except Exception as e:
            log.warning(f"[{node}] LLM generation error: {e}")
            return None

    async def _task_code_generation(self, node: str, now, subject: str, desc_text: str) -> dict:
        """Generate code using LLM (Ollama) or fallback to templates."""
        import platform
        import json as _json

        # Try LLM generation first
        llm_result = await self._task_llm_generate(node, now, subject, desc_text)
        if llm_result:
            return llm_result

        # ── Fallback: template-based generation ──
        task_summary = subject[:80] if subject else "generic task"

        # Detect language from keywords
        lang = "python"
        lower_desc = desc_text.lower()
        if any(kw in lower_desc for kw in ("bash", "shell", "sh ", "script.sh")):
            lang = "bash"
        elif any(kw in lower_desc for kw in ("javascript", "js ", "node")):
            lang = "javascript"
        elif any(kw in lower_desc for kw in ("html", "weboldal", "css")):
            lang = "html"

        # Extract task intent from subject/description for context-aware generation
        task_summary = subject[:80] if subject else "generic task"
        
        # Detect specific computation patterns
        is_math = any(kw in lower_desc for kw in ("fibonacci", "primszam", "prime", "szamold", "szamol", "calculate", "compute", "factorial", "sqrt"))
        is_system = any(kw in lower_desc for kw in ("rendszer", "system", "monitor", "status", "health", "cpu", "mem", "disk"))
        is_network = any(kw in lower_desc for kw in ("ping", "network", "halozat", "dns", "port scan"))
        is_file = any(kw in lower_desc for kw in ("fajl", "file", "directory", "konyvtar", "listazd", "ls"))

        if lang == "python":
            if is_math:
                # Math-focused script
                import re
                numbers = re.findall(r'\d+', desc_text)
                n = int(numbers[0]) if numbers else 20
                if "fibonacci" in lower_desc or "fib" in lower_desc:
                    code = f'''#!/usr/bin/env python3
"""A2A Mesh - {node}
Task: {task_summary}
Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
"""
import json

def fibonacci(n):
    """Generate first n Fibonacci numbers."""
    if n <= 0: return []
    if n == 1: return [0]
    fib = [0, 1]
    while len(fib) < n:
        fib.append(fib[-1] + fib[-2])
    return fib

if __name__ == "__main__":
    result = fibonacci({n})
    print(f"Fibonacci first {n} numbers:")
    print(result)
    print(json.dumps({{"agent": "{node}", "fibonacci_{n}": result, "count": len(result)}}, indent=2))
'''
                elif "prime" in lower_desc or "primszam" in lower_desc:
                    code = f'''#!/usr/bin/env python3
"""A2A Mesh - {node}
Task: {task_summary}
Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
"""
import json

def primes(n):
    """Generate first n prime numbers using Sieve of Eratosthenes."""
    if n <= 0: return []
    sieve_size = max(n * 15, 100)
    sieve = [True] * sieve_size
    sieve[0] = sieve[1] = False
    for i in range(2, int(sieve_size**0.5) + 1):
        if sieve[i]:
            for j in range(i*i, sieve_size, i):
                sieve[j] = False
    return [i for i, is_p in enumerate(sieve) if is_p][:n]

if __name__ == "__main__":
    result = primes({n})
    print(f"First {n} primes:")
    print(result)
    print(json.dumps({{"agent": "{node}", "primes_{n}": result, "count": len(result)}}, indent=2))
'''
                else:
                    code = f'''#!/usr/bin/env python3
"""A2A Mesh - {node}
Task: {task_summary}
Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
"""
import json
import math

if __name__ == "__main__":
    n = {n}
    result = {{"agent": "{node}", "input": n, "sqrt": math.sqrt(n), "factorial_approx": "large"}}
    print(f"Computed for n={n}")
    print(json.dumps(result, indent=2))
'''
            elif is_system:
                code = f'''#!/usr/bin/env python3
"""A2A Mesh - {node}
Task: {task_summary}
Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
"""
import json, platform, os
from datetime import datetime

def get_system_info():
    info = {{
        "agent": "{node}",
        "hostname": platform.node(),
        "os": platform.system(),
        "cpu_count": os.cpu_count(),
        "load_avg": os.getloadavg() if hasattr(os, "getloadavg") else None,
        "timestamp": datetime.utcnow().isoformat(),
    }}
    try:
        import psutil
        info["cpu_pct"] = psutil.cpu_percent(interval=1)
        info["mem_pct"] = psutil.virtual_memory().percent
        info["disk_pct"] = psutil.disk_usage("/").percent
    except ImportError:
        pass
    return info

if __name__ == "__main__":
    print(json.dumps(get_system_info(), indent=2))
'''
            else:
                # Generic Python script with task context
                code = f'''#!/usr/bin/env python3
"""A2A Mesh - {node}
Task: {task_summary}
Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
"""
import json
import platform
from datetime import datetime

def main():
    print(f"Agent: {node}")
    print(f"Host: {{platform.node()}}")
    print(f"Time: {{datetime.utcnow().isoformat()}}")
    result = {{"agent": "{node}", "host": platform.node(), "status": "ok", "task": "{_safe_ascii(task_summary)}"}}
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
'''

        elif lang == "bash":
            if is_network:
                code = f'''#!/bin/bash
# A2A Mesh - {node}
# Task: {task_summary}
# Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
echo "Network diagnostics from {node}"
echo "Hostname: $(hostname)"
echo "--- Connectivity ---"
for host in google.com 8.8.8.8 github.com; do
    ping -c 1 -W 2 $host >/dev/null 2>&1 && echo "  $host: OK" || echo "  $host: FAIL"
done
echo "--- DNS ---"
nslookup google.com >/dev/null 2>&1 && echo "DNS: OK" || echo "DNS: FAIL"
echo "--- Ports ---"
for port in 80 443 22; do
    timeout 2 bash -c "echo >/dev/tcp/google.com/$port" 2>/dev/null && echo "  Port $port: OPEN" || echo "  Port $port: CLOSED"
done
'''
            elif is_system:
                code = f'''#!/bin/bash
# A2A Mesh - {node}
# Task: {task_summary}
# Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
echo "System info from {node}"
echo "Hostname: $(hostname)"
echo "Uptime: $(uptime -p 2>/dev/null || uptime)"
echo "CPU load: $(cat /proc/loadavg 2>/dev/null || echo N/A)"
echo "Memory: $(free -h 2>/dev/null | head -2 || echo N/A)"
echo "Disk: $(df -h / 2>/dev/null | tail -1 || echo N/A)"
echo "Processes: $(ps aux 2>/dev/null | wc -l)"
'''
            else:
                code = f'''#!/bin/bash
# A2A Mesh - {node}
# Task: {task_summary}
# Generated: {now.strftime("%Y-%m-%d %H:%M:%S UTC")}
echo "Agent: {node}"
echo "Host: $(hostname)"
echo "Time: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Status: ok"
'''

        elif lang == "html":
            # Check if this is a showcase/landing page request
            is_showcase = any(kw in lower_desc for kw in ("bemutato", "showcase", "landing", "termek", "product", "marketing", "portfolio"))
            if is_showcase:
                code = _SHOWCASE_HTML.replace("NODE_NAME", node).replace("TIMESTAMP", now.strftime("%Y-%m-%d %H:%M UTC"))
            else:
                code = f'''<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>{_safe_ascii(subject)}</title>
<style>body{{font-family:sans-serif;max-width:800px;margin:2em auto;background:#1a1a2e;color:#eee}}h1{{color:#e94560}}.info{{background:#16213e;padding:1em;border-radius:8px;margin:1em 0}}</style></head>
<body><h1>{_safe_ascii(subject)}</h1>
<div class="info"><p>Generated by <strong>{node}</strong></p><p>{now.strftime("%Y-%m-%d %H:%M UTC")}</p></div>
</body></html>'''

        else:
            code = f'// A2A Mesh - {node}\n// Task: {task_summary}\nconsole.log("Hello from {node}!");\n'

        save_path = f"/tmp/generated_{lang}_{node}.txt"
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(code)
        except Exception:
            save_path = ""

        return {
            "result": f"[{node}] Generated {lang} code for '{task_summary[:50]}' at {now.isoformat()}",
            "files": [{"filename": f"generated_{lang}_{node}_{now.strftime('%Y%m%d_%H%M%S')}.{lang}",
                        "content_type": "text/plain", "content": code,
                        "size": len(code.encode("utf-8"))}],
            "context_updates": {"task_type": "code_generation", "language": lang, "save_path": save_path},
        }

    async def _task_network_check(self, node: str, now, desc_text: str) -> dict:
        """Check network connectivity and DNS resolution."""
        import subprocess
        import re

        checks = []
        targets = ["1.1.1.1", "8.8.8.8", "google.com"]

        # Extract host/port from description
        for word in desc_text.split():
            if "." in word and not word.startswith("/"):
                targets.insert(0, word.rstrip(",."))

        # Ping checks
        for target in targets[:5]:
            try:
                result = subprocess.run(["ping", "-c", "2", "-W", "3", target],
                                        capture_output=True, text=True, timeout=10)
                latency = "timeout"
                match = re.search(r'min/avg/.*?=\s*([\d.]+)', result.stdout)
                if match:
                    latency = f"{match.group(1)}ms"
                checks.append({"target": target, "type": "ping", "latency": latency,
                               "success": result.returncode == 0})
            except Exception as e:
                checks.append({"target": target, "type": "ping", "error": str(e)[:100]})

        # DNS check
        try:
            result = subprocess.run(["nslookup", "google.com"], capture_output=True, text=True, timeout=5)
            dns_ok = "Address" in result.stdout or "address" in result.stdout
            checks.append({"type": "dns", "target": "google.com", "success": dns_ok})
        except Exception:
            checks.append({"type": "dns", "success": False})

        report = {"node": node, "timestamp": now.isoformat(), "checks": checks}
        import json as _jj
        report_str = _jj.dumps(report, indent=2, ensure_ascii=False)

        return {
            "result": f"[{node}] Network check completed: {sum(1 for c in checks if c.get('success'))}/{len(checks)} OK",
            "files": [{"filename": f"network_{node}_{now.strftime('%Y%m%d_%H%M%S')}.json",
                        "content_type": "application/json", "content": report_str,
                        "size": len(report_str.encode("utf-8"))}],
            "context_updates": {"task_type": "network_check",
                                "checks_ok": str(sum(1 for c in checks if c.get('success')))},
        }

    async def stop(self):
        """Stop all transports and discovery. Deregister from mesh."""
        if not self._running:
            return  # Already stopped, avoid double-stop
        log.info(f"Stopping mesh node '{self.node_name}'")
        await self.debug_log("WARNING", "shutdown", f"Node {self.node_name} shutting down")
        self._running = False

        # Stop plugins first (they may need mesh to send final messages)
        try:
            await self.plugin_loader.stop_all()
            log.info("All plugins stopped")
        except Exception as e:
            log.warning(f"Plugin shutdown error (non-fatal): {e}")

        # Stop topology tuner
        try:
            await self.topology_tuner.stop()
            log.info("Topology tuner stopped")
        except Exception as e:
            log.warning(f"Topology tuner stop error (non-fatal): {e}")

        # Stop ACK manager
        await self.ack_manager.stop()

        # Stop delegation manager
        await self.delegation.stop()

        # Stop priority queue processor
        await self.router.stop_priority_queue()

        # Stop peer discovery
        await self.peer_discovery.stop()

        # Deregister from mesh_nodes
        try:
            await self._deregister_node()
        except Exception as e:
            log.warning(f"Failed to deregister node: {e}")

        # Cancel tasks
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Stop transports
        await self._pg_transport.stop()
        await self._p2p_transport.stop()
        await self._http_transport.stop()
        await self._ble_transport.stop()
        await self._discovery.stop()
        await self._udp_discovery.stop()

        # Close asyncpg connection pool
        if self._pg_pool:
            try:
                await self._pg_pool.close()
            except Exception:
                pass

        log.info("Mesh node stopped")

    async def _on_mdns_discover(self, node_info: dict):
        """Handle mDNS discovered node — add to peer_discovery and connect.

        This is the bridge between mDNS discovery and P2P mesh formation,
        enabling PG-independent peer discovery on local network and Tailscale.
        """
        name = node_info.get("name", "")
        host = node_info.get("host", "")
        port = node_info.get("port", 8645)

        if not name or name == self.node_name:
            return  # Skip self

        log.info(f"mDNS discovered peer: {name} at {host}:{port}")

        # Check if we already know this peer with correct port
        existing = self.peer_discovery.get_peer(name)
        if existing and existing.p2p_available and name in (self._p2p_transport._peers if self._p2p_transport else {}):
            log.debug(f"mDNS: {name} already connected, skipping")
            return

        # Add or update peer in peer_discovery (mDNS port takes priority over stale data)
        peer = self.peer_discovery.add_peer(
            name=name,
            host=host,
            port=port,
            role="router",
            p2p_port=port,
            health_port=self.config.health_port,  # Use configured health_port instead of hardcoded convention
        )

        # Auto-approve discovered peer
        self.peer_discovery.approve_peer(name)

        # Connect via P2P if not already connected
        if self._p2p_transport and name not in self._p2p_transport._peers:
            log.info(f"mDNS: Attempting P2P connection to {name} at {host}:{port}")
            await self.peer_discovery.connect_to_peer(peer)

    async def send(self, message: A2AMessage) -> SendResult:
        """Send a message via the best available transport.

        Includes: size validation, compression, ACK tracking, offline queuing.
        """
        message.sender = self.node_name
        message.sender_node_id = self.node_name

        # Validate message size
        valid, size = message.validate_size()
        if not valid:
            log.error(f"Message {message.id[:8]} too large: {size} bytes (max {MAX_MESSAGE_SIZE})")
            return SendResult(transport="none", success=False, error=f"Message too large: {size} bytes")

        # Compress if needed
        message = message.compress_payload()

        # Sign if encryption is available
        if self.encryption and not message.signature:
            content = message.sign_content()
            message.signature = self.encryption.sign_message(content)

        # Check if recipient is online — if not, queue for later
        if not message.is_broadcast() and self.offline_queue.is_node_online(message.recipient) is False:
            log.info(f"Recipient {message.recipient} is offline — queuing message")
            self.offline_queue.enqueue(message)
            return SendResult(transport="offline_queue", success=True, error="Queued for offline delivery")

        # Track for ACK (non-broadcast only)
        if not message.is_broadcast() and message.type != MSG_TYPE_HEARTBEAT:
            self.ack_manager.track(message)

        # Also persist to PG for reliability
        await self._persist_message(message)

        return await self.router.send(message)

    async def send_direct(self, recipient: str, msg_type: str,
                          payload: dict, priority: int = 5) -> SendResult:
        """Convenience method to send a directed message."""
        msg = A2AMessage.create(
            sender=self.node_name,
            recipient=recipient,
            msg_type=msg_type,
            payload=payload,
            priority=priority,
        )
        return await self.send(msg)

    async def broadcast(self, msg_type: str, payload: dict,
                        priority: int = 5) -> SendResult:
        """Convenience method to broadcast a message."""
        msg = A2AMessage.create(
            sender=self.node_name,
            recipient="broadcast",
            msg_type=msg_type,
            payload=payload,
            priority=priority,
        )
        return await self.send(msg)

    async def send_file(self, file_path: str, recipient: str,
                        priority: int = 5) -> Tuple[SendResult, str]:
        """Send a file to a peer via P2P file transfer.

        Creates a FILE_OFFER message. When the recipient accepts (FILE_ACCEPT),
        chunks are sent automatically via _send_file_chunks.

        Returns (SendResult, file_id).
        """
        try:
            offer_msg, file_id = self.file_transfer.create_offer_message(
                file_path, recipient, priority
            )
            result = await self.send(offer_msg)
            log.info(f"File transfer initiated: {file_id} → {recipient} ({os.path.basename(file_path)})")
            return result, file_id
        except FileNotFoundError as e:
            log.error(f"File transfer failed: {e}")
            return SendResult(transport="p2p", success=False, error=str(e)), ""

    async def _send_file_chunks(self, file_id: str, recipient: str):
        """Send all chunks of a file after FILE_ACCEPT is received.

        Iterates through all chunks, sends each via the mesh, then sends FILE_COMPLETE.
        
        Optimization: Uses adaptive inter-chunk delay based on P2P transport
        constants. Small delay between chunks prevents overwhelming the receiver.
        """
        import asyncio

        transfer = self.file_transfer._outgoing.get(file_id)
        if not transfer:
            log.error(f"Cannot send chunks: no outgoing transfer for {file_id}")
            return

        chunk_count = transfer["chunk_count"]
        log.info(f"Starting chunk transfer: {file_id} → {recipient} ({chunk_count} chunks)")

        for chunk_index in range(chunk_count):
            try:
                chunk_msg = self.file_transfer.create_chunk_message(
                    file_id, chunk_index, recipient, priority=3
                )
                if chunk_msg is None:
                    log.error(f"Failed to create chunk {chunk_index} for {file_id}")
                    continue

                result = await self.send(chunk_msg)
                if not result.success:
                    log.warning(f"Chunk {chunk_index} send failed for {file_id}: {result.error}")
                else:
                    log.debug(f"Chunk {chunk_index}/{chunk_count} sent for {file_id}")

                # Adaptive delay: small delay to avoid overwhelming the receiver
                # File chunks are priority 7 (lowest), so they'll yield to higher-priority messages
                await asyncio.sleep(0.01)

                # Update transfer state
                transfer["current_chunk"] = chunk_index + 1

            except Exception as e:
                log.error(f"Error sending chunk {chunk_index} for {file_id}: {e}")
        # Send FILE_COMPLETE
        complete_msg = self.file_transfer.create_complete_message(file_id, recipient)
        result = await self.send(complete_msg)
        log.info(f"FILE_COMPLETE sent for {file_id} → {recipient} (result: {result.success})")
        result = await self.send(complete_msg)
        log.info(f"FILE_COMPLETE sent for {file_id} → {recipient} (result: {result.success})")

    async def _on_p2p_ack(self, ack_for_id: str, ack_type: str):
        """Callback when a P2P ACK is received — update message status in PG."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            return
        try:
            await self._pg_pool.execute("""
                UPDATE mesh.mesh_messages SET status = 'acknowledged'
                WHERE id = $1
            """, ack_for_id)
            log.info(f"PG message {ack_for_id[:8]} status → acknowledged (P2P ACK: {ack_type})")
        except Exception as e:
            log.error(f"Failed to update message status for ACK {ack_for_id[:8]}: {e}")

    async def _on_p2p_peer_connected(self, peer_name: str):
        """Callback when a P2P connection is established (including reconnects).
        Registers the peer with the agent registry and sends our skills to the peer
        for auto-discovery. Skills are shared automatically on every P2P connect."""
        if not self.peer_discovery:
            return
        peer = self.peer_discovery.get_peer(peer_name)
        if peer:
            log.info(f"P2P peer_connected callback: registering {peer_name} with agent registry")
            await self.debug_log("INFO", "transport", f"Peer {peer_name} connected via P2P")
            peer.p2p_available = True
            self.peer_discovery._register_discovered_peer(peer)
        else:
            log.warning(f"P2P peer_connected callback: peer {peer_name} not found in discovery, skipping registry")

        # Cancel pending grace-period offline broadcast — peer reconnected
        pending_task = self._peer_offline_grace_tasks.pop(peer_name, None)
        if pending_task and not pending_task.done():
            pending_task.cancel()
            log.info(f"Cancelled pending offline broadcast for {peer_name} — peer reconnected during grace period")

        # Notify plugins about peer connection
        if hasattr(self, 'plugin_loader') and self.plugin_loader.plugins:
            asyncio.create_task(self.plugin_loader.dispatch_peer_connected(peer_name, peer or {}))

        # Health recovery: record success on P2P reconnect to recover health score
        # after temporary disconnects that drove the score to 0
        if hasattr(self, 'router') and hasattr(self.router, '_health_scorer'):
            record = self.router._health_scorer.get_record(peer_name)
            if record.health_score < 1.0:
                # Boost recovery: record multiple successes proportional to damage
                consecutive = record.consecutive_failures
                # Each success recovers by recovery_factor (0.05 by default)
                # We want to reach ~0.8 after reconnect, so compensate for past failures
                successes_needed = min(max(consecutive, 3), 20)
                old_score = record.health_score
                for _ in range(successes_needed):
                    self.router._health_scorer.record_success(peer_name, latency_ms=10.0)
                log.info(f"Health recovery: {peer_name} P2P reconnected, boosted score from {old_score:.2f} to {record.health_score:.2f} with {successes_needed} successes")

        # P2P Skill Auto-Discovery: send our skills to the newly connected peer
        # Try P2P first, fall back to PG NOTIFY (guaranteed delivery)
        # Rate limited: max 1 announcement per 60s (same as _on_peer_discovered)
        import time as _time
        now = _time.time()
        if now - self._last_skills_announcement < 60:
            log.debug(f"Skipping P2P skills announcement to {peer_name} — rate limited (last sent {now - self._last_skills_announcement:.0f}s ago)")
            return
        self._last_skills_announcement = now
        skills = list(getattr(self.config, 'skills', []) or [])
        if skills:
            sent_via = []
            # Try P2P transport
            if self._p2p_transport and self._p2p_transport.is_available():
                try:
                    from .core.message import A2AMessage
                    import uuid
                    skills_msg = A2AMessage(
                        id=str(uuid.uuid4()),
                        sender=self.node_name,
                        recipient=peer_name,
                        payload={
                            "type": "skills_announcement",
                            "skills": skills,
                            "capabilities": list(getattr(self.config, 'capabilities', []) or []),
                        },
                        type="skills_announcement",
                        priority=5,
                    )
                    await self._p2p_transport.send(skills_msg)
                    sent_via.append("p2p")
                    log.info(f"P2P skill announcement sent to {peer_name}: {[s.get('id','?') for s in skills]}")
                except Exception as e:
                    log.debug(f"P2P skills announcement failed for {peer_name}: {e}")
            # Always send via PG NOTIFY as backup (guaranteed delivery)
            if hasattr(self, '_pg_transport') and self._pg_transport and self._pg_transport.is_available():
                try:
                    from .core.message import A2AMessage
                    import uuid
                    pg_skills_msg = A2AMessage(
                        id=str(uuid.uuid4()),
                        sender=self.node_name,
                        recipient="*",
                        payload={
                            "type": "skills_announcement",
                            "skills": skills,
                            "capabilities": list(getattr(self.config, 'capabilities', []) or []),
                        },
                        type="skills_announcement",
                        priority=5,
                    )
                    await self._pg_transport.send(pg_skills_msg)
                    sent_via.append("pg")
                    log.info(f"PG skill announcement sent to {peer_name}: {[s.get('id','?') for s in skills]}")
                except Exception as e:
                    log.debug(f"PG skills announcement failed for {peer_name}: {e}")
            if sent_via:
                log.info(f"Skills announcement sent to {peer_name} via {sent_via}: {[s.get('id','?') for s in skills]}")

    async def _on_p2p_peer_disconnected(self, peer_name: str):
        """Callback when a P2P peer disconnects. Schedules a grace-period offline broadcast.

        Instead of immediately broadcasting peer_offline, we start a grace period
        (default 30s). If the peer reconnects within the grace period, the pending
        broadcast is cancelled — this eliminates false offline alerts during restarts
        and brief connection drops. After the grace period, the offline broadcast
        proceeds with the existing debounce logic.
        """
        import time

        # Mark P2P unavailable immediately (routing accuracy)
        if self.peer_discovery and peer_name in self.peer_discovery._peers:
            self.peer_discovery._peers[peer_name].p2p_available = False

        # Cancel any existing grace task for this peer (shouldn't happen, but be safe)
        existing_task = self._peer_offline_grace_tasks.get(peer_name)
        if existing_task and not existing_task.done():
            existing_task.cancel()

        # Schedule delayed offline broadcast with grace period
        grace_seconds = self._peer_offline_grace_seconds
        log.info(f"Peer {peer_name} disconnected — scheduling offline broadcast after {grace_seconds}s grace period")

        async def _grace_period_broadcast():
            """Wait grace period, then broadcast peer_offline if not cancelled."""
            try:
                await asyncio.sleep(grace_seconds)
            except asyncio.CancelledError:
                log.info(f"Grace period cancelled for {peer_name} — peer reconnected, skipping offline broadcast")
                self._peer_offline_debounce.pop(peer_name, None)
                return

            # Grace period expired — peer is genuinely offline
            now = time.time()
            last_broadcast = self._peer_offline_debounce.get(peer_name, 0)
            if now - last_broadcast < 60:
                log.info(f"Peer {peer_name} offline grace expired — debouncing (last broadcast {now - last_broadcast:.0f}s ago)")
                self._peer_offline_grace_tasks.pop(peer_name, None)
                return

            self._peer_offline_debounce[peer_name] = now
            log.warning(f"Peer {peer_name} offline grace expired — broadcasting offline notification")

            # Record failure in health scorer
            if hasattr(self, 'router') and hasattr(self.router, '_health_scorer'):
                self.router._health_scorer.record_failure(peer_name)

            # Broadcast peer_offline to the mesh
            try:
                offline_msg = A2AMessage.create(
                    sender=self.node_name,
                    recipient="broadcast",
                    msg_type="peer_offline",
                    payload={
                        "type": "peer_offline",
                        "peer_name": peer_name,
                        "source": self.node_name,
                        "timestamp": time.time(),
                    },
                    priority=7,  # High priority — routing info needs fast propagation
                )
                await self.router.send(offline_msg)
                log.info(f"Broadcast peer_offline for {peer_name}")
            except Exception as e:
                log.error(f"Failed to broadcast peer_offline for {peer_name}: {e}")

            # Update peer discovery status
            if self.peer_discovery and peer_name in self.peer_discovery._peers:
                peer = self.peer_discovery._peers[peer_name]
                peer.p2p_available = False
                log.info(f"Marked peer {peer_name} as P2P unavailable in discovery")

            self._peer_offline_grace_tasks.pop(peer_name, None)

        task = asyncio.create_task(_grace_period_broadcast())
        self._peer_offline_grace_tasks[peer_name] = task

    async def _on_peer_discovered(self, peer_name: str):
        """Callback when a new peer is discovered (via PG or static config).
        Sends our skills announcement via PG broadcast — rate limited to max 1 per 60s."""
        import time as _time
        now = _time.time()
        if now - self._last_skills_announcement < 60:
            log.debug(f"Skipping skills announcement — rate limited (last sent {now - self._last_skills_announcement:.0f}s ago)")
            return
        self._last_skills_announcement = now
        skills = list(getattr(self.config, 'skills', []) or [])
        if not skills:
            return
        capabilities = list(getattr(self.config, 'capabilities', []) or [])
        
        # Always send via PG NOTIFY as broadcast (guaranteed delivery to all nodes)
        if hasattr(self, '_pg_transport') and self._pg_transport and self._pg_transport.is_available():
            try:
                from .core.message import A2AMessage
                import uuid
                pg_msg = A2AMessage(
                    id=str(uuid.uuid4()),
                    sender=self.node_name,
                    recipient="*",
                    payload={
                        "type": "skills_announcement",
                        "skills": skills,
                        "capabilities": capabilities,
                    },
                    type="skills_announcement",
                    priority=5,
                )
                await self._pg_transport.send(pg_msg)
                log.info(f"PG skills announcement broadcast on peer discovery: {[s.get('id','?') for s in skills]}")
            except Exception as e:
                log.warning(f"PG skills announcement broadcast failed on peer discovery: {e}")

    # ─── Health Endpoint ─────────────────────────────────────────────

    def _auto_register_self(self):
        """Auto-register this node in the dashboard's agent registry.
        Also sends PG NOTIFY for near-instant discovery by other nodes (P0 optimization).
        
        Capabilities are loaded from config and augmented based on node role and
        available transports. Every agent registers its full capability list on startup."""
        from .core.registry import AgentCard

        # Start with configured capabilities
        capabilities = list(getattr(self.config, 'capabilities', []) or [
            "a2a_messaging", "file_transfer"
        ])
        
        # Add role-based capabilities
        if self.role == NodeRole.COORDINATOR:
            capabilities.extend(["coordinator", "dashboard", "registry"])
        
        # Add transport-based capabilities
        if hasattr(self, '_p2p_transport') and self._p2p_transport and self._p2p_transport.is_available():
            capabilities.append("p2p_transport")
        
        pg_transport = getattr(self, 'transports', {}).get('pg_notify') if hasattr(self, 'transports') else None
        if pg_transport and pg_transport.is_available():
            capabilities.append("pg_transport")
        
        # Add health monitoring capability (all nodes have this)
        if "health_monitor" not in capabilities:
            capabilities.append("health_monitor")
        
        # Deduplicate (filter out non-hashable items like dicts)
        capabilities = list(set(c for c in capabilities if isinstance(c, (str, int, float, tuple))))

        # Load skills from config (auto-discovery: skills shared via P2P handshake)
        skills = list(getattr(self.config, 'skills', []) or [])

        endpoint = f"http://{self.config.p2p.listen_host}:{self.config.health_port or 8650}"
        card = AgentCard(
            name=self.node_name,
            capabilities=capabilities,
            skills=skills,
            version=self._resolved_version,
            description=f"A2A Mesh node ({self.role.value})",
            endpoint=endpoint,
            health_endpoint="/api/status",
            max_concurrent=getattr(self.config, 'max_concurrent', 10),
        )

        if hasattr(self, 'dashboard') and hasattr(self.dashboard, 'registry'):
            self.dashboard.registry.register(card, force=True)
            log.info(f"Auto-registered self in registry: {self.node_name} caps={card.capabilities} skills={[s.get('id','?') for s in skills]}")

        # P0: Send PG NOTIFY for near-instant peer discovery
        self._notify_node_update("register")

    def _notify_node_update(self, action: str = "register"):
        """Send PG NOTIFY for peer discovery (P0: near-instant node discovery).
        Other nodes listening on mesh_node_update channel will discover this node immediately.
        
        Uses parameterized query via asyncpg pool to prevent SQL injection.
        """
        import json
        try:
            if not self._pg_pool or not self._pg_pool.is_connected():
                return
            payload = json.dumps({
                "node": self.node_name,
                "action": action,
                "endpoint": f"{self.config.p2p.listen_host}:{self.config.p2p.listen_port}",
                "capabilities": list(set(c for c in (getattr(self.config, 'capabilities', []) or ["a2a_messaging"]) if isinstance(c, (str, int, float, tuple)))),
            })
            # Use asyncpg notify — schedule it as a task
            asyncio.create_task(self._pg_pool.notify("mesh_node_update", payload))
            log.debug(f"PG NOTIFY sent: mesh_node_update action={action} node={self.node_name}")
        except Exception as e:
            log.debug(f"Could not send PG NOTIFY for {action}: {e}")

    async def _run_health_server(self):
        """Simple HTTP health check server on configured port."""
        try:
            from aiohttp import web
        except ImportError:
            log.warning("aiohttp not installed — health endpoint disabled")
            return

        async def health_handler(request):
            """Return node health status as JSON."""
            from core.auto_updater import AutoUpdater
            uptime = time.time() - self._start_time if self._start_time else 0
            updater_status = getattr(self, '_updater_state', {}) or {}
            if not updater_status:
                # Fallback: create temporary updater for status
                try:
                    updater = AutoUpdater(node=self)
                    updater_status = updater.get_status()
                    await updater.close()
                except Exception:
                    pass
            status = {
                "status": "running" if self._running else "stopped",
                "node": self.node_name,
                "role": self.role.value,
                "address": f"0x{self.mesh_address.short:04X}" if self.mesh_address else "pending",
                "uptime_seconds": round(uptime, 1),
                "version": self._resolved_version,
                "updater": updater_status,
                "transports": {
                    "pg": self._pg_transport.is_available(),
                    "p2p": self._p2p_transport.is_available(),
                    "http": self._http_transport.is_available(),
                    "ble": self._ble_transport.is_available(),
                },
                "election": self.election.get_status() if self.election else {},
                "ack": self.ack_manager.get_stats(),
                "offline_queue": await self.offline_queue.get_stats(),
                "auto_steer": self.auto_steer.get_stats(),
                "local_store": self.local_store.get_stats(),
                "file_transfer": self.file_transfer.get_transfer_stats(),
                "peer_discovery": self.peer_discovery.get_stats(),
                "p2p": {
                    "listen_port": self._p2p_transport._listen_port,
                    "tls_enabled": self._p2p_transport._ssl_context is not None,
                    "peers": list(self._p2p_transport._peers.keys()),
                    "peer_addresses": dict(self._p2p_transport._peer_addresses),
                    "backoff_peers": {k: f"{max(0, v - time.time()):.0f}s" for k, v in self._p2p_transport._peer_backoff.items()},
                    "incoming_queue": self._p2p_transport._incoming_queue.qsize(),
                },
                "dashboard": self.dashboard.get_stats(),
                "messages_sent": self.router._stats.get("sent", 0),
                "messages_received": self.router._stats.get("received", 0),
                "topology_tuner": self.topology_tuner.stats if hasattr(self, 'topology_tuner') else None,
            }
            return web.json_response(status=200 if self._running else 503, data=status)

        async def ready_handler(request):
            """Readiness check — returns 200 only if PG transport is available."""
            if self._pg_transport.is_available():
                return web.json_response({"ready": True})
            return web.json_response({"ready": False}, status=503)

        app = web.Application()
        app.router.add_get("/health", health_handler)
        app.router.add_get("/ready", ready_handler)

        # Update API endpoints
        async def update_check_handler(request):
            """Check for available updates."""
            from core.auto_updater import AutoUpdater
            updater = AutoUpdater(node=self)
            try:
                latest = await updater.check_for_update()
                current = updater.current_version
                await updater.close()
                if latest:
                    return web.json_response({
                        "update_available": True,
                        "current_version": current,
                        "latest_version": latest.lstrip("v"),
                        "latest_tag": latest,
                    })
                return web.json_response({
                    "update_available": False,
                    "current_version": current,
                })
            except Exception as e:
                await updater.close()
                return web.json_response({"error": str(e)}, status=500)

        async def update_apply_handler(request):
            """Apply an update."""
            from core.auto_updater import AutoUpdater
            version = request.query.get("version")
            updater = AutoUpdater(node=self)
            try:
                result = await updater.apply_update(version)
                await updater.close()
                return web.json_response({
                    "success": result.success,
                    "previous_version": result.previous_version,
                    "new_version": result.new_version,
                    "error": result.error,
                    "rollback_performed": result.rollback_performed,
                    "state": result.state.value,
                })
            except Exception as e:
                await updater.close()
                return web.json_response({"error": str(e)}, status=500)

        async def update_status_handler(request):
            """Get updater status."""
            from core.auto_updater import AutoUpdater
            updater = AutoUpdater(node=self)
            status = updater.get_status()
            await updater.close()
            return web.json_response(status)

        app.router.add_get("/update/check", update_check_handler)
        app.router.add_post("/update/apply", update_apply_handler)
        app.router.add_get("/update/status", update_status_handler)

        # Register dashboard routes
        self.dashboard.register_routes(app)

        try:
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", self._health_port)
            await site.start()
            log.info(f"Health endpoint started on port {self._health_port}")
            # Keep running until stopped
            while self._running:
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
        except OSError as e:
            if "address already in use" in str(e).lower() or getattr(e, 'errno', None) in (48, 98, 10048):
                log.info(f"Health endpoint port {self._health_port} already in use — dashboard handles it")
            else:
                log.error(f"Health endpoint failed: {e}")
        except Exception as e:
            log.error(f"Health endpoint failed: {e}")
        finally:
            try:
                await runner.cleanup()
            except Exception:
                pass

    # ─── PG Connection & Persistence ───────────────────────────────

    async def _init_pg_write_conn(self) -> bool:
        """Initialize the asyncpg connection pool for all database operations.

        Replaces the old psycopg2 synchronous connection with asyncpg pool.
        All DB operations are now async and non-blocking.
        """
        try:
            self._pg_pool = AsyncDBPool(self.config)
            if not await self._pg_pool.connect():
                log.error("Failed to create asyncpg connection pool")
                self._pg_pool = None
                return False
            log.info("AsyncPG connection pool established")
            # Initialize offline queue pool
            await self.offline_queue.init_pool(self._pg_pool)
            await self.offline_queue.ensure_table()
            return True
        except Exception as e:
            log.error(f"AsyncPG connection pool failed: {e}")
            self._pg_pool = None
            return False

    async def _persist_message(self, message: A2AMessage):
        """Persist message to mesh.mesh_messages for reliability and NOTIFY trigger.

        Uses asyncpg for non-blocking database operations.
        """
        if not self._pg_pool or not self._pg_pool.is_connected():
            return

        try:
            await self._pg_pool.execute("""
                INSERT INTO mesh.mesh_messages 
                    (id, sender, recipient, msg_type, priority, payload, 
                     routing_mode, src_addr, dst_addr, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'sent')
            """,
                message.id,
                message.sender,
                message.recipient,
                message.type,
                getattr(message, 'priority', 5),
                json.dumps(message.payload, default=str),
                getattr(message, 'routing_mode', 'hybrid'),
                self.mesh_address.short if self.mesh_address else None,
                None,  # dst_addr resolved later
            )
        except Exception as e:
            log.error(f"Failed to persist message {message.id[:8]}: {e}")

    async def _register_node(self):
        """Register this node in mesh.mesh_nodes with network info.
        
        Uses asyncpg for non-blocking database operations.
        Retries up to 3 times if PG connection is not available yet.
        """
        max_retries = 3
        for attempt in range(max_retries):
            if self._pg_pool and self._pg_pool.is_connected():
                break
            log.warning(f"_register_node: PG pool not available (attempt {attempt+1}/{max_retries}), retrying in 2s...")
            await asyncio.sleep(2)
        
        if not self._pg_pool or not self._pg_pool.is_connected():
            log.error("_register_node: PG pool not available after retries, skipping registration")
            return

        # Get capabilities from config (same as _auto_register_self)
        capabilities = list(getattr(self.config, 'capabilities', []) or [
            "a2a_messaging", "file_transfer"
        ])
        if self.role == NodeRole.COORDINATOR:
            capabilities.extend(["coordinator", "dashboard", "registry"])
        capabilities = list(set(c for c in capabilities if isinstance(c, (str, int, float, tuple))))

        # Determine host address for other nodes to connect to
        import socket
        try:
            host_ip = self._get_local_ip()
        except Exception:
            host_ip = "0.0.0.0"

        # Get port config — P2P port from transport config, health port from node config
        p2p_port = self.config.p2p.listen_port
        health_port = getattr(self.config, 'health_port', 8650)

        # All nodes start as 'active' — they've authenticated via TLS
        # and are connected to the mesh, so they're trusted.
        initial_status = 'active'

        try:
            await self._pg_pool.execute("""
                INSERT INTO mesh.mesh_nodes 
                    (node_name, role, short_addr, extended_uuid, parent_addr, depth, 
                     status, last_heartbeat, host, p2p_port, health_port,
                     pg_available, p2p_available, http_available, capabilities, version)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (node_name) DO UPDATE SET
                    role = EXCLUDED.role,
                    short_addr = EXCLUDED.short_addr,
                    status = mesh.mesh_nodes.status,  -- Don't override approved status
                    last_heartbeat = NOW(),
                    host = EXCLUDED.host,
                    p2p_port = EXCLUDED.p2p_port,
                    health_port = EXCLUDED.health_port,
                    pg_available = EXCLUDED.pg_available,
                    p2p_available = EXCLUDED.p2p_available,
                    http_available = EXCLUDED.http_available,
                    capabilities = EXCLUDED.capabilities,
                    version = EXCLUDED.version
            """,
                self.node_name,
                self.role.value,
                self.mesh_address.short if self.mesh_address else 0,
                str(self.mesh_address.extended) if self.mesh_address else self.node_name,
                self.mesh_address.parent_short if self.mesh_address else None,
                self.mesh_address.depth if self.mesh_address else 0,
                initial_status,
                host_ip,
                p2p_port,
                health_port,
                bool(self._pg_pool and self._pg_pool.is_connected()),
                self._p2p_transport.is_available() if hasattr(self, "_p2p_transport") else False,
                self._http_transport.is_available() if hasattr(self, "_http_transport") else False,
                json.dumps(capabilities, ensure_ascii=True),
                self._resolved_version,
            )
            log.info(f"Registered node {self.node_name} at {host_ip}:{p2p_port} in mesh")
            await self.debug_log("INFO", "startup", f"Node {self.node_name} registered at {host_ip}:{p2p_port}")
            # Notify other nodes immediately about our registration
            try:
                await self._pg_pool.execute("SELECT pg_notify('mesh_node_joined', $1)", self.node_name)
                log.info(f"Sent mesh_node_joined NOTIFY for {self.node_name}")
            except Exception as notify_err:
                log.debug(f"Could not send mesh_node_joined NOTIFY: {notify_err}")
        except Exception as e:
            # Handle short_addr unique constraint violation:
            # Another node may hold our short_addr from a previous session.
            if 'short_addr' in str(e) and 'duplicate' in str(e).lower():
                log.warning(f"short_addr conflict for {self.node_name}: {e} — clearing stale entry and retrying")
                await self.debug_log("WARNING", "election", f"short_addr conflict for {self.node_name}: {e}")
                try:
                    # Remove any stale node claiming our short_addr (but not our own row)
                    if self.mesh_address:
                        await self._pg_pool.execute("""
                            DELETE FROM mesh.mesh_nodes
                            WHERE short_addr = $1 AND node_name != $2
                        """, self.mesh_address.short, self.node_name)
                    # Also remove our own stale row if it exists with a different short_addr
                    await self._pg_pool.execute("""
                        DELETE FROM mesh.mesh_nodes
                        WHERE node_name = $1 AND short_addr != $2
                    """, self.node_name, self.mesh_address.short if self.mesh_address else 0)
                    # Retry registration
                    await self._pg_pool.execute("""
                        INSERT INTO mesh.mesh_nodes 
                            (node_name, role, short_addr, extended_uuid, parent_addr, depth, 
                             status, last_heartbeat, host, p2p_port, health_port,
                             pg_available, p2p_available, http_available, capabilities, version)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, $9, $10, $11, $12, $13, $14, $15)
                        ON CONFLICT (node_name) DO UPDATE SET
                            role = EXCLUDED.role,
                            short_addr = EXCLUDED.short_addr,
                            status = mesh.mesh_nodes.status,
                            last_heartbeat = NOW(),
                            host = EXCLUDED.host,
                            p2p_port = EXCLUDED.p2p_port,
                            health_port = EXCLUDED.health_port,
                            pg_available = EXCLUDED.pg_available,
                            p2p_available = EXCLUDED.p2p_available,
                            http_available = EXCLUDED.http_available,
                            capabilities = EXCLUDED.capabilities,
                            version = EXCLUDED.version
                    """,
                        self.node_name,
                        self.role.value,
                        self.mesh_address.short if self.mesh_address else 0,
                        str(self.mesh_address.extended) if self.mesh_address else self.node_name,
                        self.mesh_address.parent_short if self.mesh_address else None,
                        self.mesh_address.depth if self.mesh_address else 0,
                        initial_status,
                        host_ip,
                        p2p_port,
                        health_port,
                        bool(self._pg_pool and self._pg_pool.is_connected()),
                        self._p2p_transport.is_available() if hasattr(self, "_p2p_transport") else False,
                        self._http_transport.is_available() if hasattr(self, "_http_transport") else False,
                        json.dumps(capabilities, ensure_ascii=True),
                        self._resolved_version,
                    )
                    log.info(f"Registered node {self.node_name} at {host_ip}:{p2p_port} (retry succeeded)")
                except Exception as retry_e:
                    log.error(f"Failed to register node after retry: {retry_e}")
            else:
                log.error(f"Failed to register node: {e}")

    async def _deregister_node(self):
        """Mark this node as offline in mesh.mesh_nodes."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            return

        try:
            await self._pg_pool.execute("""
                UPDATE mesh.mesh_nodes SET status = 'offline', last_heartbeat = NOW()
                WHERE node_name = $1
            """, self.node_name)
            log.info(f"Deregistered node {self.node_name}")
        except Exception as e:
            log.error(f"Failed to deregister node: {e}")

    async def _update_heartbeat_pg(self):
        """Update heartbeat timestamp in PG and prune ghost nodes."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            return

        try:
            await self._pg_pool.execute("""
                UPDATE mesh.mesh_nodes SET 
                    last_heartbeat = NOW(), 
                    status = 'active',
                    pg_available = $1,
                    p2p_available = $2,
                    http_available = $3
                WHERE node_name = $4
            """,
                bool(self._pg_pool and self._pg_pool.is_connected()),
                self._p2p_transport.is_available() if hasattr(self, "_p2p_transport") else False,
                self._http_transport.is_available() if hasattr(self, "_http_transport") else False,
                self.node_name,
            )
        except Exception as e:
            log.error(f"Heartbeat PG update failed: {e}")

    # ─── Receive & Election Loops ──────────────────────────────────

    async def _receive_loop(self):
        """Main receive loop — polls all transports for incoming messages."""
        poll_count = 0
        while self._running:
            try:
                # Check each transport for messages
                for transport_name, transport in self.router.transports.items():
                    if not transport.is_available():
                        continue
                    try:
                        messages = await transport.receive()
                        if messages:
                            log.debug(f"Receive loop got {len(messages)} messages from {transport_name}")
                        for msg, from_transport in messages:
                            # Skip own messages (loop prevention)
                            if msg.sender == self.node_name:
                                continue

                            # Skip empty payloads (wake-agent noise, not real messages)
                            payload = msg.payload if hasattr(msg, 'payload') else None
                            if payload is None or (isinstance(payload, dict) and len(payload) == 0) or (isinstance(payload, str) and payload.strip() in ('', '{}')):
                                log.debug(f"Skipping empty payload message {msg.id[:8]} from {msg.sender}")
                                continue

                            result = await self.router.receive(msg, from_transport)
                            if result.status == "duplicate":
                                log.debug(f"Received message {msg.id[:8]} from {msg.sender} → {msg.recipient} via {from_transport}: {result.status}")
                            else:
                                log.info(f"Received message {msg.id[:8]} from {msg.sender} → {msg.recipient} via {from_transport}: {result.status}")
                            # Skip internal mesh protocol messages for dashboard notification
                            # (ACK, heartbeat, skills_announcement are not user-facing)
                            if result.status in ("processed", "forwarded") and msg.type not in (MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT, "skills_announcement", "memory_sync"):
                                # Notify dashboard for processed AND forwarded messages (chat visibility)
                                # Forwarded messages are replies to dashboard users that need to be displayed
                                try:
                                    await self.dashboard.on_mesh_message(msg)
                                except Exception as e:
                                    log.debug(f"Dashboard notification failed: {e}")

                            if result.status == "processed":
                                log.debug(f"Processing msg id={msg.id[:8]} type={msg.type} from {msg.sender} pri={msg.priority}")
                                # Wake the local agent for incoming messages, but NOT for
                                # ACK, heartbeat, or skills_announcement — these are internal
                                # mesh protocol messages that don't need agent processing
                                if msg.type not in (MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT, "skills_announcement", "memory_sync"):
                                    asyncio.create_task(self._trigger_webhook(msg))

                                # Critical mesh protocol messages must always go to handlers
                                # regardless of priority level (file_transfer, memory_sync)
                                if msg.type in ("file_transfer", "memory_sync"):
                                    log.info(f"Dispatching {msg.type} msg id={msg.id[:8]} from {msg.sender} to handlers")
                                    await self._dispatch_to_handlers(msg)
                                else:
                                    # Auto-steer classification and dispatch
                                    action = await self.auto_steer.process_message(msg)

                                    if action in ("interrupt", "steer_interrupt"):
                                        # P10+: immediate handler dispatch
                                        await self._dispatch_to_handlers(msg)
                                    elif action in ("high", "steer_queued"):
                                        # P7-9: handler dispatch
                                        await self._dispatch_to_handlers(msg)
                                        self.auto_steer._stats["high_priority_dispatched"] += 1
                                    elif action == "skipped":
                                        # Internal housekeeping — already counted in skipped_internal
                                        pass
                                    else:
                                        # P1-6: queued backlog processing
                                        await self.router.enqueue(msg)
                                        self.auto_steer._stats["normal_priority_dispatched"] += 1

                            # Process broadcast messages that were "forwarded" by the router
                            # (e.g., skills_announcement with recipient="*" is forwarded, not processed)
                            if result.status == "forwarded":
                                log.info(f"Forwarded msg: id={msg.id[:8]} type='{msg.type}' sender={msg.sender} recipient={msg.recipient}")
                                if msg.type == "skills_announcement":
                                    log.info(f"Processing forwarded skills_announcement from {msg.sender}")
                                    try:
                                        await self._dispatch_to_handlers(msg)
                                        log.info(f"Successfully processed skills_announcement from {msg.sender}")
                                    except Exception as e:
                                        log.error(f"Error processing skills_announcement from {msg.sender}: {e}", exc_info=True)
                                elif msg.recipient == "*":
                                    log.debug(f"Skipping forwarded broadcast msg type={msg.type} from {msg.sender}")

                    except Exception as e:
                        log.debug(f"Receive error on {transport_name}: {e}")

                await asyncio.sleep(0.1)  # 100ms polling interval

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Receive loop error: {e}")
                await asyncio.sleep(1)

    async def _heartbeat_loop(self):
        """Send periodic heartbeat messages."""
        while self._running:
            try:
                await asyncio.sleep(self.config.heartbeat.interval)
                if not self._running:
                    break

                uptime = int(time.time() - self._start_time)
                msg = A2AMessage.create(
                    sender=self.node_name,
                    recipient="broadcast",
                    msg_type=MSG_TYPE_HEARTBEAT,
                    payload={"uptime": uptime, "transports": list(self.router.transports.keys())},
                    priority=1,
                )

                # Persist heartbeat to PG
                await self._update_heartbeat_pg()

                # Also send via transport
                result = await self.router.send(msg)
                if not result.success:
                    log.warning(f"Heartbeat send failed: {result.error}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Heartbeat error: {e}")

    async def _election_monitor_loop(self):
        """Monitor coordinator health and trigger election if needed."""
        while self._running:
            try:
                # Check every heartbeat interval
                await asyncio.sleep(self.config.heartbeat.interval)

                if not self._running:
                    break

                # Get known routers from PG for election
                routers = await self._get_known_routers()

                # Check coordinator health
                state = self.election.check_coordinator_health(routers)

                if state == CoordinatorState.DOWN:
                    if self.election.should_initiate_election(routers):
                        claim = self.election.initiate_election()
                        log.warning(f"🏛️ Coordinator DOWN — claiming acting coordinator: {claim}")

                        # Broadcast election claim
                        await self.broadcast(
                            msg_type="coordinator_claim",
                            payload=claim,
                            priority=10,
                        )

                elif state == CoordinatorState.SUSPECTED:
                    log.warning(f"⚠️ Coordinator suspected — age: {time.time() - self.election.coordinator.last_heartbeat:.0f}s")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Election monitor error: {e}")

    async def _get_known_routers(self) -> list:
        """Get list of known routers from PG for election."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            return []

        try:
            rows = await self._pg_pool.fetch("""
                SELECT node_name, short_addr FROM mesh.mesh_nodes 
                WHERE role = 'router' AND status = 'active'
                ORDER BY short_addr
            """)
            return [(row['node_name'], row['short_addr']) for row in rows]
        except Exception as e:
            log.error(f"Failed to get routers: {e}")
            return []

    def _get_local_ip(self) -> str:
        """Get local IP address for mDNS registration."""
        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def get_status(self) -> dict:
        """Return node status."""
        status = {
            "node_name": self.node_name,
            "role": self.role.value,
            "running": self._running,
            "uptime": int(time.time() - self._start_time) if self._start_time else 0,
            "transports": self.router.get_stats(),
            "encryption": "enabled" if self.encryption else "disabled",
            "dedup_cache_size": self.router.dedup.size,
            "auto_steer": self.auto_steer.get_stats(),
            "local_store": self.local_store.get_stats(),
            "file_transfer": self.file_transfer.get_transfer_stats(),
            "peer_discovery": self.peer_discovery.get_stats(),
            "coordinator": self.election.get_status() if self.election else None,
            "topology_tuner": self.topology_tuner.stats if hasattr(self, 'topology_tuner') else None,
        }
        if self.mesh_address:
            status["address"] = f"0x{self.mesh_address.short:04X}"
            status["depth"] = self.mesh_address.depth
        return status

    # ─── Health Monitoring Loop ──────────────────────────────────────

    async def _health_monitor_loop(self):
        """Periodic health check: verify PG connection, transports, and peer connectivity."""
        while self._running:
            try:
                await asyncio.sleep(30)  # Check every 30s
                if not self._running:
                    break

                # Check PG pool connection
                if self._pg_pool and not self._pg_pool.is_connected():
                    log.warning("PG pool connection lost — attempting reconnect")
                    try:
                        if await self._pg_pool.connect():
                            log.info("PG pool connection restored")
                    except Exception as e:
                        log.error(f"PG pool reconnect failed: {e}")

                # Check transport availability
                for name, transport in self.router.transports.items():
                    if not transport.is_available():
                        log.debug(f"Transport {name} unavailable")
                        # Re-check HTTP transport if it's marked unavailable
                        if name == 'http' and hasattr(transport, 'health_check'):
                            try:
                                await transport.health_check()
                            except Exception:
                                pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Health monitor error: {e}")

    # ─── Stats Update Loop ───────────────────────────────────────────

    async def _stats_update_loop(self):
        """Periodically update node stats in PG (messages sent/received, uptime)."""
        while self._running:
            try:
                await asyncio.sleep(60)  # Update every 60s
                if not self._running:
                    break
                await self._update_node_stats()
                # Cleanup old steer directives
                self.auto_steer.cleanup_old_steers(max_age_seconds=3600)
                # Cleanup old outbound messages from local_store (pg_synced > 1h old)
                self.local_store.cleanup_outbound(max_age_hours=1)
                # Cleanup old mesh_messages (retention: 7 days)
                await self._cleanup_old_messages(max_age_days=7)
                # Cleanup old debug logs (retention: 7 days)
                await self._cleanup_debug_logs(max_age_hours=168)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"Stats update error: {e}")

    async def _auto_update_loop(self, check_interval: int = 300):
        """Periodically check Gitea for new versions and auto-update if configured."""
        log.info(f"🔄 Auto-update loop starting (interval={check_interval}s, enabled=True)")
        # Wait a bit after startup before first check
        await asyncio.sleep(60)
        while self._running:
            try:
                await asyncio.sleep(check_interval)
                if not self._running:
                    break

                auto_update_cfg = getattr(self.config, 'auto_update', None)
                if not auto_update_cfg or not getattr(auto_update_cfg, 'enabled', False):
                    break

                from core.auto_updater import AutoUpdater
                updater = AutoUpdater(node=self)
                try:
                    # Update state: checking
                    self._updater_state["state"] = "checking"
                    self._updater_state["last_check"] = time.time()
                    
                    latest_tag = await updater.check_for_update()
                    current = updater.current_version
                    self._updater_state["current_version"] = current
                    
                    if latest_tag:
                        apply_auto = getattr(auto_update_cfg, 'apply_automatically', False)
                        if apply_auto:
                            log.info(f"🔄 Auto-update: {current} → {latest_tag.lstrip('v')}, applying...")
                            self._updater_state["state"] = "updating"
                            result = await updater.apply_update(latest_tag)
                            if result.success:
                                log.info(f"✅ Auto-update successful: {result.previous_version} → {result.new_version}")
                                self._updater_state["state"] = "updated"
                                self._updater_state["last_update"] = time.time()
                            else:
                                log.error(f"❌ Auto-update failed: {result.error}")
                                self._updater_state["state"] = "failed"
                        else:
                            log.info(f"🆕 Update available: {current} → {latest_tag.lstrip('v')} (auto-apply disabled)")
                            self._updater_state["state"] = "update_available"
                    else:
                        self._updater_state["state"] = "idle"
                finally:
                    await updater.close()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"Auto-update error: {e}")
                self._updater_state["state"] = "error"

    # ── Debug Logging ────────────────────────────────────────────

    async def debug_log(self, level: str, category: str, message: str, metadata: dict = None):
        """Log a debug message to the mesh_debug_logs table (shared across agents).

        Levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
        Categories: startup, shutdown, transport, election, delegation, health, general
        """
        if not self._pg_pool or not self._pg_pool.is_connected():
            log.warning(f"Debug log skipped (DB not connected): [{level}] {category}: {message}")
            return
        try:
            import json
            await self._pg_pool.execute(
                "INSERT INTO mesh.mesh_debug_logs (source_node, log_level, category, message, metadata) "
                "VALUES ($1, $2, $3, $4, $5)",
                self.node_name, level.upper(), category, message,
                json.dumps(metadata or {})
            )
        except Exception as e:
            log.warning(f"Debug log write failed: {e}")

    async def _cleanup_debug_logs(self, max_age_hours: int = 168):
        """Remove debug logs older than max_age_hours (default: 7 days)."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            return
        try:
            result = await self._pg_pool.execute(
                "DELETE FROM mesh.mesh_debug_logs WHERE created_at < NOW() - ($1 || ' hours')::INTERVAL",
                str(max_age_hours)
            )
            deleted = int(result.split()[-1]) if result else 0
            if deleted > 0:
                log.info(f"Debug log cleanup: removed {deleted} entries older than {max_age_hours}h")
        except Exception as e:
            log.warning(f"Debug log cleanup error: {e}")

    async def _update_node_stats(self):
        """Update node statistics in the mesh_nodes and mesh_node_health tables."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            log.warning(f"Stats update skipped: pg_pool={'None' if not self._pg_pool else 'not connected'}")
            return
        try:
            # Collect health metrics first (before DB ops)
            cpu_pct = 0.0
            memory_pct = 0.0
            disk_pct = 0.0
            try:
                import psutil
                cpu_pct = psutil.cpu_percent(interval=0.1)
                memory_pct = psutil.virtual_memory().percent
                disk_pct = psutil.disk_usage('/').percent
            except ImportError:
                try:
                    import os, multiprocessing
                    load1, _, _ = os.getloadavg()
                    cpu_count = multiprocessing.cpu_count() or 1
                    cpu_pct = min((load1 / cpu_count) * 100, 100.0)
                    memory_pct = 50.0
                    disk_pct = 50.0
                except Exception:
                    pass
            
            host_ip = self._get_local_ip()
            
            # UPSERT into mesh_nodes — only UPDATE if exists (INSERT requires short_addr etc.)
            try:
                await self._pg_pool.execute("""
                    UPDATE mesh.mesh_nodes 
                    SET last_heartbeat = NOW(),
                        status = 'active',
                        host = $1
                    WHERE node_name = $2
                """, host_ip, self.node_name)
            except Exception as node_err:
                log.debug(f"mesh_nodes update failed (non-critical): {node_err}")
            
            # UPSERT into mesh_node_health
            await self._pg_pool.execute("""
                INSERT INTO mesh_node_health (node_name, status, cpu_pct, memory_pct, disk_pct, last_seen, updated_at)
                VALUES ($1, 'active', $2, $3, $4, NOW(), NOW())
                ON CONFLICT (node_name) DO UPDATE SET
                    status = 'active',
                    cpu_pct = EXCLUDED.cpu_pct,
                    memory_pct = EXCLUDED.memory_pct,
                    disk_pct = EXCLUDED.disk_pct,
                    last_seen = NOW(),
                    updated_at = NOW()
            """, self.node_name, cpu_pct, memory_pct, disk_pct)
            log.info(f"Stats updated: {self.node_name} cpu={cpu_pct:.1f}% mem={memory_pct:.1f}% disk={disk_pct:.1f}%")
            
        except Exception as e:
            log.warning(f"Stats update failed: {e}")

    async def _cleanup_old_messages(self, max_age_days: int = 7):
        """Delete old mesh_messages and vacuum to reclaim space.
        Runs every 60s from _stats_update_loop. Only coordinator runs cleanup
        to avoid race conditions.
        """
        # Only coordinator should run DB cleanup to avoid races
        if self.role != "coordinator":
            return
        try:
            conn = self._pg_transport._write_conn or self._pg_transport._conn
            if not conn or conn.closed:
                return
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM mesh.mesh_messages WHERE created_at < now() - interval '%s days'" % max_age_days
            )
            deleted = cur.rowcount
            conn.commit()
            if deleted > 0:
                log.info(f"Message cleanup: deleted {deleted} messages older than {max_age_days} days")
                # Vacuum to reclaim space (cannot run inside transaction)
                old_autocommit = conn.autocommit
                conn.autocommit = True
                try:
                    cur2 = conn.cursor()
                    cur2.execute("VACUUM mesh.mesh_messages")
                    cur2.close()
                except Exception:
                    pass  # VACUUM is best-effort
                finally:
                    conn.autocommit = old_autocommit
            cur.close()
        except Exception as e:
            log.debug(f"Message cleanup failed: {e}")

    # ─── Webhook Trigger ─────────────────────────────────────────────

    async def _trigger_webhook(self, message: A2AMessage):
        """Wake the local Hermes agent via the webhook URL or dashboard wake-agent API.
        
        Tries webhook URL first (Hermes webhook), then health_port dashboard API.
        The webhook triggers `hermes -z` which wakes the agent to process the message.
        """
        # Determine the correct URL for waking the agent
        # Priority: webhook_port (Hermes webhook) > health_port (dashboard API) > HTTP transport
        wake_url = None
        webhook_url = None
        
        # Try webhook_port first (direct Hermes webhook on localhost)
        if self.config.webhook_port:
            webhook_url = f"http://localhost:{self.config.webhook_port}/webhooks/a2a-instant"
        
        # Dashboard wake-agent API on health_port
        dashboard_url = None
        if self.config.health_port and self.config.health_port > 0:
            dashboard_url = f"http://localhost:{self.config.health_port}/api/wake-agent"
        
        # Try dashboard wake-agent API first (most reliable), then webhook
        wake_url = dashboard_url or webhook_url
        
        if not wake_url:
            log.debug("No URL available for wake-agent")
            return
        
        # Prepare webhook secret for signature
        webhook_secret = getattr(self.config, 'webhook_secret', None) or os.environ.get('WEBHOOK_SECRET', '')
        
        try:
            import aiohttp
            payload = {
                "message_id": message.id,
                "sender": message.sender,
                "recipient": message.recipient,
                "type": message.type,
                "priority": message.priority,
                "content": message.payload if isinstance(message.payload, str) else str(message.payload),
            }
            # Add reply_endpoint for dashboard API
            if dashboard_url:
                payload["reply_endpoint"] = dashboard_url.replace("/api/wake-agent", "/api/agent-reply")
            
            # Add mesh_secret for dashboard wake-agent API auth
            if wake_url == dashboard_url:
                payload["mesh_secret"] = "mesh-wake-secret-2026"
                # Build prompt from message content for wake-agent
                prompt_text = f"[A2A Message from {message.sender}] {payload['content']}"
                payload["prompt"] = prompt_text
                payload["agent_name"] = self.node_name
            
            headers = {}
            if webhook_secret and webhook_url and wake_url == webhook_url:
                # Sign payload with webhook secret for Hermes webhook
                import hmac, hashlib
                payload_json = json.dumps(payload, sort_keys=True)
                signature = hmac.new(webhook_secret.encode(), payload_json.encode(), hashlib.sha256).hexdigest()
                headers["X-Hermes-Signature"] = f"sha256={signature}"
                headers["Content-Type"] = "application/json"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(wake_url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        log.info(f"Wake-agent triggered for message {message.id[:8]} from {message.sender} via {wake_url}")
                        return  # Success, no need for fallback
                    else:
                        body = await resp.text()
                        # Auth errors (401/403) mean the endpoint exists but rejects us — don't fallback to webhook
                        if resp.status in (401, 403):
                            log.warning(f"Wake-agent auth error {resp.status} from {wake_url}: check mesh_secret config")
                            return  # Don't fallback — auth issue won't be solved by trying another endpoint
                        log.warning(f"Wake-agent response {resp.status} from {wake_url}: {body[:200]}")
        except aiohttp.ClientError as e:
            log.debug(f"Wake-agent network error via {wake_url}: {e}")
            # Network error — try fallback URL
            fallback = dashboard_url if wake_url == webhook_url else webhook_url
            if fallback and fallback != wake_url:
                try:
                    import aiohttp as _aiohttp_fallback
                    async with _aiohttp_fallback.ClientSession() as session:
                        async with session.post(fallback, json=payload, timeout=_aiohttp_fallback.ClientTimeout(total=5)) as resp:
                            if resp.status == 200:
                                log.info(f"Wake-agent triggered via fallback {fallback}")
                            else:
                                log.warning(f"Wake-agent fallback {fallback} returned {resp.status}")
                except Exception as e2:
                    log.debug(f"Wake-agent fallback {fallback} also failed: {e2}")
        except Exception as e:
            log.debug(f"Wake-agent via {wake_url} failed: {e}")


async def main():
    """CLI entry point for mesh node."""
    import argparse

    parser = argparse.ArgumentParser(description="A2A Mesh Node")
    parser.add_argument("--config", "-c", default="~/.hermes/mesh_config.yaml",
                        help="Path to config file")
    parser.add_argument("--name", "-n", default=os.environ.get("A2A_NODE_NAME", "nova"),
                        help="Node name")
    parser.add_argument("--port", "-p", type=int, default=8645,
                        help="P2P listen port")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose logging")
    args = parser.parse_args()

    # Load config
    config_path = os.path.expanduser(args.config)
    if os.path.exists(config_path):
        config = MeshConfig.from_yaml(config_path)
    else:
        config = MeshConfig()

    config.node_name = args.name
    config.p2p.listen_port = args.port
    # Ensure health_port != P2P port (convention: health_port = p2p_port + 5)
    if config.health_port == config.p2p.listen_port:
        config.health_port = config.p2p.listen_port + 5
        log.info(f"Health port synced to p2p_port+5: {config.health_port}")

    if args.verbose:
        logging.getLogger("a2a_mesh").setLevel(logging.DEBUG)

    # Create and start node
    node = MeshNode(config)
    node.add_handler(lambda msg: print(f"📨 {msg.sender} → {msg.recipient}: {msg.type}"))

    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def _signal_handler():
        log.info("Received shutdown signal, stopping gracefully...")
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        if await node.start():
            print(f"🟢 Mesh node '{args.name}' started")
            print(f"   Role: {node.role.value}")
            print(f"   Address: 0x{node.mesh_address.short:04X}" if node.mesh_address else "   Address: pending")
            print(f"   PG: {'✅' if node._pg_transport.is_available() else '❌'}")
            print(f"   P2P: {'✅' if node._p2p_transport.is_available() else '❌'}")
            print(f"   HTTP: {'✅' if node._http_transport.is_available() else '❌'}")

            # Wait for shutdown signal
            await shutdown_event.wait()
            log.info("Shutdown signal received, stopping node...")
        else:
            print("🔴 Failed to start mesh node")
            sys.exit(1)
    finally:
        await node.stop()
        log.info("Mesh node stopped completely")


if __name__ == "__main__":
    asyncio.run(main())