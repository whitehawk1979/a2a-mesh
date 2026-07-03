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
            self.tree_router = TreeRouter(self.mesh_address, self.address_manager)
            log.info(f"Coordinator mode: address={self.mesh_address}")
        elif self.role == NodeRole.ROUTER:
            # Router joins network, gets address from coordinator
            # If coordinator not reachable, assign self as first router (0x0001)
            self.address_manager = AddressManager(
                max_children=topo.max_children,
                max_routers=topo.max_routers,
                max_depth=topo.max_depth,
            )
            self.mesh_address = self.address_manager.assign_address(
                self.node_name, NodeRole.ROUTER
            )
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
        self._pg_conn = None  # Direct PG connection for writes

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
            # Sync skills & capabilities to DB
            # Convert dict skills to string IDs for SQL_ASCII compatibility
            try:
                if self._pg_conn:
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
                    cur = self._pg_conn.cursor()
                    cur.execute(
                        "UPDATE mesh.mesh_nodes SET skills = %s, capabilities = %s WHERE node_name = %s",
                        (json.dumps(db_skills, ensure_ascii=True), json.dumps(db_caps, ensure_ascii=True), peer_name)
                    )
                    self._pg_conn.commit()
                    cur.close()
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
            log.info("✅ P2P callbacks registered (ACK + peer_connected)")
        else:
            log.warning("❌ P2P TCP transport failed")

        # 2. PG NOTIFY (optional fallback — gracefully degrades if unavailable)
        results["pg_notify"] = await self._pg_transport.start()
        if results["pg_notify"]:
            log.info("✅ PG NOTIFY transport started (fallback)")
        else:
            log.warning("⚠️ PG NOTIFY transport unavailable — running in P2P-only mode")

        # 3. HTTP/MCP (tertiary)
        results["http"] = await self._http_transport.start()
        if results["http"]:
            log.info("✅ HTTP/MCP transport started")
        else:
            log.warning("❌ HTTP/MCP transport failed")

        # Start BLE transport
        results["ble"] = await self._ble_transport.start()
        if results["ble"]:
            log.info("✅ BLE transport started")
        else:
            log.warning("❌ BLE transport failed (non-critical)")

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

        # Start priority queue processor
        self.router.start_priority_queue()

        # Start peer discovery (link P2P transport and PG conn for auto-connect)
        self.peer_discovery.p2p_transport = self._p2p_transport
        self.peer_discovery._pg_conn = self._pg_conn
        # P2: Wire up peer address resolver — P2P transport can now dynamically
        # connect to peers it hasn't connected to yet, using peer_discovery data
        self._p2p_transport._peer_address_resolver = self.peer_discovery.resolve_peer_address
        self.memory_sync.set_pg_conn(self._pg_conn)
        await self.peer_discovery.start()

        # Start ACK manager
        await self.ack_manager.start()

        # Ensure offline queue table exists
        self.offline_queue.ensure_table()

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

        # At least one transport must be working
        any_ok = any(results.values())
        if any_ok:
            log.info(f"Mesh node '{self.node_name}' started ({sum(results.values())}/3 transports)")
        else:
            log.error("All transports failed!")

        return any_ok

    async def stop(self):
        """Stop all transports and discovery. Deregister from mesh."""
        if not self._running:
            return  # Already stopped, avoid double-stop
        log.info(f"Stopping mesh node '{self.node_name}'")
        self._running = False

        # Stop plugins first (they may need mesh to send final messages)
        try:
            await self.plugin_loader.stop_all()
            log.info("All plugins stopped")
        except Exception as e:
            log.warning(f"Plugin shutdown error (non-fatal): {e}")

        # Stop ACK manager
        await self.ack_manager.stop()

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

        # Close PG write connection
        if self._pg_conn:
            try:
                self._pg_conn.close()
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
            health_port=port + 5,  # Convention: health_port = p2p_port + 5
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

                # Small delay to avoid overwhelming the receiver
                await asyncio.sleep(0.01)

                # Update transfer state
                transfer["current_chunk"] = chunk_index + 1

            except Exception as e:
                log.error(f"Error sending chunk {chunk_index} for {file_id}: {e}")

        # Send FILE_COMPLETE
        complete_msg = self.file_transfer.create_complete_message(file_id, recipient)
        result = await self.send(complete_msg)
        log.info(f"FILE_COMPLETE sent for {file_id} → {recipient} (result: {result.success})")

    async def _on_p2p_ack(self, ack_for_id: str, ack_type: str):
        """Callback when a P2P ACK is received — update message status in PG."""
        if not self._pg_conn:
            return
        try:
            cur = self._pg_conn.cursor()
            cur.execute("""
                UPDATE mesh.mesh_messages SET status = 'acknowledged'
                WHERE id = %s
            """, (ack_for_id,))
            self._pg_conn.commit()
            cur.close()
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
            peer.p2p_available = True
            self.peer_discovery._register_discovered_peer(peer)
        else:
            log.warning(f"P2P peer_connected callback: peer {peer_name} not found in discovery, skipping registry")

        # Notify plugins about peer connection
        if hasattr(self, 'plugin_loader') and self.plugin_loader.plugins:
            asyncio.create_task(self.plugin_loader.dispatch_peer_connected(peer_name, peer or {}))

        # P2P Skill Auto-Discovery: send our skills to the newly connected peer
        # Try P2P first, fall back to PG NOTIFY (guaranteed delivery)
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

    async def _on_peer_discovered(self, peer_name: str):
        """Callback when a new peer is discovered (via PG or static config).
        Sends our skills announcement via PG broadcast so all nodes receive it."""
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
            version=getattr(self.config, 'version', '1.0.0'),
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
        Other nodes listening on mesh_node_update channel will discover this node immediately."""
        import json
        try:
            if not self._pg_conn:
                return
            payload = json.dumps({
                "node": self.node_name,
                "action": action,
                "endpoint": f"{self.config.p2p.listen_host}:{self.config.p2p.listen_port}",
                "capabilities": list(set(c for c in (getattr(self.config, 'capabilities', []) or ["a2a_messaging"]) if isinstance(c, (str, int, float, tuple)))),
            })
            cur = self._pg_conn.cursor()
            cur.execute(f"NOTIFY mesh_node_update, '{payload}'")
            self._pg_conn.commit()
            cur.close()
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
            uptime = time.time() - self._start_time if self._start_time else 0
            status = {
                "status": "running" if self._running else "stopped",
                "node": self.node_name,
                "role": self.role.value,
                "address": f"0x{self.mesh_address.short:04X}" if self.mesh_address else "pending",
                "uptime_seconds": round(uptime, 1),
                "transports": {
                    "pg": self._pg_transport.is_available(),
                    "p2p": self._p2p_transport.is_available(),
                    "http": self._http_transport.is_available(),
                    "ble": self._ble_transport.is_available(),
                },
                "election": self.election.get_status() if self.election else {},
                "ack": self.ack_manager.get_stats(),
                "offline_queue": self.offline_queue.get_stats(),
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
            if "address already in use" in str(e).lower() or getattr(e, 'errno', None) in (48, 98):
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
        """Initialize a direct PG connection for writes (INSERT into mesh_messages)."""
        try:
            import psycopg2
            self._pg_conn = psycopg2.connect(
                host=self.config.pg.host,
                port=self.config.pg.port,
                dbname=self.config.pg.dbname,
                user=self.config.pg.user,
                password=self.config.pg.password,
            )
            self._pg_conn.autocommit = True
            # Fix: Set client_encoding to match SQL_ASCII database encoding
            self._pg_conn.set_client_encoding("SQL_ASCII")
            log.info("PG write connection established")
            return True
        except Exception as e:
            log.error(f"PG write connection failed: {e}")
            return False

    async def _persist_message(self, message: A2AMessage):
        """Persist message to mesh.mesh_messages for reliability and NOTIFY trigger."""
        if not self._pg_conn:
            return

        try:
            cur = self._pg_conn.cursor()
            cur.execute("""
                INSERT INTO mesh.mesh_messages 
                    (id, sender, recipient, msg_type, priority, payload, 
                     routing_mode, src_addr, dst_addr, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'sent')
            """, (
                message.id,
                message.sender,
                message.recipient,
                message.type,
                getattr(message, 'priority', 5),
                json.dumps(message.payload, default=str),
                getattr(message, 'routing_mode', 'hybrid'),
                self.mesh_address.short if self.mesh_address else None,
                None,  # dst_addr resolved later
            ))
            cur.close()
        except Exception as e:
            log.error(f"Failed to persist message {message.id[:8]}: {e}")

    async def _register_node(self):
        """Register this node in mesh.mesh_nodes with network info.
        
        Retries up to 3 times if PG connection is not available yet.
        """
        max_retries = 3
        for attempt in range(max_retries):
            if self._pg_conn:
                break
            log.warning(f"_register_node: PG connection not available (attempt {attempt+1}/{max_retries}), retrying in 2s...")
            await asyncio.sleep(2)
        
        if not self._pg_conn:
            log.error("_register_node: PG connection not available after retries, skipping registration")
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
        import json
        try:
            host_ip = self._get_local_ip()
        except Exception:
            host_ip = "0.0.0.0"

        # Get port config — P2P port from transport config, health port from node config
        p2p_port = self.config.p2p.listen_port
        health_port = getattr(self.config, 'health_port', 8650)

        # Coordinator auto-approves itself; other nodes start as 'pending'
        initial_status = 'active' if self.role == NodeRole.COORDINATOR else 'pending'

        try:
            cur = self._pg_conn.cursor()
            cur.execute("""
                INSERT INTO mesh.mesh_nodes 
                    (node_name, role, short_addr, extended_uuid, parent_addr, depth, 
                     status, last_heartbeat, host, p2p_port, health_port,
                     pg_available, p2p_available, http_available, capabilities)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s)
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
                    capabilities = EXCLUDED.capabilities
            """, (
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
                bool(self._pg_conn),
                self._p2p_transport.is_available() if hasattr(self, "_p2p_transport") else False,
                self._http_transport.is_available() if hasattr(self, "_http_transport") else False,
                json.dumps(capabilities, ensure_ascii=True),
            ))
            cur.close()
            log.info(f"Registered node {self.node_name} at {host_ip}:{p2p_port} in mesh")
        except Exception as e:
            # Handle short_addr unique constraint violation:
            # Another node may hold our short_addr from a previous session.
            # Clear stale entries and retry once.
            if 'short_addr' in str(e) and 'duplicate' in str(e).lower():
                log.warning(f"short_addr conflict for {self.node_name}: {e} — clearing stale entry and retrying")
                try:
                    cur = self._pg_conn.cursor()
                    # Remove any stale node claiming our short_addr (but not our own row)
                    if self.mesh_address:
                        cur.execute("""
                            DELETE FROM mesh.mesh_nodes
                            WHERE short_addr = %s AND node_name != %s
                        """, (self.mesh_address.short, self.node_name))
                        log.info(f"Cleared {cur.rowcount} stale short_addr={self.mesh_address.short} entries")
                    # Also remove our own stale row if it exists with a different short_addr
                    cur.execute("""
                        DELETE FROM mesh.mesh_nodes
                        WHERE node_name = %s AND short_addr != %s
                    """, (self.node_name, self.mesh_address.short if self.mesh_address else 0))
                    self._pg_conn.commit()
                    cur.close()
                    # Retry registration
                    cur = self._pg_conn.cursor()
                    cur.execute("""
                        INSERT INTO mesh.mesh_nodes 
                            (node_name, role, short_addr, extended_uuid, parent_addr, depth, 
                             status, last_heartbeat, host, p2p_port, health_port,
                             pg_available, p2p_available, http_available, capabilities)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s)
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
                            capabilities = EXCLUDED.capabilities
                    """, (
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
                        bool(self._pg_conn),
                        self._p2p_transport.is_available() if hasattr(self, "_p2p_transport") else False,
                        self._http_transport.is_available() if hasattr(self, "_http_transport") else False,
                        json.dumps(capabilities, ensure_ascii=True),
                    ))
                    cur.close()
                    log.info(f"Registered node {self.node_name} at {host_ip}:{p2p_port} (retry succeeded)")
                except Exception as retry_e:
                    log.error(f"Failed to register node after retry: {retry_e}")
            else:
                log.error(f"Failed to register node: {e}")

    async def _deregister_node(self):
        """Mark this node as offline in mesh.mesh_nodes."""
        if not self._pg_conn:
            return

        try:
            cur = self._pg_conn.cursor()
            cur.execute("""
                UPDATE mesh.mesh_nodes SET status = 'offline', last_heartbeat = NOW()
                WHERE node_name = %s
            """, (self.node_name,))
            cur.close()
            log.info(f"Deregistered node {self.node_name}")
        except Exception as e:
            log.error(f"Failed to deregister node: {e}")

    async def _update_heartbeat_pg(self):
        """Update heartbeat timestamp in PG."""
        if not self._pg_conn:
            return

        try:
            cur = self._pg_conn.cursor()
            cur.execute("""
                UPDATE mesh.mesh_nodes SET 
                    last_heartbeat = NOW(), 
                    status = 'active',
                    pg_available = %s,
                    p2p_available = %s,
                    http_available = %s
                WHERE node_name = %s
            """, (
                bool(self._pg_conn),
                self._p2p_transport.is_available() if hasattr(self, "_p2p_transport") else False,
                self._http_transport.is_available() if hasattr(self, "_http_transport") else False,
                self.node_name,
            ))
            cur.close()
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
                                    else:
                                        # P1-6: queued backlog processing
                                        await self.router.enqueue(msg)

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
        if not self._pg_conn:
            return []

        try:
            cur = self._pg_conn.cursor()
            cur.execute("""
                SELECT node_name, short_addr FROM mesh.mesh_nodes 
                WHERE role = 'router' AND status = 'active'
                ORDER BY short_addr
            """)
            rows = cur.fetchall()
            cur.close()
            return [(name, addr) for name, addr in rows]
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

                # Check PG connection
                if self._pg_conn and self._pg_conn.closed:
                    log.warning("PG connection lost — attempting reconnect")
                    try:
                        self._pg_conn = await asyncio.get_event_loop().run_in_executor(
                            None, self._pg_transport._connect
                        )
                        if self._pg_conn and not self._pg_conn.closed:
                            log.info("PG connection restored")
                    except Exception as e:
                        log.error(f"PG reconnect failed: {e}")

                # Check transport availability
                for name, transport in self.router.transports.items():
                    if not transport.is_available():
                        log.debug(f"Transport {name} unavailable")

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
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"Stats update error: {e}")

    async def _update_node_stats(self):
        """Update node statistics in the mesh_nodes table."""
        if not self._pg_transport or not self._pg_transport._available:
            return
        try:
            conn = self._pg_transport._write_conn or self._pg_transport._conn
            if not conn or conn.closed:
                return
            cur = conn.cursor()
            stats = self.router._stats.copy()
            cur.execute("""
                UPDATE mesh.mesh_nodes 
                SET last_heartbeat = NOW(),
                    status = 'active'
                WHERE node_name = %s
            """, (self.node_name,))
            conn.commit()
            cur.close()
        except Exception as e:
            log.debug(f"Stats update failed: {e}")

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