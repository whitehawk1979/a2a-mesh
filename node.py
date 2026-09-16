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
import resource
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
from .core.diagnostics import DiagnosticEngine
from .core.local_store import LocalStore
from .core.file_transfer import P2PFileTransfer, FILE_OFFER, FILE_ACCEPT, FILE_REJECT, FILE_CHUNK, FILE_COMPLETE, FILE_ACK
from .core.peer_discovery import PeerDiscovery
from .core.memory_sync import MemorySync
from .core.dashboard import DashboardHandler
from .transports.pg_transport import PGTransport
from .transports.p2p_transport import P2PTransport
from .transports.http_transport import HTTPTransport
from .transports.ble_transport import BLETransport
from .transports.ssh_tunnel_transport import SSHTunnelTransport
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
        # Also store on config so P2P transport can include it in heartbeat payload
        self.config._resolved_version = self._resolved_version

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
        self.delegation.router = self.router  # Wire router for A2A message sending

        # Initialize message router (Marveen-inspired: tracing + backlog batching)
        from .core.message_router import create_message, process_backlog, get_router_status
        self._msg_router_create = create_message
        self._msg_router_process = process_backlog
        self._msg_router_status = get_router_status

        # Initialize Marveen DB (audit trail, kanban comments, daily logs)
        from .core.marveen_db import set_pg_pool as set_marveen_db_pool
        self._set_marveen_db_pool = set_marveen_db_pool

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

        # Initialize SSH key auto-sync (approved peers exchange pubkeys,
        # enabling bidirectional SSH tunnels without manual key copies)
        from .core.ssh_key_sync import SSHKeySync
        ssh_cfg = getattr(self.config, 'ssh_tunnel', None) or getattr(getattr(self.config, 'transports', None), 'ssh_tunnel', None)
        self.ssh_key_sync = SSHKeySync(
            node_name=self.node_name,
            registry=self.dashboard.registry,
            router=self.router,
            pg_pool=None,  # injected after PG pool connect
            identity_files=(list(ssh_cfg.identity_files) if ssh_cfg and getattr(ssh_cfg, 'identity_files', None) else None),
            advertised_ssh_port=int(getattr(self.config, 'advertised_ssh_port', 0) or 0),
            node_config=self.config,
        )
        self.ssh_key_sync.set_node_ref(self)
        # Embedded sshd manager: guarantees a reachable sshd for INBOUND
        # tunnels on every node (installs openssh in containers, self-heals).
        from .core.ssh_server import detect_environment, start_embedded_sshd
        self._env_kind = detect_environment()
        sshd_enabled = bool(getattr(ssh_cfg, 'embedded_sshd', False)) if ssh_cfg else False
        # Auto-enable inside containers/HAOS — no system sshd there
        if not sshd_enabled and self._env_kind in ('docker', 'haos'):
            sshd_enabled = True
        self.embedded_sshd = None
        if sshd_enabled:
            sshd_cfg = {
                'sshd_port': int(getattr(ssh_cfg, 'sshd_port', 2230) or 2230) if ssh_cfg else 2230,
                'sshd_bind': getattr(ssh_cfg, 'sshd_bind', '0.0.0.0') if ssh_cfg else '0.0.0.0',
                'sshd_config_dir': getattr(ssh_cfg, 'sshd_config_dir', '') if ssh_cfg else '',
            }
            self.embedded_sshd = start_embedded_sshd(self.node_name, sshd_cfg)
            if self.embedded_sshd:
                log.info(f"Embedded sshd active on :{self.embedded_sshd.port} (env={self._env_kind})")
                # Peer keys land in the sshd's PERSISTENT authorized_keys
                # (containers wipe /root/.ssh — /config/.ssh survives).
                try:
                    self.ssh_key_sync._ak_path = self.embedded_sshd._authorized_keys
                except Exception:
                    pass
        # Set callback for peer discovery → triggers skills announcement via PG broadcast
        self.peer_discovery._on_peer_discovered = self._on_peer_discovered

        # Initialize plugin loader
        self.plugin_loader = PluginLoader(self)

        # Initialize topology tuner (health-score-based auto-tuning)
        self.topology_tuner = TopologyTuner(self, config=self.config)
        # Initialize diagnostic engine
        self.diagnostics = DiagnosticEngine(self)
        # Debounce peer offline broadcasts — prevent broadcast storms during P2P flapping
        self._peer_offline_debounce: dict[str, float] = {}  # peer_name -> last_broadcast_time
        # Grace period tasks — delayed offline broadcasts cancelled on reconnect
        self._peer_offline_grace_tasks: dict[str, asyncio.Task] = {}  # peer_name -> pending broadcast task
        self._peer_offline_grace_seconds: int = 30  # wait before declaring peer offline
        # Track which peers we've broadcast as offline — so we can send peer_online on reconnect
        self._peer_offline_broadcasted: set[str] = set()  # peer_names currently believed offline by mesh
        self._peer_online_debounce: dict[str, float] = {}  # peer_name -> last peer_online broadcast time

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

        # Initialize transports (shared PG pool injected after _init_pg_write_conn)
        self._pg_transport = PGTransport(self.config)
        self._p2p_transport = P2PTransport(self.config, node_version=self._resolved_version)
        self._http_transport = HTTPTransport(self.config)
        self._ble_transport = BLETransport(self.config)
        self._ssh_tunnel_transport = SSHTunnelTransport(
            self.config.ssh_tunnel,
            node_name=self.node_name,
            node_version=self._resolved_version,
            peer_discovery=getattr(self, '_discovery', None),
            peer_connected_callback=self._on_transport_peer_connected,
            mesh_config=self.config,
        )

        # Register transports with router
        self.router.register_transport("pg_notify", self._pg_transport)

        # Multi-hop relay: give the router our tree topology (parent lookup)
        if getattr(self, 'tree_router', None):
            self.router.set_tree_router(self.tree_router)
        self.router.register_transport("p2p", self._p2p_transport)
        self.router.register_transport("http", self._http_transport)
        self.router.register_transport("ble", self._ble_transport)
        if self.config.ssh_tunnel.enabled:
            self.router.register_transport("ssh_tunnel", self._ssh_tunnel_transport)

        # Initialize discovery
        self._discovery = MeshDiscovery(
            node_name=self.node_name,
            port=self.config.p2p.listen_port,
            version=self._resolved_version,
        )

        # Message handlers
        self._handlers: List[Callable] = []

        # State
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._start_time = 0
        self._pg_pool: Optional[AsyncDBPool] = None  # asyncpg connection pool for all DB ops
        self._cpu_baseline_initialized: bool = False  # Track if psutil CPU baseline is ready
        self._cpu_ema: float = 0.0  # Exponential Moving Average for CPU (alpha=0.3)
        self._cpu_ema_initialized: bool = False

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure structured JSON logging to file and human-readable to console."""
        from core.json_logger import setup_json_logging
        import os
        
        log_file = self.config.log_file
        if log_file:
            log_file = os.path.expanduser(log_file)
        log_dir = os.path.dirname(log_file) if log_file else ''
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        
        # Use structured JSON logging: JSON to file, human-readable to console
        setup_json_logging(
            node_name=self.config.node_name,
            log_file=log_file,
            json_mode='auto',  # JSON to file, human to console
        )
        log.info(f"📝 Structured logging initialized (JSON→file, human→console, node={self.config.node_name})")

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

        # Handle diagnostic messages (diagnostic_report, config_suggestion)
        if message.type in ("diagnostic_report", "config_suggestion"):
            log.debug(f"🔍 Handling diagnostic message: type={message.type} sender={message.sender} payload_keys={list(message.payload.keys()) if isinstance(message.payload, dict) else 'str'}")
            try:
                payload = message.payload if isinstance(message.payload, dict) else {}
                if isinstance(message.payload, str):
                    import json as _json
                    payload = _json.loads(message.payload)
                await self.diagnostics.handle_diagnostic_message(payload)
            except Exception as e:
                log.warning(f"Failed to handle diagnostic message: {e}")
            return

        # Handle SSH key sync — automatic pubkey exchange between approved peers
        if message.type == "ssh_key_sync":
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}
            try:
                if getattr(self, 'ssh_key_sync', None):
                    await self.ssh_key_sync.handle_incoming(payload, message.sender)
                else:
                    log.warning("ssh_key_sync message received but module not initialized")
            except Exception as e:
                log.warning(f"SSH key sync handling failed: {e}")
            return

        # v2: coordinator-aggregated key bundle — apply all peers' keys + tunnels
        if message.type == "ssh_key_bundle":
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}
            try:
                if getattr(self, 'ssh_key_sync', None):
                    await self.ssh_key_sync.handle_bundle(payload, message.sender)
                else:
                    log.warning("ssh_key_bundle message received but module not initialized")
            except Exception as e:
                log.warning(f"SSH key bundle handling failed: {e}")
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
            peer_version = payload.get("version") or None
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

        # Handle vault share protocol — per-agent vault access + cross-node secret sharing
        if message.type in ("vault_request", "vault_share", "vault_response"):
            from .core.vault_share import (
                handle_vault_request, handle_vault_response, handle_vault_share,
            )
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}
            try:
                if message.type == "vault_request":
                    resp_payload = handle_vault_request(payload)
                    if isinstance(resp_payload, dict):
                        resp_payload.setdefault("node", self.node_name)
                    resp = A2AMessage.create(
                        sender=self.node_name,
                        recipient=message.sender,
                        msg_type="vault_response",
                        payload=resp_payload,
                    )
                    asyncio.create_task(self.router.send(resp))
                elif message.type == "vault_share":
                    resp_payload = handle_vault_share(payload)
                    resp = A2AMessage.create(
                        sender=self.node_name,
                        recipient=message.sender,
                        msg_type="vault_response",
                        payload=resp_payload,
                    )
                    asyncio.create_task(self.router.send(resp))
                elif message.type == "vault_response":
                    handle_vault_response(payload)
            except Exception as e:
                log.warning(f"vault_share protocol error from {message.sender}: {e}")
            return

        # Handle idea_submit — agents submit ideas to the shared Ötletláda
        if message.type == "idea_submit":
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}
            try:
                from .core.idea_review import parse_idea_submit, store_agent_idea
                idea = parse_idea_submit(payload)
                if idea:
                    if not idea.get("submitted_by") or idea.get("submitted_by") == "agent":
                        idea["submitted_by"] = message.sender
                    pg_pool = getattr(self, "_pg_pool", None)
                    idea_id = await store_agent_idea(pg_pool, idea)
                    resp = A2AMessage.create(
                        sender=self.node_name,
                        recipient=message.sender,
                        msg_type="idea_submit_ack",
                        payload={"ok": bool(idea_id), "idea_id": idea_id, "title": idea["title"][:100]},
                    )
                    asyncio.create_task(self.router.send(resp))
                    log.info(f"💡 idea_submit from {message.sender}: {idea['title'][:60]} → {idea_id}")
                else:
                    log.warning(f"idea_submit invalid payload from {message.sender}")
            except Exception as e:
                log.warning(f"idea_submit error from {message.sender}: {e}")
            return

        # Handle idea_vote — agents vote on ideas (determinisztikus szabályokkal)
        if message.type == "idea_vote":
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}
            try:
                from .core.idea_review import apply_vote_with_rules, make_implement_fn
                idea_id = (payload or {}).get("idea_id", "")
                vote = (payload or {}).get("vote", "up")
                pg_pool = getattr(self, "_pg_pool", None)
                if idea_id and pg_pool:
                    voter = f"agent:{message.sender}"
                    implement_fn = make_implement_fn(self, pg_pool)
                    result = await apply_vote_with_rules(pg_pool, idea_id, voter, vote, implement_fn=implement_fn)
                    resp = A2AMessage.create(
                        sender=self.node_name,
                        recipient=message.sender,
                        msg_type="idea_vote_ack",
                        payload={"idea_id": idea_id, "ok": bool(result.get("ok")),
                                 "score": result.get("score"), "action": result.get("action", "none")},
                    )
                    asyncio.create_task(self.router.send(resp))
                    log.info(f"🗳️ idea_vote from {message.sender} on {idea_id}: {vote} → {result.get('action')}")
            except Exception as e:
                log.warning(f"idea_vote error from {message.sender}: {e}")
            return

        # Handle peer_offline / peer_online status broadcasts — update peer_discovery
        if message.type in ("peer_offline", "peer_online"):
            payload = message.payload if isinstance(message.payload, dict) else {}
            if isinstance(message.payload, str):
                try:
                    import json as _json
                    payload = _json.loads(message.payload)
                except Exception:
                    payload = {}
            offline_peer_name = payload.get("peer_name", "")
            if offline_peer_name and offline_peer_name != self.node_name and self.peer_discovery:
                peer = self.peer_discovery.get_peer(offline_peer_name)
                if peer:
                    import time as _time
                    # Check our own direct P2P link to this peer before trusting
                    # a remote peer_offline broadcast. If we have an active,
                    # fresh P2P connection, the peer is reachable from us —
                    # the remote report refers to a different link, not ours.
                    our_p2p_active = (
                        peer.p2p_available
                        and peer.last_seen > 0
                        and (_time.time() - peer.last_seen) < 120  # seen in last 2 min
                    )
                    if message.type == "peer_offline":
                        if our_p2p_active:
                            log.info(
                                f"Received peer_offline from {message.sender} for {offline_peer_name} "
                                f"but our P2P link is active (last_seen {_time.time() - peer.last_seen:.0f}s ago) — ignoring false offline"
                            )
                        else:
                            peer.p2p_available = False
                            peer.http_available = False
                            log.info(f"Received peer_offline from {message.sender}: marked {offline_peer_name} as unavailable")
                    else:  # peer_online
                        peer.p2p_available = True
                        peer.pg_available = True
                        log.info(f"Received peer_online from {message.sender}: marked {offline_peer_name} as available")
                else:
                    log.debug(f"Received {message.type} for unknown peer {offline_peer_name} from {message.sender}")

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

    def apply_resource_limits(self) -> None:
        """Apply process-level resource limits from config (memory, nice, open files, etc.).
        
        Called early in start() to prevent OOM and control priority.
        Silently skips any limits that require elevated privileges.
        """
        rl = self.config.resource_limits
        if not rl:
            return
        
        log.info(f"Applying resource limits: memory_max_mb={rl.memory_max_mb}, nice={rl.nice}, "
                 f"open_files={rl.open_files}, cpu_time={rl.cpu_time_seconds}")
        
        # Memory limit (RLIMIT_AS — virtual memory, works on both Linux and macOS)
        if rl.memory_max_mb is not None and rl.memory_max_mb > 0:
            mem_bytes = rl.memory_max_mb * 1024 * 1024
            mem_soft = (rl.memory_soft_mb * 1024 * 1024) if rl.memory_soft_mb else mem_bytes
            try:
                resource.setrlimit(resource.RLIMIT_AS, (mem_soft, mem_bytes))
                log.info(f"  ✅ Memory limit set: soft={rl.memory_soft_mb or rl.memory_max_mb}MB, hard={rl.memory_max_mb}MB")
            except (ValueError, OSError) as e:
                # macOS may not support RLIMIT_AS; try RLIMIT_RSS as fallback
                try:
                    resource.setrlimit(resource.RLIMIT_RSS, (mem_soft, mem_bytes))
                    log.info(f"  ✅ Memory limit set (RSS): soft={rl.memory_soft_mb or rl.memory_max_mb}MB, hard={rl.memory_max_mb}MB")
                except (ValueError, OSError) as e2:
                    log.warning(f"  ⚠️ Could not set memory limit: {e2}")
        
        # Nice (process priority) — requires no special privileges for positive values
        if rl.nice is not None and rl.nice > 0:
            try:
                os.nice(rl.nice)
                log.info(f"  ✅ Nice set to {rl.nice} (lower priority)")
            except OSError as e:
                log.warning(f"  ⚠️ Could not set nice to {rl.nice}: {e}")
        
        # Open file descriptors limit
        if rl.open_files is not None and rl.open_files > 0:
            try:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                new_soft = min(rl.open_files, hard)
                resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
                log.info(f"  ✅ Open files limit set: {new_soft} (hard={hard})")
            except (ValueError, OSError) as e:
                log.warning(f"  ⚠️ Could not set open files limit: {e}")
        
        # CPU time limit
        if rl.cpu_time_seconds is not None and rl.cpu_time_seconds > 0:
            try:
                resource.setrlimit(resource.RLIMIT_CPU, (rl.cpu_time_seconds, rl.cpu_time_seconds))
                log.info(f"  ✅ CPU time limit set: {rl.cpu_time_seconds}s")
            except (ValueError, OSError) as e:
                log.warning(f"  ⚠️ Could not set CPU time limit: {e}")
        
        # Core dump size (0 = no core dumps)
        if rl.core_size_mb is not None:
            core_bytes = rl.core_size_mb * 1024 * 1024
            try:
                resource.setrlimit(resource.RLIMIT_CORE, (core_bytes, core_bytes))
                log.info(f"  ✅ Core dump limit set: {rl.core_size_mb}MB")
            except (ValueError, OSError) as e:
                log.warning(f"  ⚠️ Could not set core dump limit: {e}")

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

        # Ensure an SSH identity keypair exists for mesh tunnels (installer
        # does this too, but nodes started without install get it here).
        try:
            from .core.bootstrap import ensure_ssh_key
            _priv, _pub = ensure_ssh_key(self.node_name)
            if _priv:
                log.info(f"SSH identity ready: {_priv}")
        except Exception as e:
            log.debug(f"ensure_ssh_key skipped: {e}")

        # ── Process Lock Takeover (Marveen-inspired) ──
        # Ensure only one instance runs per port — kill zombie predecessors
        try:
            from .core.process_lock import ensure_single_instance
            port = getattr(self.config, 'api_port', 8650)
            lock_ok = await ensure_single_instance(port, grace_s=3.0)
            if not lock_ok:
                log.error(f"Port {port} is held by another process — takeover failed")
                return False
        except ImportError:
            pass
        except Exception as e:
            log.warning(f"Process lock check failed (non-fatal): {e}")

        # Apply resource limits early (memory cap, nice, etc.)
        self.apply_resource_limits()

        # Initialize direct PG connection for writes (shared pool for all subsystems)
        if not await self._init_pg_write_conn():
            log.warning("PG write connection failed — will retry")

        # Inject shared pool into subsystems that would otherwise create their own
        if self._pg_pool and self._pg_pool.is_connected():
            self._pg_transport._shared_pool = self._pg_pool
            self._pg_transport._owns_pool = False
            self._p2p_transport._shared_pool = self._pg_pool
            log.info("Shared PG pool injected into PG transport + P2P MessageAuth")

            # Inject PG pool into SSH key sync (audit persist)
            if getattr(self, 'ssh_key_sync', None):
                self.ssh_key_sync._pg_pool = self._pg_pool._pool if hasattr(self._pg_pool, '_pool') else self._pg_pool

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
            # Wire up P2P heartbeat callback — updates peer version in peer_discovery
            self._p2p_transport.set_heartbeat_callback(self._on_p2p_heartbeat)
            log.info("✅ P2P callbacks registered (ACK + peer_connected + peer_disconnected + heartbeat)")
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

        # Start SSH tunnel transport (if enabled)
        if self.config.ssh_tunnel.enabled:
            results["ssh_tunnel"] = await self._ssh_tunnel_transport.start()
            if results["ssh_tunnel"]:
                log.info("✅ SSH tunnel transport started")
            else:
                log.warning("❌ SSH tunnel transport failed (non-critical, P2P fallback)")
                await self.debug_log("WARNING", "transport", "SSH tunnel transport failed (non-critical)")

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
                try:
                    await self.debug_log("WARNING", "transport", "mDNS discovery failed (zeroconf not installed or multicast unavailable)")
                except Exception:
                    pass  # debug_log may block if PG pool not fully ready

        # 5. UDP broadcast discovery (works on local network + Tailscale)
        tailscale_if = self.config.discovery.tailscale_interface
        udp_interfaces = [tailscale_if] if tailscale_if else None
        self._udp_discovery = UDPBroadcastDiscovery(
            node_name=self.node_name,
            p2p_port=self.config.p2p.listen_port,
            health_port=self.config.health_port or 8650,
            discovery_port=self.config.discovery.udp_broadcast_port,
            interfaces=udp_interfaces,
            version=self._resolved_version,
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
        # v2 SSH key protocol: periodic announce to coordinator (new nodes
        # announce themselves; the root aggregates and bundles for everyone)
        if getattr(self, 'ssh_key_sync', None):
            self._tasks.append(asyncio.create_task(self._ssh_key_announce_loop()))
            # Embedded sshd self-heal loop (containers: restarts heal sshd)
            if getattr(self, 'embedded_sshd', None):
                self._tasks.append(asyncio.create_task(self.embedded_sshd.self_heal_loop(interval=60)))
        # v0.29: Auto-Bootstrap + Self-Healing loop
        self._tasks.append(asyncio.create_task(self._auto_bootstrap_heal_loop()))

        # v0.40: Memory maintenance loop — capsule promotion + auto skill generation
        self._tasks.append(asyncio.create_task(self._memory_maintenance_loop()))

        # v0.42: Built-in log rotation — gzip+truncate at log_max_mb (default 100MB)
        self._tasks.append(asyncio.create_task(self._log_rotation_loop()))
        self._prune_node_log_archives()

        # v0.43: VPN (Tailscale) health loop — figyeli a VPN-állapotot, state-váltásnál logol
        try:
            from .core.vpn import vpn_health_loop
            self._tasks.append(asyncio.create_task(vpn_health_loop(self)))
        except Exception as _vpn_loop_err:
            log.debug(f"VPN health loop indítása kihagyva: {_vpn_loop_err}")

        # v0.41: Coordinator idea-review loop — ötletláda felülvizsgálat (csak coordinatoron fut)
        try:
            from .core.idea_review import start_review_loop, make_implement_fn
            # Az auto-approve a node delegációján keresztül valósítson meg:
            self._idea_implement_fn = make_implement_fn(self, getattr(self, "_pg_pool", None))
            review_task = await start_review_loop(self)
            if review_task:
                self._tasks.append(review_task)
        except Exception as e:
            log.debug(f"idea-review loop not started: {e}")

        # Start alert manager evaluation loop
        if hasattr(self, 'dashboard') and self.dashboard and hasattr(self.dashboard, 'alert_manager'):
            asyncio.create_task(self.dashboard.alert_manager.start())

        # Auto-update: check for new versions periodically
        auto_update_cfg = getattr(self.config, 'auto_update', None)
        if auto_update_cfg and getattr(auto_update_cfg, 'enabled', False):
            check_interval = getattr(auto_update_cfg, 'check_interval', 300)
            self._tasks.append(asyncio.create_task(self._auto_update_loop(check_interval)))

        # Auto-update state (shared with health endpoint)
        self._updater_state = {"state": "idle", "last_check": None, "last_update": None, "current_version": self._resolved_version}

        # Start priority queue processor
        self.router.start_priority_queue()

        # Start GossipSub for efficient topic-based broadcast
        asyncio.create_task(self.router._gossipsub.start())

        # Start peer discovery (link P2P transport and PG pool for auto-connect)
        self.peer_discovery.p2p_transport = self._p2p_transport
        self.peer_discovery._pg_pool = self._pg_pool
        # P2: Wire up peer address resolver — P2P transport can now dynamically
        # connect to peers it hasn't connected to yet, using peer_discovery data
        self._p2p_transport._peer_address_resolver = self.peer_discovery.resolve_peer_address
        self.memory_sync._pg_pool = self._pg_pool
        # Wire up delegation manager with PG pool and start polling
        self.delegation.pg_pool = self._pg_pool
        # Wire up Marveen DB (audit trail, kanban comments, daily logs)
        self._set_marveen_db_pool(self._pg_pool)
        # Wire up health scorer with PG pool for persistence
        if hasattr(self, 'router') and hasattr(self.router, '_health_scorer'):
            self.router._health_scorer.set_pg_pool(self._pg_pool, self.node_name)
            # Restrict health records to real mesh agents (self + known peers).
            # Without this, load_from_pg() skips EVERYTHING (empty
            # valid_node_names regression from 2026-08-31 patch) or loads
            # phantom entries ("unknown", "http", "pg_notify", human names).
            valid_names = {self.node_name}
            try:
                if self.peer_discovery:
                    for pname in (self.peer_discovery.get_all_peers() or {}):
                        valid_names.add(pname)
            except Exception as e:
                log.debug(f"Could not enumerate peers for health scorer: {e}")
            try:
                self.router._health_scorer.set_valid_node_names(valid_names)
            except Exception as e:
                log.debug(f"set_valid_node_names failed: {e}")
            # Load previous health scores from PG
            asyncio.create_task(self.router._health_scorer.load_from_pg())
            # Start background persistence (60s interval)
            asyncio.create_task(self.router._health_scorer.start_persistence())
        # Register built-in task handlers
        self.delegation.register_handler("monitoring", self._handle_monitoring_task)
        self.delegation.register_handler("generic", self._handle_generic_task)
        # Research/analysis: dedicated handler — the keyword dispatcher inside
        # _handle_generic_task misroutes research tasks with "teszt"/"validalas"/"generate"
        # wording into code-generation. task_type is the EXPLICIT signal: honor it.
        self.delegation.register_handler("research", self._handle_research_task)
        self.delegation.register_handler("analysis", self._handle_research_task)
        self.delegation.register_handler("web_search", self._handle_research_task)
        self.delegation.register_handler("code", self._handle_generic_task)
        self.delegation.register_handler("diagnostic", self._handle_generic_task)  # diagnostic tasks use generic handler
        self.delegation.register_handler("deploy", self._handle_deploy_task)
        self.delegation.register_handler("code_review", self._handle_code_review_task)
        self.delegation.register_handler("local_maintenance", self._handle_local_maintenance_task)
        # Workflow capability task types → generic handler
        self.delegation.register_handler("web_search", self._handle_generic_task)
        self.delegation.register_handler("summarization", self._handle_generic_task)
        self.delegation.register_handler("data_analysis", self._handle_generic_task)
        self.delegation.register_handler("code_generation", self._handle_generic_task)
        self.delegation.on_result(self._on_delegation_result)


        # ── Sync delegation handler types into config capabilities ──
        # This lets the SmartRouter find agents by task type (monitoring, code, etc.)
        existing_caps = set(getattr(self.config, 'capabilities', []) or [])
        for handler_type in self.delegation._handlers.keys():
            existing_caps.add(handler_type)
        self.config.capabilities = sorted(existing_caps)
        log.info(f"Capabilities updated with delegation handlers: {self.config.capabilities}")

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

        # Start diagnostic engine
        try:
            await self.diagnostics.start()
        except Exception as e:
            log.warning(f"Diagnostic engine start failed (non-fatal): {e}")

        # Auto-advertise config skills to mesh_skills table
        try:
            await self._auto_advertise_skills()
        except Exception as e:
            log.warning(f"Skill auto-advertise failed (non-fatal): {e}")

        # Auto-sync published skills from PG (pull skills from other nodes)
        asyncio.create_task(self._auto_sync_skills_delayed())

        # At least one transport must be working
        any_ok = any(results.values())
        if any_ok:
            log.info(f"Mesh node '{self.node_name}' started ({sum(results.values())}/3 transports)")
            # Read recovery notes at startup (if PG pool available)
            try:
                from .core.dashboard_recovery import read_unread_recovery_notes
                notes = await read_unread_recovery_notes(self._pg_pool, self.node_name)
                if notes:
                    log.info(f"📋 Recovery notes: {len(notes)} unread note(s) found")
                    for note in notes:
                        log.info(f"   📝 From {note['author']}: {note['note']}")
                        if note.get('actions'):
                            log.info(f"      Actions: {', '.join(note['actions'])}")
                else:
                    log.debug("No unread recovery notes")
            except Exception as e:
                log.warning(f"Recovery notes read failed (non-fatal): {e}")
        else:
            log.error("All transports failed!")

        return any_ok

    # ── Outbound delegation: proactive task forwarding ──

    def _get_known_peer_count(self) -> int:
        """Return the number of known peers (excluding self)."""
        if not self.peer_discovery:
            return 0
        return len(self.peer_discovery._peers)

    def _is_overloaded(self) -> bool:
        """Check if this node is overloaded and should delegate out."""
        try:
            import psutil
            cpu = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory().percent
            # Overloaded if CPU > 75% OR memory > 85%
            return cpu > 75 or mem > 85
        except ImportError:
            return False

    async def _maybe_auto_delegate(self, task: dict) -> bool:
        """Check if an incoming task should be forwarded to other peers.

        Delegates out when:
        - This node is overloaded (high CPU/memory)
        - There are known peers who could pick it up
        - The task wasn't already delegated by someone else (avoid loops)

        Returns True if the task was forwarded (caller should skip local execution).
        """
        from_agent = task.get("from_agent", "")
        task_id = str(task.get("task_id", ""))
        subject = task.get("subject", "")

        # Don't auto-delegate if no peers available
        peer_count = self._get_known_peer_count()
        if peer_count < 1:
            return False

        # Don't auto-delegate if we're not overloaded
        if not self._is_overloaded():
            return False

        # Don't create delegation loops: skip if task came from another agent
        # (only forward tasks that originated from dashboard/API, not from peers)
        # We detect this: if from_agent != self.node_name, it came from a peer
        if from_agent != self.node_name:
            log.debug(f"Auto-delegate skip: task {task_id} from {from_agent} (not our origin)")
            return False

        # Re-post as available for any peer to claim
        desc_raw = task.get("description", "")
        try:
            import json as _json
            desc_data = _json.loads(desc_raw) if isinstance(desc_raw, str) else desc_raw
            task_type = desc_data.get("type", "generic") if isinstance(desc_data, dict) else "generic"
        except Exception:
            task_type = "generic"

        try:
            new_task_id = await self.delegation.delegate_task(
                to_agent="any",
                subject=f"[fwd] {subject}",
                description=desc_raw,
                task_type=task_type,
                priority=int(task.get("priority", 5)),
                available=True,
                timeout_minutes=30,
                max_retries=int(task.get("max_retries", 2)),
            )
            log.info(f"Auto-delegated overloaded task '{subject}' as available (new task_id={new_task_id}, peers={peer_count})")
            return True
        except Exception as e:
            log.warning(f"Auto-delegate failed for '{subject}': {e} — will execute locally")
            return False

    async def _on_delegation_result(self, task_row: dict):
        """Callback when a task we delegated out completes."""
        status = task_row.get("status", "")
        subject = task_row.get("subject", "?")
        result = task_row.get("result", "")
        assigned = task_row.get("assigned_agent", "?")
        if status == "completed":
            log.info(f"Delegated task '{subject}' completed by {assigned}: {result[:200]}")
        elif status == "failed":
            log.warning(f"Delegated task '{subject}' FAILED on {assigned}: {result[:200]}")
        
        # ── Update Kanban card agent_history ──
        if status in ("completed", "failed"):
            try:
                import time as _time
                from .core.kanban import _load_boards, _save_boards
                kanban_card_id = task_row.get("kanban_card_id", "")
                task_type = task_row.get("task_type", "")
                if kanban_card_id:
                    boards = _load_boards()
                    for board in boards:
                        for c in board.get("cards", []):
                            if c["id"] == kanban_card_id:
                                if "agent_history" not in c:
                                    c["agent_history"] = []
                                # Determine role
                                role = "executor"
                                if task_type == "code_review":
                                    role = "reviewer"
                                entry = {
                                    "agent": assigned,
                                    "role": role,
                                    "action": f"{'completed' if status == 'completed' else 'failed'} {'review' if task_type == 'code_review' else 'task'}",
                                    "result": (result or "")[:500],
                                    "timestamp": _time.time(),
                                }
                                if task_type == "code_review":
                                    # Parse verdict from result
                                    import re as _re
                                    json_match = _re.search(r'\{[^{}]*"verdict"[^{}]*\}', result or "", _re.DOTALL)
                                    if json_match:
                                        try:
                                            import json as _json
                                            vd = _json.loads(json_match.group())
                                            entry["verdict"] = vd.get("verdict", "")
                                            entry["reason"] = vd.get("reason", "")[:300]
                                        except Exception:
                                            pass
                                c["agent_history"].append(entry)
                                c["updated_at"] = _time.time()
                                if task_type == "code_review":
                                    c["review_status"] = entry.get("verdict", status)
                                    if entry.get("reason"):
                                        c["review_reason"] = entry["reason"]
                                    c["reviewed_at"] = _time.time()
                                else:
                                    c["delegation_status"] = status
                                    c["delegation_result"] = (result or "")[:2000]
                                    c["completed_at"] = str(task_row.get("completed_at", ""))[:30]
                                break
                    _save_boards(boards)
            except Exception as e:
                log.debug(f"Kanban agent_history update failed: {e}")
        
        # Feed delegation result into health scorer
        try:
            if hasattr(self, 'router') and hasattr(self.router, '_health_scorer'):
                hs = self.router._health_scorer
                # Try to get latency from task_row
                latency = 0.0
                if "latency_ms" in task_row:
                    latency = float(task_row.get("latency_ms", 0))
                elif "started_at" in task_row and "completed_at" in task_row:
                    import datetime as _dt
                    try:
                        s = _dt.datetime.fromisoformat(str(task_row["started_at"]).replace("Z", "+00:00"))
                        e = _dt.datetime.fromisoformat(str(task_row["completed_at"]).replace("Z", "+00:00"))
                        latency = (e - s).total_seconds() * 1000
                    except Exception:
                        pass
                if status == "completed":
                    hs.record_success(assigned, latency_ms=latency)
                elif status == "failed":
                    hs.record_failure(assigned)
                    log.info(f"📊 Health score updated: {assigned} → {hs.get_score(assigned):.3f} (delegation {'success' if status == 'completed' else 'failure'})")
        except Exception as e:
            log.debug(f"Health score feedback skipped: {e}")
        
        # Save to Hindsight for future context injection
        try:
            from .core.hindsight_sync import HindsightSync
            if not hasattr(self, '_hindsight_sync'):
                self._hindsight_sync = HindsightSync(self)
                if hasattr(self, '_pg_pool') and self._pg_pool:
                    self._hindsight_sync.set_pg_pool(self._pg_pool)
            if self._hindsight_sync._enabled:
                await self._hindsight_sync.save_delegation_result(task_row)
        except Exception as e:
            log.debug(f"Hindsight save skipped: {e}")
        
        # ── Record token usage + cost ──
        try:
            from .core.token_usage import record_usage as _record_usage
            # Extract token counts from result if available
            result_str = str(result or "")
            # Try to parse token info from result JSON
            input_tokens = 0
            output_tokens = 0
            model = "unknown"
            try:
                import json as _json
                # result may be JSON with token info, or plain text
                result_data = _json.loads(result_str) if result_str.startswith("{") else {}
                if isinstance(result_data, dict):
                    usage = result_data.get("usage", {})
                    input_tokens = usage.get("input_tokens", usage.get("prompt_tokens", 0))
                    output_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0))
                    model = result_data.get("model", "unknown")
            except Exception:
                pass
            # Fallback: estimate tokens from result length (rough: 1 token ≈ 4 chars)
            if input_tokens == 0 and output_tokens == 0:
                output_tokens = min(len(result_str) // 4, 50000)
            _record_usage(assigned, model, input_tokens, output_tokens, task_id=str(task_row.get("id", "")))
            log.debug(f"[costops] Recorded: {assigned} model={model} in={input_tokens} out={output_tokens}")
        except Exception as e:
            log.debug(f"[costops] Token usage recording skipped: {e}")
        
        # ── Auto Skill-Factory ──
        try:
            from .core.auto_skill import maybe_generate_skill
            skill_name = await maybe_generate_skill(task_row, self.node_name)
            if skill_name:
                log.info(f"🧠 Auto-skill generated: {skill_name}")
                # Register in PG mesh_skills
                if self._pg_pool:
                    try:
                        skill_id = "skill-" + self.node_name + "-" + skill_name
                        async with self._pg_pool.acquire() as conn:
                            await conn.execute(
                                """INSERT INTO mesh.mesh_skills (skill_id, agent_name, skill_name, display_name, description, tags, status)
                                   VALUES ($1, $2, $3, $4, $5, $6, 'active')
                                   ON CONFLICT (skill_id) DO UPDATE SET updated_at = NOW()""",
                                skill_id, self.node_name, skill_name,
                                skill_name.replace('-', ' ').title(),
                                "Auto-generated from delegation: " + task_row.get("subject", "")[:200],
                                ["auto", "generated"]
                            )
                            log.info(f"🧠 Auto-skill registered in mesh: {skill_id}")
                    except Exception as re:
                        log.debug(f"Auto-skill PG register skipped: {re}")
                # Broadcast to mesh so other nodes know about the new skill
                try:
                    await self._auto_advertise_skills()
                except Exception:
                    pass
        except Exception as e:
            log.debug(f"Auto-skill generation skipped: {e}")


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

    async def _handle_local_maintenance_task(self, task: dict, context: dict) -> dict:
        """Handle local_maintenance tasks — execute on THIS node only.
        
        Supported actions (via context['action']):
        - git_pull: git fetch + merge on local repo
        - service_restart: restart a systemd/launchd service
        - log_rotate: compress and truncate old logs
        - disk_check: check disk usage, alert if > threshold
        - health_check: run self-diagnostic
        - cleanup: remove temp files, old git locks
        - custom: run arbitrary shell command (from context['command'])
        
        Returns dict with result, actions_taken, and metrics.
        """
        import subprocess
        import shutil
        import os
        
        action = context.get("action", "health_check")
        results = []
        actions_taken = []
        
        log.info(f"Local maintenance task: action={action}")
        
        if action == "git_pull":
            repo_dir = context.get("repo_dir", os.path.dirname(os.path.abspath(__file__)))
            git_remote = context.get("remote", "origin")
            try:
                r = subprocess.run(
                    ["git", "fetch", "--prune", git_remote],
                    capture_output=True, text=True, timeout=60, cwd=repo_dir
                )
                results.append(f"git fetch: {r.stdout.strip() or r.stderr.strip() or 'OK'}")
                actions_taken.append(f"git fetch {git_remote}")
                
                r2 = subprocess.run(
                    ["git", "merge", "--ff-only", f"{git_remote}/main"],
                    capture_output=True, text=True, timeout=30, cwd=repo_dir
                )
                if r2.returncode == 0:
                    results.append(f"git merge: {r2.stdout.strip() or 'Already up to date'}")
                    actions_taken.append(f"git merge --ff-only {git_remote}/main")
                else:
                    results.append(f"git merge FAILED: {r2.stderr.strip()}")
            except Exception as e:
                results.append(f"git_pull error: {e}")
        
        elif action == "service_restart":
            service_name = context.get("service", "a2a-mesh")
            platform = context.get("platform", "")
            try:
                import platform as plat
                system = platform or plat.system().lower()
                if system == "darwin" or system == "macos":
                    r = subprocess.run(
                        ["launchctl", "stop", f"com.hermes.{service_name}"],
                        capture_output=True, text=True, timeout=10
                    )
                    import time as _t
                    _t.sleep(2)
                    subprocess.run(
                        ["launchctl", "start", f"com.hermes.{service_name}"],
                        capture_output=True, text=True, timeout=10
                    )
                    results.append(f"launchctl restart {service_name}")
                    actions_taken.append(f"launchctl stop+start {service_name}")
                elif system == "linux":
                    r = subprocess.run(
                        ["systemctl", "--user", "restart", service_name],
                        capture_output=True, text=True, timeout=30
                    )
                    results.append(f"systemctl restart {service_name}: {r.stdout.strip() or r.stderr.strip() or 'OK'}")
                    actions_taken.append(f"systemctl --user restart {service_name}")
                elif system == "windows":
                    r = subprocess.run(
                        ["schtasks", "/End", "/TN", f"A2A-Mesh-{service_name}"],
                        capture_output=True, text=True, timeout=10
                    )
                    import time as _t
                    _t.sleep(2)
                    subprocess.run(
                        ["schtasks", "/Run", "/TN", f"A2A-Mesh-{service_name}"],
                        capture_output=True, text=True, timeout=10
                    )
                    results.append(f"schtasks restart {service_name}")
                    actions_taken.append(f"schtasks restart {service_name}")
                else:
                    results.append(f"Unknown platform: {system}")
            except Exception as e:
                results.append(f"service_restart error: {e}")
        
        elif action == "log_rotate":
            log_dir = context.get("log_dir", os.path.expanduser("~/a2a_mesh/logs"))
            max_size_mb = context.get("max_size_mb", 50)
            try:
                import glob
                rotated = 0
                for log_file in glob.glob(os.path.join(log_dir, "*.log")):
                    size_mb = os.path.getsize(log_file) / (1024 * 1024)
                    if size_mb > max_size_mb:
                        # Compress + truncate
                        archive = f"{log_file}.{int(time.time())}.gz"
                        subprocess.run(["gzip", "-c", log_file], stdout=open(archive, "wb"), timeout=60)
                        open(log_file, "w").close()  # Truncate
                        rotated += 1
                        results.append(f"Rotated {os.path.basename(log_file)} ({size_mb:.1f}MB → archive)")
                if rotated == 0:
                    results.append("No logs needed rotation")
                actions_taken.append(f"log_rotate: {rotated} files")
            except Exception as e:
                results.append(f"log_rotate error: {e}")
        
        elif action == "disk_check":
            threshold = context.get("threshold_pct", 90)
            try:
                total, used, free = shutil.disk_usage("/")
                pct = (used / total) * 100
                results.append(f"Disk: {used//(1024**3)}GB/{total//(1024**3)}GB used ({pct:.1f}%)")
                if pct > threshold:
                    results.append(f"⚠️  Disk usage {pct:.1f}% > threshold {threshold}%")
                    # Find biggest files in a2a_mesh
                    r = subprocess.run(
                        ["find", os.path.dirname(os.path.abspath(__file__)), "-type", "f", "-size", "+100M"],
                        capture_output=True, text=True, timeout=30
                    )
                    if r.stdout.strip():
                        results.append(f"Large files: {r.stdout.strip()[:500]}")
                actions_taken.append(f"disk_check: {pct:.1f}%")
            except Exception as e:
                results.append(f"disk_check error: {e}")
        
        elif action == "cleanup":
            try:
                import glob
                cleaned = 0
                # Remove git lock files
                script_dir = os.path.dirname(os.path.abspath(__file__))
                for lock in glob.glob(os.path.join(script_dir, ".git/**/*.lock"), recursive=True):
                    os.remove(lock)
                    cleaned += 1
                    results.append(f"Removed: {lock}")
                # Remove old compressed logs (>30 days)
                log_dir = os.path.expanduser("~/a2a_mesh/logs")
                import time as _t
                cutoff = _t.time() - (30 * 86400)
                for old in glob.glob(os.path.join(log_dir, "*.gz")):
                    if os.path.getmtime(old) < cutoff:
                        os.remove(old)
                        cleaned += 1
                # Remove temp files
                for tmp in glob.glob("/tmp/a2a-mesh-*"):
                    if os.path.getmtime(tmp) < cutoff:
                        if os.path.isfile(tmp):
                            os.remove(tmp)
                            cleaned += 1
                results.append(f"Cleaned {cleaned} items")
                actions_taken.append(f"cleanup: {cleaned} items")
            except Exception as e:
                results.append(f"cleanup error: {e}")
        
        elif action == "health_check":
            try:
                uptime = int(time.time() - self._start_time) if self._start_time else 0
                peers = len(self.peer_discovery._peers) if self.peer_discovery else 0
                cpu = 0
                mem = 0
                try:
                    import psutil
                    cpu = psutil.cpu_percent(interval=0.5)
                    mem = psutil.virtual_memory().percent
                except ImportError:
                    pass
                results.append(f"Uptime: {uptime}s, Peers: {peers}, CPU: {cpu:.0f}%, Mem: {mem:.0f}%")
                actions_taken.append(f"health_check: uptime={uptime}s cpu={cpu:.0f}% mem={mem:.0f}%")
            except Exception as e:
                results.append(f"health_check error: {e}")
        
        elif action == "custom":
            command = context.get("command", "")
            timeout_s = context.get("timeout", 60)
            if command:
                try:
                    r = subprocess.run(
                        command, shell=True, capture_output=True, text=True, timeout=timeout_s
                    )
                    output = (r.stdout + r.stderr).strip()[:2000]
                    results.append(f"Command: {command}\nOutput: {output}")
                    actions_taken.append(f"custom: {command[:100]}")
                except subprocess.TimeoutExpired:
                    results.append(f"Command timed out after {timeout_s}s")
                except Exception as e:
                    results.append(f"Command error: {e}")
            else:
                results.append("No command specified")
        
        else:
            results.append(f"Unknown action: {action}")
        
        return {
            "result": "\n".join(results),
            "actions_taken": actions_taken,
            "context_updates": {
                "last_maintenance": int(time.time()),
                "action": action,
            },
        }

    async def _handle_deploy_task(self, task: dict, context: dict) -> dict:
        """Handle deploy-type tasks: git pull + service restart + health check.
        
        Runs locally on each peer node. The coordinator (Nova) delegates this
        task to each peer after pushing to gitea.
        
        Description JSON:
        {
            "remote": "gitea" | "origin",  # git remote name
            "branch": "main",
            "restart_cmd": "systemctl --user restart a2a-mesh",  # or launchctl for macOS
            "health_url": "http://localhost:8650/api/health",
            "timeout": 30
        }
        """
        import subprocess
        import json as _json
        import asyncio
        from datetime import datetime, timezone

        node = self.node_name
        now = datetime.now(timezone.utc).isoformat()
        steps = []

        # Parse description — may be double-wrapped JSON
        desc_raw = task.get("description", "{}")
        try:
            cfg = _json.loads(desc_raw) if isinstance(desc_raw, str) else desc_raw
            # Unwrap if delegate_task wrapped our config in {"type":..., "description": "..."}
            if isinstance(cfg, dict) and "description" in cfg and "remote" not in cfg:
                inner = cfg.get("description", "")
                if isinstance(inner, str):
                    try:
                        cfg = _json.loads(inner)
                    except (ValueError, TypeError):
                        pass
        except (ValueError, TypeError):
            cfg = {}

        remote = cfg.get("remote", "origin")
        branch = cfg.get("branch", "main")
        restart_cmd = cfg.get("restart_cmd", "systemctl --user restart a2a-mesh")
        health_url = cfg.get("health_url", "http://localhost:8650/api/health")
        timeout = cfg.get("timeout", 30)

        # Determine repo path — try config, then __file__ location
        repo_path = self.config._config.get("repo_path") if hasattr(self.config, "_config") else None
        if not repo_path:
            import os
            script_path = os.path.abspath(__file__)
            # node.py is at repo_root/node.py — repo_path = dirname(node.py)
            # On some installs node.py is at repo_root/core/node.py — go up one more
            repo_path = os.path.dirname(script_path)
            if os.path.basename(repo_path) == "core":
                repo_path = os.path.dirname(repo_path)
        
        steps.append(f"[{node}] repo_path={repo_path}")

        # Step 1: Git fetch + pull (robust: stale-lock cleanup, retry, FETCH_HEAD merge)
        try:
            import os as _os
            # Stale index.lock cleanup — known issue on Runa after crashed deploy processes
            try:
                lock = _os.path.join(repo_path, ".git", "index.lock")
                if _os.path.exists(lock):
                    _os.remove(lock)
                    steps.append(f"[{node}] git: removed stale index.lock")
            except Exception:
                pass

            fetch_ok = False
            # Try full prune fetch first; fallback to branch-only fetch (immune to ref-lock)
            for fetch_args in ([remote], [remote, branch]):
                try:
                    r = subprocess.run(
                        ["git", "fetch"] + fetch_args,
                        cwd=repo_path, capture_output=True, text=True, timeout=60
                    )
                    if r.returncode == 0:
                        fetch_ok = True
                        steps.append(f"[{node}] git fetch {' '.join(fetch_args)}: OK")
                        break
                    steps.append(f"[{node}] git fetch {' '.join(fetch_args)}: FAIL — {r.stderr.strip()[:150]}")
                except subprocess.TimeoutExpired:
                    steps.append(f"[{node}] git fetch {' '.join(fetch_args)}: TIMEOUT")
            if not fetch_ok:
                return {"result": "\n".join(steps), "files": [], "context_updates": {"deploy_status": "failed_git"}}

            # Merge from FETCH_HEAD — always reflects the last successful fetch,
            # immune to 'cannot lock ref' failures that leave origin/main stale.
            r = subprocess.run(
                ["git", "merge", "--ff-only", "FETCH_HEAD"],
                cwd=repo_path, capture_output=True, text=True, timeout=30
            )
            if r.returncode != 0:
                steps.append(f"[{node}] git merge --ff-only: retry with reset --hard FETCH_HEAD")
                r2 = subprocess.run(
                    ["git", "reset", "--hard", "FETCH_HEAD"],
                    cwd=repo_path, capture_output=True, text=True, timeout=30
                )
                if r2.returncode != 0:
                    steps.append(f"[{node}] git reset: FAIL — {r2.stderr.strip()[:200]}")
                    return {"result": "\n".join(steps), "files": [], "context_updates": {"deploy_status": "failed_git"}}
                steps.append(f"[{node}] git reset --hard FETCH_HEAD: OK — {r2.stdout.strip()[:100]}")
            else:
                # Check if anything actually changed
                r_status = subprocess.run(
                    ["git", "log", "--oneline", "-1"],
                    cwd=repo_path, capture_output=True, text=True, timeout=10
                )
                steps.append(f"[{node}] git merge: OK — {r_status.stdout.strip()[:80]}")
        except subprocess.TimeoutExpired:
            steps.append(f"[{node}] git: TIMEOUT")
            return {"result": "\n".join(steps), "files": [], "context_updates": {"deploy_status": "timeout_git"}}
        except Exception as e:
            steps.append(f"[{node}] git: ERROR — {e}")
            return {"result": "\n".join(steps), "files": [], "context_updates": {"deploy_status": "error_git"}}

        # Step 2: Restart service (delayed — let handler return first)
        try:
            import platform as _pf
            import asyncio as _aio
            if _pf.system() == "Darwin":
                # macOS — launchctl (delayed in background)
                subprocess.Popen(
                    ["bash", "-c", "sleep 3 && launchctl stop com.hermes.a2a-mesh-node && sleep 2 && launchctl start com.hermes.a2a-mesh-node"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                steps.append(f"[{node}] launchctl restart: scheduled (3s delay)")
            else:
                # Linux — systemctl (delayed in background)
                subprocess.Popen(
                    ["bash", "-c", f"sleep 3 && {restart_cmd}"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                steps.append(f"[{node}] restart: scheduled (3s delay)")
            # Don't wait for restart — return result immediately
            steps.append(f"[{node}] deploy complete — node will restart shortly")
            return {"result": "\n".join(steps), "files": [], "context_updates": {"deploy_status": "success"}}
        except Exception as e:
            steps.append(f"[{node}] restart: ERROR — {e}")
            return {"result": "\n".join(steps), "files": [], "context_updates": {"deploy_status": "error_restart"}}

        # Unreachable — deploy returns before restart
        return {"result": "\n".join(steps), "files": [], "context_updates": {"deploy_status": "success"}}

    async def _handle_code_review_task(self, task: dict, context: dict) -> dict:
        """Handle code_review tasks: read a file from the repo, send it to LLM for review.

        Description JSON:
        {
            "file": "core/delegation.py",     # file to review (relative to repo root)
            "code": "...",                     # OR inline code to review
            "focus": "security|performance|bugs|style",  # review focus (optional)
            "context": "..."                   # additional context (optional)
        }
        """
        import os
        import json as _json
        from datetime import datetime, timezone

        node = self.node_name
        now = datetime.now(timezone.utc).isoformat()
        steps = []

        # Parse description (may be double-wrapped)
        desc_raw = task.get("description", "{}")
        try:
            cfg = _json.loads(desc_raw) if isinstance(desc_raw, str) else desc_raw
            if isinstance(cfg, dict) and "description" in cfg and "file" not in cfg and "code" not in cfg:
                inner = cfg.get("description", "")
                if isinstance(inner, str):
                    try:
                        cfg = _json.loads(inner)
                    except (ValueError, TypeError):
                        pass
        except (ValueError, TypeError):
            cfg = {}

        file_path = cfg.get("file", "")
        inline_code = cfg.get("code", "")
        focus = cfg.get("focus", "general")
        extra_ctx = cfg.get("context", "")

        # Get code to review
        code_to_review = ""
        if inline_code:
            code_to_review = inline_code
            steps.append(f"[{node}] Reviewing inline code ({len(code_to_review)} chars)")
        elif file_path:
            # Determine repo path
            script_path = os.path.abspath(__file__)
            repo_path = os.path.dirname(script_path)
            if os.path.basename(repo_path) == "core":
                repo_path = os.path.dirname(repo_path)
            full_path = os.path.join(repo_path, file_path)
            try:
                with open(full_path, "r", errors="replace") as f:
                    code_to_review = f.read()
                steps.append(f"[{node}] Reviewing {file_path} ({len(code_to_review)} chars)")
            except FileNotFoundError:
                return {"result": f"[{node}] File not found: {file_path}", "files": [], "context_updates": {"review_status": "file_not_found"}}
        else:
            return {"result": f"[{node}] No file or code provided for review", "files": [], "context_updates": {"review_status": "no_input"}}

        # Truncate if too long (LLM context limit)
        max_chars = 12000
        truncated = False
        if len(code_to_review) > max_chars:
            code_to_review = code_to_review[:max_chars]
            truncated = True
            steps.append(f"[{node}] Code truncated to {max_chars} chars")

        # LLM review
        import aiohttp

        ollama_url = getattr(self, '_ollama_url', None)
        if not ollama_url:
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
            steps.append(f"[{node}] No Ollama available — basic review only")
            # Basic heuristic review
            issues = []
            if "eval(" in code_to_review:
                issues.append("⚠️ Uses eval() — security risk")
            if "exec(" in code_to_review:
                issues.append("⚠️ Uses exec() — security risk")
            if "except:" in code_to_review and "except Exception" not in code_to_review:
                issues.append("🟡 Bare except clauses — may catch too broadly")
            if "TODO" in code_to_review or "FIXME" in code_to_review:
                issues.append("🟡 Contains TODO/FIXME markers")
            if code_to_review.count("def ") > 50:
                issues.append("🟡 Large file with many functions — consider splitting")
            result_text = f"[{node}] Code Review (heuristic)\nFile: {file_path or 'inline'}\nFocus: {focus}\n\n"
            result_text += f"Size: {len(code_to_review)} chars{' (truncated)' if truncated else ''}\n\n"
            result_text += "Issues:\n" + ("\n".join(issues) if issues else "✅ No obvious issues found")
            return {"result": result_text, "files": [], "context_updates": {"review_status": "heuristic", "issues": str(len(issues))}}

        # Pick model
        preferred_models = ["glm-5.2", "glm-5.1", "glm-4.7", "gemma4:31b", "kimi-k2.5", "qwen2.5:7b", "qwen2.5:3b"]
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
                model = available[0]
        except Exception:
            pass

        if not model:
            return {"result": f"[{node}] No LLM model available for review", "files": [], "context_updates": {"review_status": "no_model"}}

        steps.append(f"[{node}] Using LLM: {model}")

        focus_prompt = {
            "security": "Focus on security vulnerabilities: injection, path traversal, unsafe deserialization, secrets in code.",
            "performance": "Focus on performance issues: O(n²) loops, unnecessary allocations, blocking calls in async code, memory leaks.",
            "bugs": "Focus on logic bugs: race conditions, off-by-one errors, null/None dereference, unhandled exceptions.",
            "style": "Focus on code style: naming, documentation, complexity, DRY violations, type hints.",
            "general": "Review for bugs, security, performance, and code quality.",
        }.get(focus, "Review for bugs, security, performance, and code quality.")

        prompt = f"""You are a code reviewer on A2A Mesh node '{node}'. Review the following code.

File: {file_path or 'inline code'}
Review focus: {focus_prompt}

Additional context: {extra_ctx or 'none'}

Code to review:
```
{code_to_review}
```

Provide a structured review:
1. **Summary**: 1-2 sentence overview
2. **Issues**: List each issue with severity (🔴 CRITICAL, 🟠 HIGH, 🟡 MEDIUM, 🔵 LOW)
3. **Suggestions**: Specific improvement suggestions with code snippets where relevant
4. **Score**: Overall code quality score (1-10)

Be concise but thorough. Only report real issues, not style nitpicks unless focus is 'style'."""

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.2, "num_predict": 4096},
                }
                async with session.post(f"{ollama_url}/api/generate", json=payload) as resp:
                    if resp.status != 200:
                        steps.append(f"[{node}] LLM review failed: HTTP {resp.status}")
                        return {"result": "\n".join(steps), "files": [], "context_updates": {"review_status": "llm_error"}}
                    result = await resp.json()
                    review = result.get("response", "")

            if not review or len(review) < 20:
                steps.append(f"[{node}] LLM returned empty review")
                return {"result": "\n".join(steps), "files": [], "context_updates": {"review_status": "empty"}}

            # Build result
            result_text = f"[{node}] Code Review via {model}\n"
            result_text += f"File: {file_path or 'inline code'}\n"
            result_text += f"Focus: {focus}\n"
            result_text += f"Size: {len(code_to_review)} chars{' (truncated)' if truncated else ''}\n\n"
            result_text += review

            # Save review as file
            review_file = {
                "filename": f"review_{file_path.replace('/', '_')}_{now[:10]}.md" if file_path else f"review_inline_{now[:10]}.md",
                "content_type": "text/markdown",
                "content": review,
                "size": len(review),
            }

            steps.append(f"[{node}] Review complete ({len(review)} chars)")
            return {
                "result": result_text,
                "files": [review_file],
                "context_updates": {
                    "review_status": "success",
                    "review_model": model,
                    "review_focus": focus,
                },
            }
        except Exception as e:
            steps.append(f"[{node}] LLM review error: {e}")
            return {"result": "\n".join(steps), "files": [], "context_updates": {"review_status": "error"}}

    async def _handle_research_task(self, task: dict, context: dict) -> dict:
        """Dedicated handler for research/analysis task types.

        task_type='research'|'analysis'|'web_search' is the EXPLICIT route: it goes
        straight to the LLM text-answer path (_task_llm_research) without passing
        through the keyword dispatcher, which can misroute research-y wording
        ("teszt", "validalas", "generate") into code generation. Falls back to
        the generic handler when no LLM is reachable on this node.
        """
        import json as _json
        from datetime import datetime, timezone

        subject = task.get("subject", "unknown")
        desc_raw = task.get("description", "")
        now = datetime.now(timezone.utc)
        node = self.node_name

        # Extract description text (same parsing as the generic handler)
        desc_text = ""
        try:
            d = _json.loads(desc_raw) if isinstance(desc_raw, str) else desc_raw
            desc_text = d.get("description", "") if isinstance(d, dict) else str(desc_raw)
        except (ValueError, TypeError, AttributeError):
            desc_text = desc_raw if desc_raw else subject

        # Inject prior memory if available in context
        if isinstance(context, dict) and context.get("prior_memory"):
            desc_text = desc_text + "\n\n--- Prior Memory ---\n" + context["prior_memory"]

        result = await self._task_llm_research(node, now, subject, desc_text)
        if result:
            return result

        # No LLM on this node → let the generic handler try its deterministic paths
        log.info(f"[{node}] research: no LLM reachable, falling back to generic handler")
        return await self._handle_generic_task(task, context)

    async def _handle_generic_task(self, task: dict, context: dict) -> dict:
        """Handle generic delegated tasks. Parses description for instructions
        and dispatches to specialized sub-handlers based on keywords."""
        import platform
        import subprocess
        import json as _json
        from datetime import datetime, timezone

        # Auto-delegate if overloaded and peers are available
        if await self._maybe_auto_delegate(task):
            return {
                "result": f"[{self.node_name}] Task forwarded to available peers (overloaded)",
                "files": [],
                "context_updates": {"auto_delegated": "true"},
            }

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
            # Ötletláda-meta: az idea_id és source maradjon elérhető a végrehajtó
            # útvonalak számára (a JSON top-level mezői egyébként elvesznének itt)
            if isinstance(d, dict) and d.get("idea_id"):
                desc_ctx = dict(desc_ctx) if desc_ctx else {}
                desc_ctx["idea_id"] = d.get("idea_id")
                desc_ctx["idea_source"] = d.get("source", "")
                # A desc_text végére is ráfűzzük, hogy a kód-integrációs blokk megtalálja
                desc_text = (desc_text or "") + f"\n\n[idea_id: {d.get('idea_id')}]"
        except (ValueError, TypeError, AttributeError):
            desc_text = desc_raw if desc_raw else subject

        # Inject prior_memory from HindsightSync recall
        if isinstance(context, dict) and context.get("prior_memory"):
            desc_ctx = dict(desc_ctx) if desc_ctx else {}
            desc_ctx["prior_memory"] = context["prior_memory"]
            desc_text = desc_text + "\n\n--- Prior Memory ---\n" + context["prior_memory"]

        # ── Task dispatcher based on keywords ────────────────────────
        # Normalize: remove diacritics for matching (írj -> irj, fájl -> fajl)
        import unicodedata
        lower_raw = (subject + " " + desc_text).lower()
        # Also create an ASCII-normalized version for matching
        nfkd = unicodedata.normalize('NFKD', lower_raw)
        lower = ''.join(c for c in nfkd if not unicodedata.combining(c))

        # --- Research / analysis / comparison tasks → LLM text answer (NO code exec) ---
        # These must NOT reach the code-generation path: the LLM there treats the
        # description as a spec to program from (observed: it tried to execute the
        # task text as Python → SyntaxError). Research tasks need an ANSWER.
        # Trigger: explicit type in JSON description, or research-y keywords.
        _explicit_type = ""
        if isinstance(desc_ctx, dict):
            _explicit_type = str(desc_ctx.get("type", "")).lower()
        research_kws = ("felmérés", "felmeres", "felmérését", "survey", "research", "összehasonlít", "osszehasonlit",
                        "comparison", "compare", "elemzés", "elemzes", "analysis", "evaluation", "értékelés",
                        "ertekeles", "marveen.io", "marveen io", "review a projekt", "vélemény", "velemeny")
        if _explicit_type == "research" or any(kw in lower_raw for kw in research_kws):
            research_result = await self._task_llm_research(node, now, subject, desc_text)
            if research_result:
                return research_result
            # LLM unreachable → fall through to the normal paths below

        # --- Development suggestions / LLM analysis (check BEFORE system analysis) ---
        if any(kw in lower for kw in ("javaslat", "suggestion", "fejlesztesi", "development", "improvement", "optimalizal", "optimize", "refactor", "hiba", "bug", "fix", "problema", "problem", "issue", "hiány", "missing", "javit", "improve")):
            return await self._task_code_generation(node, now, subject, desc_text)

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

    async def _task_llm_research(self, node: str, now, subject: str, desc_text: str) -> dict | None:
        """Handle research/analysis/comparison tasks via LLM (text answer, NO code execution).

        The code-generation path treats the description as a spec to write a program
        from — for research tasks (surveys, comparisons, evaluations) that is the
        wrong tool: the LLM once tried to run the task text itself as Python
        (SyntaxError on the first em-dash). This handler asks the LLM to ANSWER
        the question instead, in the style of the code-review handler.
        Returns a result dict, or None if no LLM is reachable (caller falls back).
        """
        import aiohttp
        import json as _json

        ollama_url = getattr(self, '_ollama_url', None)
        if not ollama_url:
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
            return None

        # Pick model (same preference list as code review)
        preferred_models = ["glm-5.2", "glm-5.1", "glm-4.7", "gemma4:31b", "kimi-k2.5", "qwen2.5:7b", "qwen2.5:3b"]
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
                model = available[0]
        except Exception:
            pass
        if not model:
            return None

        prompt = f"""You are the '{node}' agent of the A2A Mesh — a decentralized multi-agent
network (4 nodes: nova/macOS, morzsa+runa/Linux, tor/HAOS container; each agent runs
on its OWN machine with its OWN local LLM; core mesh features are deterministic,
LLM is an optional layer). You are completing a RESEARCH task delegated to you.

Task subject: {subject}

Task description:
{desc_text[:12000]}

Answer the task as a research agent would — structured TEXT, not code:
1. Do what the task asks (analysis, comparison, evaluation, survey).
2. Structure the answer with clear sections/bullet lists.
3. If the task names external sources, reason about them from your knowledge
   and clearly mark what you could NOT verify.
4. If the task expects concrete suggestions, list them explicitly
   (e.g. 'SUGGESTION: <title> | <description> | <priority>').
Answer in Hungarian unless the task is in another language."""

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.3, "num_predict": 4096},
                }
                async with session.post(f"{ollama_url}/api/generate", json=payload) as resp:
                    if resp.status != 200:
                        log.warning(f"[{node}] research LLM HTTP {resp.status}")
                        return None
                    data = await resp.json()
                    answer = data.get("response", "").strip()

            if not answer or len(answer) < 20:
                return None

            result_text = f"[{node}] Research via {model}\nTask: {subject[:80]}\n\n{answer}"
            answer_file = {
                "filename": f"research_{now.strftime('%Y%m%d_%H%M%S')}.md",
                "content_type": "text/markdown",
                "content": answer,
                "size": len(answer),
            }
            return {
                "result": result_text,
                "files": [answer_file],
                "context_updates": {"task_type": "research", "model": model, "llm": True},
            }
        except Exception as e:
            log.warning(f"[{node}] research LLM error: {e}")
            return None

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
            # Először a markdown-fence nyelvi jelölése (```python) — az LLM-ek gyakran
            # fence-en belül adják a kódot, ilyenkor a raw startswith sosem matchel
            _strip = generated.strip()
            _fence_lang = None
            _fence_body = _strip
            if _strip.startswith("```"):
                _first_line = _strip.split("\n", 1)[0]
                _fence_lang = _first_line.replace("```", "").strip().lower() or None
                rest = _strip.split("\n", 1)[1] if "\n" in _strip else ""
                _fence_body = rest.rsplit("```", 1)[0] if "```" in rest else rest
            _start = (_fence_body.lstrip())[:200]

            ext = "txt"
            lang = "python"
            if _fence_lang in ("python", "py"):
                ext = "py"; lang = "python"
            elif _fence_lang in ("bash", "sh", "shell"):
                ext = "bash"; lang = "bash"
            elif _fence_lang in ("javascript", "js"):
                ext = "js"; lang = "javascript"
            elif _fence_lang == "html":
                ext = "html"; lang = "html"
            elif _start.startswith("<!DOCTYPE") or _start.startswith("<html"):
                ext = "html"; lang = "html"
            elif _start.startswith("#!/bin/bash") or _start.startswith("#!/bin/sh"):
                ext = "bash"; lang = "bash"
            elif _start.startswith("#!/usr/bin/env python") or "import " in _start or "def " in _start:
                ext = "py"; lang = "python"
            elif "function " in _start or "const " in _start or "=>" in _start:
                ext = "js"; lang = "javascript"

            filename = f"generated_{ext}_{node}_{now.strftime('%Y%m%d_%H%M%S')}.{ext}"
            log.info(f"[{node}] LLM generated {len(generated)} chars, saved as {filename}")

            # ── Auto-execute Python/bash scripts ──
            execution_result = None
            execution_error = None
            result_files = []

            if lang in ("python", "bash"):
                import subprocess as _sp
                import os as _os
                import time as _time
                import base64 as _b64

                pre_exec_time = _time.time()
                # Strip markdown code fences (```python ... ```) from LLM output
                import re as _re
                cleaned = generated.strip()
                fence_match = _re.match(r'^```[\w]*\n(.*?)```\s*$', cleaned, _re.DOTALL)
                if fence_match:
                    cleaned = fence_match.group(1).strip()
                elif cleaned.startswith('```'):
                    lines = cleaned.split('\n')
                    if lines[0].strip().startswith('```'):
                        lines = lines[1:]
                    if lines and lines[-1].strip() == '```':
                        lines = lines[:-1]
                    cleaned = '\n'.join(lines).strip()

                script_ext = ".py" if lang == "python" else ".sh"
                script_path = f"/tmp/a2a_task_{node}_{now.strftime('%Y%m%d_%H%M%S')}{script_ext}"
                try:
                    with open(script_path, "w", encoding="utf-8") as sf:
                        sf.write(cleaned)
                    if lang == "bash":
                        _os.chmod(script_path, 0o755)
                except Exception:
                    script_path = None

                if script_path:
                    try:
                        timeout_val = self.config.task.python_timeout if hasattr(self, 'config') and hasattr(self.config, 'task') else 120
                        cwd_val = self.config.task.tmp_dir if hasattr(self, 'config') and hasattr(self.config, 'task') else "/tmp"
                        if lang == "python":
                            venv_python = _os.path.expanduser(self.config.task.venv_python) if hasattr(self, 'config') and hasattr(self.config, 'task') else _os.path.expanduser("~/.hermes/scripts/a2a_mesh/.venv/bin/python")
                            python_bin = venv_python if _os.path.exists(venv_python) else "python3"
                            proc = _sp.run([python_bin, script_path], capture_output=True, text=True, timeout=timeout_val, cwd=cwd_val)
                        else:
                            bash_timeout = self.config.task.bash_timeout if hasattr(self, 'config') and hasattr(self.config, 'task') else 60
                            proc = _sp.run(["bash", script_path], capture_output=True, text=True, timeout=bash_timeout, cwd=cwd_val)

                        max_output = self.config.task.max_output_chars if hasattr(self, 'config') and hasattr(self.config, 'task') else 5000
                        max_error = self.config.task.max_error_chars if hasattr(self, 'config') and hasattr(self.config, 'task') else 2000
                        execution_result = proc.stdout[:max_output] if proc.stdout else ""
                        if proc.returncode != 0:
                            execution_error = proc.stderr[:max_error] if proc.stderr else f"Exit code: {proc.returncode}"
                            log.warning(f"[{node}] LLM auto-exec of {script_path} failed: {execution_error[:200]}")
                        else:
                            log.info(f"[{node}] LLM auto-exec of {script_path} succeeded, output: {execution_result[:200]}")
                    except _sp.TimeoutExpired:
                        execution_error = f"Execution timed out after {timeout_val if lang == 'python' else 60}s"
                        execution_result = ""
                    except Exception as exec_err:
                        execution_error = str(exec_err)[:500]
                        execution_result = ""

                    # Collect ALL new files created by the script
                    _MIME_MAP = {
                        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        ".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg",
                        ".jpeg": "image/jpeg", ".gif": "image/gif", ".svg": "image/svg+xml",
                        ".webp": "image/webp", ".html": "text/html", ".css": "text/css",
                        ".js": "application/javascript", ".json": "application/json",
                        ".csv": "text/csv", ".txt": "text/plain", ".md": "text/markdown",
                        ".py": "text/x-python", ".zip": "application/zip",
                    }
                    _BINARY_EXTS = {".pptx", ".xlsx", ".docx", ".pdf", ".png", ".jpg", ".jpeg",
                                    ".gif", ".webp", ".zip", ".tar", ".gz", ".rar",
                                    ".mp3", ".wav", ".mp4", ".avi", ".mkv", ".sqlite", ".db"}
                    max_file_size = self.config.task.max_file_size if hasattr(self, 'config') and hasattr(self.config, 'task') else 50_000_000

                    script_basename = _os.path.basename(script_path)
                    tmp_dir = self.config.task.tmp_dir if hasattr(self, 'config') and hasattr(self.config, 'task') else "/tmp"
                    for fname in _os.listdir(tmp_dir):
                        fpath = _os.path.join(tmp_dir, fname)
                        try:
                            st = _os.stat(fpath)
                            if st.st_mtime < pre_exec_time or st.st_size == 0 or st.st_size > max_file_size:
                                continue
                            if fname == script_basename or fname.startswith(".") or fname.endswith((".lock", "~")):
                                continue
                        except (OSError, PermissionError):
                            continue

                        _, fext = _os.path.splitext(fname)
                        fext_lower = fext.lower()
                        content_type = _MIME_MAP.get(fext_lower, "application/octet-stream")
                        is_binary = fext_lower in _BINARY_EXTS

                        try:
                            if is_binary:
                                with open(fpath, "rb") as bf:
                                    raw = bf.read()
                                content = _b64.b64encode(raw).decode("ascii")
                                result_files.append({
                                    "filename": fname, "content_type": content_type,
                                    "content": content, "size": len(raw), "encoding": "base64",
                                })
                            else:
                                with open(fpath, "r", encoding="utf-8", errors="replace") as tf:
                                    content = tf.read()
                                if "\x00" in content:
                                    with open(fpath, "rb") as bf:
                                        raw = bf.read()
                                    content = _b64.b64encode(raw).decode("ascii")
                                    result_files.append({
                                        "filename": fname, "content_type": content_type,
                                        "content": content, "size": len(raw), "encoding": "base64",
                                    })
                                else:
                                    result_files.append({
                                        "filename": fname, "content_type": content_type,
                                        "content": content, "size": len(content.encode("utf-8")),
                                    })
                            try:
                                _os.remove(fpath)
                            except Exception:
                                pass
                        except Exception as file_err:
                            log.warning(f"[{node}] Failed to collect output file {fname}: {file_err}")

                    # Clean up script
                    try:
                        _os.remove(script_path)
                    except Exception:
                        pass

            # Build result text
            result_text = f"[{node}] LLM ({model}) generated code for '{subject[:50]}' — {len(generated)} chars"
            if execution_result is not None:
                result_text += f"\n\n── Execution Output ──\n{execution_result}"
                if execution_error:
                    result_text += f"\n\n── Execution Error ──\n{execution_error}"
            elif execution_error:
                result_text += f"\n\n── Execution Error ──\n{execution_error}"

            # ── Ötletláda Implementation Pipeline: a generált kód bekerül a repóba ──
            # Ha a description-ben idea_id van (ötletláda-megvalósítás), a kód a repó
            # ideas/ mappájába mentődik + git commit — valódi beépítés, nem csak artifact.
            repo_integrated = False
            try:
                import json as _json_mod
                _desc_obj = None
                try:
                    _desc_obj = _json_mod.loads(desc_text) if desc_text else None
                except Exception:
                    _desc_obj = None
                _idea_id = None
                if isinstance(_desc_obj, dict):
                    _idea_id = _desc_obj.get("idea_id") or (_desc_obj.get("description") if isinstance(_desc_obj.get("description"), dict) else None)
                    if isinstance(_idea_id, dict):
                        _idea_id = _idea_id.get("idea_id")
                if not _idea_id and desc_text and "idea_id" in desc_text:
                    import re as _re2
                    _m = _re2.search(r'"idea_id"\s*:\s*"([^"]+)"', desc_text)
                    if not _m:
                        _m = _re2.search(r'\[idea_id:\s*([a-zA-Z0-9_]+)\]', desc_text)
                    if _m:
                        _idea_id = _m.group(1)
                if _idea_id and lang in ("python", "bash", "js", "javascript", "html"):
                    import os as _os2, subprocess as _sp2
                    _repo_root = _os2.path.dirname(_os2.path.abspath(__file__))
                    _ideas_dir = _os2.path.join(_repo_root, "ideas")
                    _os2.makedirs(_ideas_dir, exist_ok=True)
                    _slug = _re2.sub(r'[^a-zA-Z0-9_-]', '_', subject[:40]).strip('_') or "idea_impl"
                    _impl_ext = {"python": "py", "bash": "sh", "js": "js", "javascript": "js", "html": "html"}.get(lang, "py")
                    _impl_name = f"{_idea_id}_{_slug}.{_impl_ext}"
                    _impl_path = _os2.path.join(_ideas_dir, _impl_name)
                    with open(_impl_path, "w", encoding="utf-8") as _f:
                        _f.write(cleaned if lang in ("python", "bash") else generated)
                    # Git commit a repóban
                    _git = _sp2.run(["git", "add", "-A", "ideas/"], cwd=_repo_root, capture_output=True, text=True, timeout=15)
                    _git = _sp2.run(
                        ["git", "commit", "-m", f"feat(idea): {_idea_id} implementáció — ötletláda pipeline\n\nGenerálta: {node} ({model})\nSubject: {subject[:100]}"],
                        cwd=_repo_root, capture_output=True, text=True, timeout=15,
                    )
                    if _git.returncode == 0:
                        repo_integrated = True
                        result_text += f"\n\n✅ Repóba integrálva: ideas/{_impl_name} (git commit)"
                        log.info(f"[{node}] Idea {str(_idea_id)[:16]} implemented → ideas/{_impl_name}")
                    else:
                        # Már commitolva van / nincs változás
                        if "nothing to commit" in (_git.stdout or "") + (_git.stderr or ""):
                            repo_integrated = True
                            result_text += f"\n\n✅ Repóban: ideas/{_impl_name}"
                        else:
                            log.warning(f"[{node}] Idea git commit failed: {(_git.stderr or '')[:200]}")
            except Exception as _integ_err:
                log.warning(f"[{node}] Idea repo-integration failed: {_integ_err}")

            files_list = [{"filename": filename,
                            "content_type": "text/plain", "content": generated,
                            "size": len(generated.encode("utf-8"))}]
            files_list.extend(result_files)

            return {
                "result": result_text,
                "files": files_list,
                "context_updates": {"task_type": "code_generation", "language": ext, "model": model, "llm": True,
                                    "auto_executed": execution_result is not None,
                                    "execution_ok": execution_error is None or execution_error == ""},
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

        # ── Auto-execute Python/bash scripts ──
        execution_result = None
        execution_error = None
        result_files = []

        if lang in ("python", "bash") and save_path:
            import subprocess as _sp
            import os as _os
            import time as _time
            import base64 as _b64

            # Record timestamp BEFORE execution to find new files
            pre_exec_time = _time.time()

            # Determine correct extension and script path
            ext = ".py" if lang == "python" else ".sh"
            script_path = f"/tmp/a2a_task_{node}_{now.strftime('%Y%m%d_%H%M%S')}{ext}"
            try:
                with open(script_path, "w", encoding="utf-8") as f:
                    f.write(code)
                if lang == "bash":
                    _os.chmod(script_path, 0o755)
            except Exception:
                script_path = save_path

            try:
                if lang == "python":
                    venv_python = _os.path.expanduser(self.config.task.venv_python) if hasattr(self, 'config') and hasattr(self.config, 'task') else _os.path.expanduser("~/.hermes/scripts/a2a_mesh/.venv/bin/python")
                    python_bin = venv_python if _os.path.exists(venv_python) else "python3"
                    proc = _sp.run([python_bin, script_path], capture_output=True, text=True, timeout=self.config.task.python_timeout if hasattr(self, 'config') and hasattr(self.config, 'task') else 120, cwd=self.config.task.tmp_dir if hasattr(self, 'config') and hasattr(self.config, 'task') else "/tmp")
                else:  # bash
                    proc = _sp.run(["bash", script_path], capture_output=True, text=True, timeout=self.config.task.bash_timeout if hasattr(self, 'config') and hasattr(self.config, 'task') else 60, cwd=self.config.task.tmp_dir if hasattr(self, 'config') and hasattr(self.config, 'task') else "/tmp")

                max_output = self.config.task.max_output_chars if hasattr(self, 'config') and hasattr(self.config, 'task') else 5000
                max_error = self.config.task.max_error_chars if hasattr(self, 'config') and hasattr(self.config, 'task') else 2000
                execution_result = proc.stdout[:max_output] if proc.stdout else ""
                if proc.returncode != 0:
                    execution_error = proc.stderr[:max_error] if proc.stderr else f"Exit code: {proc.returncode}"
                    log.warning(f"Auto-execution of {script_path} failed: {execution_error[:200]}")
                else:
                    log.info(f"Auto-execution of {script_path} succeeded, output: {execution_result[:200]}")
            except _sp.TimeoutExpired:
                execution_error = f"Execution timed out after {120 if lang == 'python' else 60}s"
                execution_result = ""
            except Exception as exec_err:
                execution_error = str(exec_err)[:500]
                execution_result = ""

            # ── Collect ALL new files created by the script ──
            # Scan /tmp for any file modified/created after script start
            _MIME_MAP = {
                ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg", ".gif": "image/gif", ".svg": "image/svg+xml",
                ".webp": "image/webp", ".ico": "image/x-icon",
                ".html": "text/html", ".css": "text/css", ".js": "application/javascript",
                ".json": "application/json", ".xml": "application/xml",
                ".csv": "text/csv", ".txt": "text/plain", ".md": "text/markdown",
                ".py": "text/x-python", ".sh": "application/x-sh",
                ".zip": "application/zip", ".tar": "application/x-tar",
                ".gz": "application/gzip", ".rar": "application/vnd.rar",
                ".mp3": "audio/mpeg", ".wav": "audio/wav", ".mp4": "video/mp4",
                ".avi": "video/x-msvideo", ".mkv": "video/x-matroska",
            }
            _BINARY_EXTS = {".pptx", ".xlsx", ".docx", ".pdf", ".png", ".jpg", ".jpeg",
                            ".gif", ".webp", ".ico", ".zip", ".tar", ".gz", ".rar",
                            ".mp3", ".wav", ".mp4", ".avi", ".mkv", ".sqlite", ".db"}

            script_basename = _os.path.basename(script_path)
            for fname in _os.listdir("/tmp"):
                fpath = _os.path.join("/tmp", fname)
                try:
                    st = _os.stat(fpath)
                    if st.st_mtime < pre_exec_time or st.st_size == 0 or st.st_size > (self.config.task.max_file_size if hasattr(self, 'config') and hasattr(self.config, 'task') else 50_000_000):
                        continue
                    if fname == script_basename or fname.startswith(".") or fname.endswith((".lock", "~")):
                        continue
                except (OSError, PermissionError):
                    continue

                _, fext = _os.path.splitext(fname)
                fext_lower = fext.lower()
                content_type = _MIME_MAP.get(fext_lower, "application/octet-stream")
                is_binary = fext_lower in _BINARY_EXTS

                try:
                    if is_binary:
                        with open(fpath, "rb") as bf:
                            raw = bf.read()
                        content = _b64.b64encode(raw).decode("ascii")
                        result_files.append({
                            "filename": fname, "content_type": content_type,
                            "content": content, "size": len(raw), "encoding": "base64",
                        })
                    else:
                        with open(fpath, "r", encoding="utf-8", errors="replace") as tf:
                            content = tf.read()
                        if "\x00" in content:
                            with open(fpath, "rb") as bf:
                                raw = bf.read()
                            content = _b64.b64encode(raw).decode("ascii")
                            result_files.append({
                                "filename": fname, "content_type": content_type,
                                "content": content, "size": len(raw), "encoding": "base64",
                            })
                        else:
                            result_files.append({
                                "filename": fname, "content_type": content_type,
                                "content": content, "size": len(content.encode("utf-8")),
                            })
                    try:
                        _os.remove(fpath)
                    except Exception:
                        pass
                except Exception as file_err:
                    log.warning(f"Failed to collect output file {fname}: {file_err}")

            # Clean up script
            try:
                _os.remove(script_path)
            except Exception:
                pass

        # Build result text
        result_text = f"[{node}] Generated {lang} code for '{task_summary[:50]}' at {now.isoformat()}"
        if execution_result is not None:
            result_text += f"\n\n── Execution Output ──\n{execution_result}"
            if execution_error:
                result_text += f"\n\n── Execution Error ──\n{execution_error}"
        elif execution_error:
            result_text += f"\n\n── Execution Error ──\n{execution_error}"

        # Build files list — source code + execution output files
        files_list = [{"filename": f"generated_{lang}_{node}_{now.strftime('%Y%m%d_%H%M%S')}.{lang}",
                        "content_type": "text/plain", "content": code,
                        "size": len(code.encode("utf-8"))}]
        files_list.extend(result_files)

        return {
            "result": result_text,
            "files": files_list,
            "context_updates": {"task_type": "code_generation", "language": lang,
                                "save_path": save_path,
                                "auto_executed": execution_result is not None,
                                "execution_ok": execution_error is None or execution_error == ""},
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

        # Stop GossipSub
        await self.router._gossipsub.stop()

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
        if self.config.ssh_tunnel.enabled:
            await self._ssh_tunnel_transport.stop()
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
        # Extract version from discovery info (UDP broadcast or mDNS)
        peer_version = node_info.get("version") or None
        peer = self.peer_discovery.add_peer(
            name=name,
            host=host,
            port=port,
            role="router",
            p2p_port=port,
            health_port=node_info.get("health_port") or self.config.health_port,
            version=peer_version,
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

        # Check if recipient is online — if not, queue for later.
        # NOTE: skip the check for empty/None recipients (heartbeat forwards) and
        # non-broadcast forwarding paths — only queue for real directed recipients.
        if (
            not message.is_broadcast()
            and message.recipient
            and message.recipient not in ("", "*", "broadcast")
            and await self.offline_queue.is_node_online(message.recipient) is False
        ):
            log.info(f"Recipient {message.recipient} is offline — queuing message")
            await self.offline_queue.enqueue(message)
            return SendResult(transport="offline_queue", success=True, error="Queued for offline delivery")

        # Track for ACK (non-broadcast only)
        # Skip self-directed messages: no peer will ACK them, so tracking
        # always exhausts retries → spurious "ACK failed → <self>" warnings
        # (~118/day from Dream Engine self-reports and dashboard self-sends).
        if (
            not message.is_broadcast()
            and message.type != MSG_TYPE_HEARTBEAT
            and message.recipient not in ("", "*", "broadcast")
            and message.recipient != self.node_name
        ):
            self.ack_manager.track(message)

        # Also persist to PG for reliability
        await self._persist_message(message)

        return await self.router.send(message)

    async def send_direct(self, recipient: str, msg_type: str,
                          payload: dict, priority: int = 5) -> SendResult:
        """Convenience method to send a directed message.
        
        Marveen-inspired: logs to message_router for distributed tracing.
        """
        msg = A2AMessage.create(
            sender=self.node_name,
            recipient=recipient,
            msg_type=msg_type,
            payload=payload,
            priority=priority,
        )
        # Trace via message_router
        try:
            trace_id = payload.get("trace_id") if isinstance(payload, dict) else None
            self._msg_router_create(
                self.node_name, recipient,
                f"[{msg_type}] {str(payload.get('text', payload.get('subject', '')))[:100]}",
                msg_type=msg_type,
                trace_id=trace_id,
            )
        except Exception:
            pass
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

    async def _on_p2p_heartbeat(self, peer_name: str, version: str, provider_status: dict = None):
        """Callback when a P2P heartbeat is received — update peer version + provider health."""
        if not self.peer_discovery:
            return
        # Late-discovered peers must be allowed into the health scorer
        # (valid_node_names is seeded at startup when discovery may be empty).
        try:
            if hasattr(self, 'router') and self.router and hasattr(self.router, '_health_scorer'):
                hs = self.router._health_scorer
                if peer_name not in hs.valid_node_names:
                    hs.valid_node_names.add(peer_name)
        except Exception:
            pass
        peer = self.peer_discovery.get_peer(peer_name)
        if peer:
            if not peer.version or peer.version == 'unknown' or peer.version == '1.0.0':
                peer.version = version
                log.info(f"Updated peer {peer_name} version from heartbeat: {version}")
            elif peer.version != version:
                peer.version = version
                log.info(f"Updated peer {peer_name} version from heartbeat: {version} (was {peer.version})")
        
        # Feed provider status into health scorer
        if provider_status and hasattr(self, 'router') and hasattr(self.router, '_health_scorer'):
            try:
                self.router._health_scorer.update_provider_status(peer_name, provider_status)
            except Exception as e:
                log.debug(f"Provider status → health scorer failed: {e}")

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

    async def _on_transport_peer_connected(self, peer_name: str):
        """Callback when any transport (P2P, SSH tunnel) establishes a connection.
        Delegates to the P2P peer connected handler for registry + skills sync."""
        log.info(f"Transport peer_connected callback: {peer_name}")
        await self.debug_log("INFO", "transport", f"Peer {peer_name} connected via SSH tunnel")
        # Reuse the P2P peer connected logic for registry registration
        await self._on_p2p_peer_connected(peer_name)

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
            # Register peer with GossipSub for efficient topic-based broadcast
            if hasattr(self, 'router') and hasattr(self.router, 'register_gossipsub_peer'):
                self.router.register_gossipsub_peer(peer_name, topics={'mesh', 'broadcast', 'diagnostic'})
                log.debug(f"GossipSub: registered peer {peer_name} for topic-based broadcast")
        else:
            # Peer connected via P2P but not in peer_discovery — create entry from transport data
            # This happens when static config peers get pruned or when incoming connections
            # arrive before discovery has tracked the peer.
            peer_addr = self._p2p_transport._peer_addresses.get(peer_name) if self._p2p_transport else None
            if peer_addr:
                host, port_str = peer_addr.rsplit(":", 1)
                port = int(port_str)
                log.info(f"P2P peer_connected callback: creating discovery entry for {peer_name} from P2P transport ({peer_addr})")
                peer = self.peer_discovery.add_peer(
                    name=peer_name, host=host, p2p_port=port,
                    role="router", health_port=port + 5,
                )
                peer.p2p_available = True
            else:
                # No address info — create minimal entry so discovery tracks it
                log.info(f"P2P peer_connected callback: creating minimal discovery entry for {peer_name}")
                peer = self.peer_discovery.add_peer(
                    name=peer_name, host="unknown", p2p_port=8645,
                    role="router", health_port=8650,
                )
                peer.p2p_available = True
            # Register with GossipSub for efficient topic-based broadcast
            if hasattr(self, 'router') and hasattr(self.router, 'register_gossipsub_peer'):
                self.router.register_gossipsub_peer(peer_name, topics={'mesh', 'broadcast', 'diagnostic'})
                log.debug(f"GossipSub: registered peer {peer_name} for topic-based broadcast (recovered from P2P)")

        # Cancel pending grace-period offline broadcast — peer reconnected
        pending_task = self._peer_offline_grace_tasks.pop(peer_name, None)
        if pending_task and not pending_task.done():
            pending_task.cancel()
            log.info(f"Cancelled pending offline broadcast for {peer_name} — peer reconnected during grace period")

        # SSH key auto-sync: exchange pubkeys with the (re)connected peer
        if getattr(self, 'ssh_key_sync', None):
            asyncio.create_task(self.ssh_key_sync.on_peer_connected(peer_name))

        # If we previously broadcast peer_offline for this peer, send peer_online to restore mesh state.
        # This handles the case where the grace period already expired (offline was broadcast)
        # but the peer reconnected shortly after.
        if peer_name in self._peer_offline_broadcasted:
            import time as _t
            now = _t.time()
            last_online = self._peer_online_debounce.get(peer_name, 0)
            if now - last_online >= 30:  # debounce: min 30s between peer_online broadcasts
                self._peer_online_debounce[peer_name] = now
                self._peer_offline_broadcasted.discard(peer_name)
                log.info(f"Peer {peer_name} reconnected after offline broadcast — sending peer_online to mesh")
                try:
                    online_msg = A2AMessage.create(
                        sender=self.node_name,
                        recipient="broadcast",
                        msg_type="peer_online",
                        payload={
                            "type": "peer_online",
                            "peer_name": peer_name,
                            "source": self.node_name,
                            "timestamp": now,
                        },
                        priority=7,
                    )
                    asyncio.create_task(self.router.send(online_msg))
                    log.info(f"Broadcast peer_online for {peer_name}")
                except Exception as e:
                    log.error(f"Failed to broadcast peer_online for {peer_name}: {e}")
            else:
                self._peer_offline_broadcasted.discard(peer_name)
                log.debug(f"Peer {peer_name} reconnected — debouncing peer_online (last sent {now - last_online:.0f}s ago)")
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
        # Use full skills + capabilities from registry (auto-built in _auto_register_self)
        skills = list(getattr(self.config, 'skills', []) or [])
        # Get capabilities from registry card (includes workflow + transport + role caps)
        reg_card = self.dashboard.registry.get(self.node_name) if hasattr(self, 'dashboard') and hasattr(self.dashboard, 'registry') else None
        full_caps = list(getattr(reg_card, 'capabilities', []) or []) if reg_card else list(getattr(self.config, 'capabilities', []) or [])
        if skills or full_caps:
            sent_via = []
            # Try P2P transport
            if self._p2p_transport and self._p2p_transport.is_available():
                try:
                    import uuid
                    skills_msg = A2AMessage(
                        id=str(uuid.uuid4()),
                        sender=self.node_name,
                        recipient=peer_name,
                        payload={
                            "type": "skills_announcement",
                            "skills": skills,
                            "capabilities": full_caps,
                            "version": self._resolved_version,
                        },
                        type="skills_announcement",
                        priority=5,
                    )
                    await self._p2p_transport.send(skills_msg)
                    sent_via.append("p2p")
                    log.info(f"P2P skill announcement sent to {peer_name}: {[s.get('id','?') for s in skills]}")
                except Exception as e:
                    log.debug(f"P2P skills announcement failed for {peer_name}: {e}")
            # Store via PG for offline resilience, but skip NOTIFY if P2P already delivered.
            # P2P already sent this directly to the peer — PG NOTIFY would cause
            # duplicate delivery on all nodes, inflating dedup hit rate from ~0% to ~50%.
            if hasattr(self, '_pg_transport') and self._pg_transport and self._pg_transport.is_available():
                try:
                    import uuid
                    pg_skills_msg = A2AMessage(
                        id=str(uuid.uuid4()),
                        sender=self.node_name,
                        recipient="broadcast",
                        payload={
                            "type": "skills_announcement",
                            "skills": skills,
                            "capabilities": full_caps,
                            "version": self._resolved_version,
                        },
                        type="skills_announcement",
                        priority=5,
                    )
                    p2p_delivered = "p2p" in sent_via
                    await self._pg_transport.send(pg_skills_msg, notify=not p2p_delivered)
                    sent_via.append("pg_store" if p2p_delivered else "pg")
                    log.info(f"PG skill announcement {'store-only' if p2p_delivered else 'full'} for {peer_name}")
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

        # Remove peer from GossipSub
        if hasattr(self, 'router') and hasattr(self.router, 'remove_gossipsub_peer'):
            self.router.remove_gossipsub_peer(peer_name)

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

            # Grace period expired — but re-check: is the peer actually unreachable?
            # The P2P connection may have been re-established from the peer's side
            # (incoming connection) without triggering our _on_p2p_peer_connected callback.
            # If our own peer_discovery shows the peer as p2p_available and recently
            # seen, broadcasting peer_offline would be a false alarm.
            now = time.time()
            if self.peer_discovery:
                peer = self.peer_discovery.get_peer(peer_name)
                if peer and peer.p2p_available and peer.last_seen > 0 and (now - peer.last_seen) < 120:
                    log.info(
                        f"Peer {peer_name} grace expired but P2P link is active "
                        f"(last_seen {now - peer.last_seen:.0f}s ago) — skipping false offline broadcast"
                    )
                    self._peer_offline_grace_tasks.pop(peer_name, None)
                    return
            last_broadcast = self._peer_offline_debounce.get(peer_name, 0)
            if now - last_broadcast < 60:
                log.info(f"Peer {peer_name} offline grace expired — debouncing (last broadcast {now - last_broadcast:.0f}s ago)")
                self._peer_offline_grace_tasks.pop(peer_name, None)
                return

            self._peer_offline_debounce[peer_name] = now
            self._peer_offline_broadcasted.add(peer_name)
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
        Sends our skills announcement via PG broadcast — rate limited to max 1 per 60s.
        Also registers the peer's HTTP URL for the HTTP transport."""
        # Register peer HTTP URL for HTTP transport fallback
        try:
            peer = self.peer_discovery._peers.get(peer_name)
            if peer and peer.host:
                peer_http = f"http://{peer.host}:8650"
                if hasattr(self, '_http_transport') and hasattr(self._http_transport, 'register_peer_url'):
                    self._http_transport.register_peer_url(peer_name, peer_http)
        except Exception as e:
            log.debug(f"Peer HTTP URL registration failed for {peer_name}: {e}")

        import time as _time
        now = _time.time()
        if now - self._last_skills_announcement < 60:
            log.debug(f"Skipping skills announcement — rate limited (last sent {now - self._last_skills_announcement:.0f}s ago)")
            return
        self._last_skills_announcement = now
        skills = list(getattr(self.config, 'skills', []) or [])
        # Get capabilities from registry card (includes workflow + transport + role caps)
        reg_card = self.dashboard.registry.get(self.node_name) if hasattr(self, 'dashboard') and hasattr(self.dashboard, 'registry') else None
        capabilities = list(getattr(reg_card, 'capabilities', []) or []) if reg_card else list(getattr(self.config, 'capabilities', []) or [])
        if not skills and not capabilities:
            return
        
        # Use router broadcast for skills announcement — this applies smart dedup
        # (P2P first + PG store-only) instead of direct PG NOTIFY which causes
        # duplicate delivery on all nodes (~50% dedup hit rate).
        # If P2P is available, it delivers in real-time and PG only stores for
        # offline resilience (notify=False). Without P2P, PG NOTIFY delivers.
        try:
            import uuid
            skills_msg = A2AMessage(
                id=str(uuid.uuid4()),
                sender=self.node_name,
                recipient="broadcast",
                payload={
                    "type": "skills_announcement",
                    "skills": skills,
                    "version": self._resolved_version,
                    "capabilities": capabilities,
                },
                type="skills_announcement",
                priority=5,
            )
            result = await self.router.send(skills_msg)
            if result.success:
                log.info(f"Skills announcement broadcast via router on peer discovery: {[s.get('id','?') for s in skills]}")
            else:
                log.warning(f"Skills announcement broadcast failed: {result.error}")
        except Exception as e:
            log.warning(f"Skills announcement broadcast failed on peer discovery: {e}")

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
        
        # Add skills as capabilities (skills are advertised but not used for routing)
        skills = list(getattr(self.config, 'skills', []) or [])
        for skill in skills:
            if isinstance(skill, dict):
                skill_id = skill.get('id', '')
                if skill_id and skill_id not in capabilities:
                    capabilities.append(skill_id)
            elif isinstance(skill, str) and skill not in capabilities:
                capabilities.append(skill)
        
        # Add common workflow capabilities (all nodes can execute delegated tasks)
        for cap in ["task_execution", "web_search", "summarization", "data_analysis", "code_generation"]:
            if cap not in capabilities:
                capabilities.append(cap)
        
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

        # Update PG with full capabilities (async, fire-and-forget)
        import asyncio as _aio
        _aio.ensure_future(self._update_pg_capabilities(card.capabilities))

    async def _update_pg_capabilities(self, capabilities: list):
        """Update PG mesh_nodes.capabilities with the full list from registry."""
        try:
            if self._pg_pool and self._pg_pool.is_connected():
                import json as _json
                await self._pg_pool.execute(
                    "UPDATE mesh.mesh_nodes SET capabilities = $1 WHERE node_name = $2",
                    _json.dumps(capabilities),
                    self.node_name,
                )
                log.info(f"PG capabilities updated for {self.node_name}: {len(capabilities)} caps")
        except Exception as e:
            log.warning(f"Failed to update PG capabilities: {e}")

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
            "version": self._resolved_version or "",
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
                    "ssh_tunnel": self._ssh_tunnel_transport.is_available() if self.config.ssh_tunnel.enabled else False,
                },
                "ssh_tunnel": self._ssh_tunnel_transport.get_peer_status() if self.config.ssh_tunnel.enabled else {},
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

        app = web.Application(
            # 60MB upload limit — aiohttp default is 1MB, which rejected real
            # chat file uploads with 413 (server handler checks 50MB itself)
            client_max_size=60 * 1024 * 1024,
        )
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

        Retries up to 5 times with 2s delay — handles PG startup race condition
        where the node starts before PG is fully ready (e.g. after deploy).
        """
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self._pg_pool = AsyncDBPool(self.config)
                if not await self._pg_pool.connect():
                    log.error(f"Failed to create asyncpg connection pool (attempt {attempt + 1}/{max_retries})")
                    self._pg_pool = None
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                    continue
                log.info("AsyncPG connection pool established")
                # Initialize offline queue pool
                await self.offline_queue.init_pool(self._pg_pool)
                await self.offline_queue.ensure_table()
                # Attach offline queue to the router so _flush_offline_queue()
                # (self-heal step 15 + transport recovery) actually finds it.
                # Previously set_offline_queue() was never called, leaving
                # router._offline_queue = None — flush was a silent no-op.
                if getattr(self, "router", None) is not None:
                    self.router.set_offline_queue(self.offline_queue)
                return True
            except Exception as e:
                log.error(f"AsyncPG connection pool failed (attempt {attempt + 1}/{max_retries}): {e}")
                self._pg_pool = None
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)

        log.error(f"PG write connection failed after {max_retries} retries — running in P2P-only mode")
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
                     routing_mode, src_addr, dst_addr, status, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'sent', NOW())
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

        # Get capabilities from registry card (auto-built in _auto_register_self), fallback to config
        reg_card = self.dashboard.registry.get(self.node_name) if hasattr(self, 'dashboard') and hasattr(self.dashboard, 'registry') else None
        if reg_card and reg_card.capabilities:
            capabilities = list(reg_card.capabilities)
        else:
            # Build full capabilities list (same as _auto_register_self)
            capabilities = list(getattr(self.config, 'capabilities', []) or [
                "a2a_messaging", "file_transfer"
            ])
            # Add workflow capabilities
            for cap in ["task_execution", "web_search", "summarization", "data_analysis", "code_generation"]:
                if cap not in capabilities:
                    capabilities.append(cap)
            # Add role-based capabilities
            if self.role == NodeRole.COORDINATOR:
                capabilities.extend(["coordinator", "dashboard", "registry"])
            # Add transport + health caps
            capabilities.append("p2p_transport")
            capabilities.append("pg_transport")
            capabilities.append("health_monitor")
        capabilities = list(set(c for c in capabilities if isinstance(c, (str, int, float, tuple))))

        # Get skills from config — these are the node's own skills (not from plugins)
        config_skills = list(getattr(self.config, 'skills', []) or [])
        db_skills = []
        for s in config_skills:
            if isinstance(s, dict):
                db_skills.append(s.get('id', str(s)))
            elif isinstance(s, str):
                db_skills.append(s)
        # Merge with plugin-announced skills (if already loaded)
        if hasattr(self, 'plugin_loader') and self.plugin_loader.plugins:
            for name, plugin in self.plugin_loader.plugins.items():
                for cap in plugin.capabilities:
                    sid = f"{name}_{cap}"
                    if sid not in db_skills:
                        db_skills.append(sid)

        # Determine host address for other nodes to connect to
        # Use advertise_ip for Docker/HA containers (host IP instead of container IP)
        try:
            host_ip = self._get_advertise_ip()
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
                     pg_available, p2p_available, http_available, capabilities, skills, version)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, $9, $10, $11, $12, $13, $14, $15, $16)
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
                    skills = EXCLUDED.skills,
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
                json.dumps(db_skills, ensure_ascii=True),
                self._resolved_version,
            )
            log.info(f"Registered node {self.node_name} at {host_ip}:{p2p_port} in mesh with {len(db_skills)} skills")
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

        # Provider health check for PG storage
        provider_status = {
            "version": self._resolved_version or "",}
        try:
            # Try multiple import strategies
            try:
                from core.provider_health import check_provider_health
                provider_status = await asyncio.to_thread(check_provider_health, self.node_name)
                log.info(f"Provider health (PG) via import: {provider_status}")
            except ImportError:
                import importlib.util as _ilu
                # Try relative to this file
                _this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.path.dirname(os.path.abspath(inspect.getfile(self.__class__))) if "inspect" in dir() else ""
                if not _this_dir:
                    import inspect
                    _this_dir = os.path.dirname(os.path.abspath(inspect.getfile(self.__class__)))
                _ph_path = os.path.join(_this_dir, "core", "provider_health.py")
                if os.path.exists(_ph_path):
                    _spec = _ilu.spec_from_file_location("provider_health_pg", _ph_path)
                    _mod = _ilu.module_from_spec(_spec)
                    _spec.loader.exec_module(_mod)
                    provider_status = await asyncio.to_thread(_mod.check_provider_health, self.node_name)
                    log.info(f"Provider health (PG) via importlib: {provider_status}")
            else:
                if not isinstance(provider_status, dict) or "primary" not in provider_status:
                    log.warning(f"Provider health returned unexpected: {provider_status}")
        except Exception as e:
            log.error(f"Provider health check (PG) failed: {e}", exc_info=True)

        # Augment provider_status with this node's own live model profile so the
        # context gate can read per-node context_length/max_turns/model dynamically.
        if isinstance(provider_status, dict):
            try:
                from core.context_gate import resolve_local_model_info
                local = await asyncio.to_thread(resolve_local_model_info)
                if isinstance(local, dict) and local.get("model", "unknown") != "unknown":
                    provider_status["model"] = local
            except Exception as _mpe:
                log.debug(f"Local model profile augment failed: {_mpe}")

        try:
            await self._pg_pool.execute("""
                UPDATE mesh.mesh_nodes SET 
                    last_heartbeat = NOW(), 
                    status = 'active',
                    host = $1,
                    health_port = $2,
                    p2p_port = $8,
                    pg_available = $3,
                    p2p_available = $4,
                    http_available = $5,
                    capabilities = $7
                WHERE node_name = $6
            """,
                self._get_advertise_ip(),
                getattr(self.config, 'health_port', 8650),
                bool(self._pg_pool and self._pg_pool.is_connected()),
                self._p2p_transport.is_available() if hasattr(self, "_p2p_transport") else False,
                self._http_transport.is_available() if hasattr(self, "_http_transport") else False,
                self.node_name,
                json.dumps(list(getattr(self.config, 'capabilities', []) or [])),
                self.config.p2p.listen_port,
            )
            # Separate update for provider_status (backward compatible)
            if provider_status:
                try:
                    await self._pg_pool.execute(
                        "UPDATE mesh.mesh_nodes SET provider_status = $1 WHERE node_name = $2",
                        json.dumps(provider_status),
                        self.node_name,
                    )
                except Exception:
                    pass  # Column may not exist on older nodes
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
                            # ── Multi-hop relay (ZigBee concept) ──────────────────
                            # If this message carries relay_to and WE are not the
                            # final destination, forward it toward relay_to and
                            # skip local processing. Every channel (P2P, SSH-tunnel)
                            # thus reaches the coordinator even through chained routers.
                            _relay_final = getattr(msg, 'relay_to', '') or ''
                            if (
                                _relay_final
                                and _relay_final != self.node_name
                                and msg.type not in (MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK)
                            ):
                                # Prevent relay loops: TTL + path check
                                if msg.ttl <= 0 or self.node_name in (msg.path or []):
                                    log.warning(f"Relay: dropping {msg.id[:8]} (ttl={msg.ttl}, loop) final={_relay_final}")
                                    continue
                                log.info(f"Relay: forwarding {msg.id[:8]} to final destination {_relay_final}")
                                fwd = msg.add_hop(self.node_name)
                                fwd.recipient = _relay_final  # final destination
                                fwd.relay_to = ""  # let router pick the next hop fresh
                                asyncio.create_task(self.router.send(fwd))
                                continue
                            if _relay_final == self.node_name:
                                # We are the final destination — clear relay header, process locally
                                msg.relay_to = ""
                                log.info(f"Relay: message {msg.id[:8]} arrived via relay (hops={msg.hop_count})")

                            # Skip own messages (loop prevention) — except directives and broadcast chat
                            _is_broadcast_chat = False
                            try:
                                _p = msg.payload
                                if isinstance(_p, str):
                                    import json as _j2
                                    _p = _j2.loads(_p)
                                if isinstance(_p, dict) and _p.get("chat_type") == "broadcast":
                                    _is_broadcast_chat = True
                            except Exception:
                                pass
                            if msg.sender == self.node_name and msg.type not in ("directive",) and not _is_broadcast_chat:
                                continue

                            # Skip empty payloads (wake-agent noise, not real messages)
                            payload = msg.payload if hasattr(msg, 'payload') else None
                            if payload is None or (isinstance(payload, dict) and len(payload) == 0) or (isinstance(payload, str) and payload.strip() in ('', '{}')):
                                log.debug(f"Skipping empty payload message {msg.id[:8]} from {msg.sender}")
                                continue

                            # ── Per-user chat: extract chat_username BEFORE untrusted framing ──
                            # The framing converts payload to string, breaking JSON parsing.
                            # So we extract chat_username from the original dict payload first.
                            _chat_user = None
                            _chat_reply_text = ""
                            if msg.type in ("a2a_message", "agent_reply"):
                                # Try dict payload first, then parse string
                                _p = payload
                                log.info(f"🔍 Chat DM debug: msg.id={msg.id[:8]} msg.type={msg.type} payload_type={type(_p).__name__} payload_preview={str(_p)[:200]}")
                                if isinstance(_p, str):
                                    try:
                                        import json as _j
                                        _p = _j.loads(_p)
                                    except Exception:
                                        _p = None
                                if isinstance(_p, dict):
                                    _chat_user = _p.get("chat_username")
                                    _chat_type = _p.get("chat_type", "")
                                    if _chat_user:
                                        _chat_reply_text = _p.get("text", "") or _p.get("content", "")
                                    elif _chat_type == "agent_dm":
                                        # Agent-to-agent DM — always accept, set chat_user to sender
                                        _chat_user = _p.get("sender_display", msg.sender or "")
                                        _chat_reply_text = _p.get("text", "") or _p.get("content", "")

                            # ── Untrusted framing for peer messages ──
                            # Wrap only user-facing message types (a2a_message, agent_reply)
                            # Internal protocol messages (ACK, heartbeat, skills_announcement,
                            # memory_sync, file_transfer, diagnostic_report) keep their original
                            # dict/bytes payload — wrapping them would break protocol parsing.
                            if msg.type in ("a2a_message", "agent_reply"):
                                from .core.prompt_safety import wrap_trusted_peer
                                trust = self._get_peer_trust_level(msg.sender)
                                _is_agent_dm = False
                                try:
                                    _p2 = payload
                                    if isinstance(_p2, str):
                                        import json as _j3
                                        _p2 = _j3.loads(_p2)
                                    if isinstance(_p2, dict) and _p2.get("chat_type") == "agent_dm":
                                        _is_agent_dm = True
                                except Exception:
                                    pass
                                if trust == "full":
                                    msg.payload = wrap_trusted_peer(msg.sender, str(msg.payload) if not isinstance(msg.payload, str) else msg.payload)
                                elif trust == "limited":
                                    msg.payload = wrap_trusted_peer(msg.sender, str(msg.payload) if not isinstance(msg.payload, str) else msg.payload) + "\n\n⚠️ LIMITED TRUST — verify all claims."
                                elif _is_agent_dm:
                                    # Agent-to-agent DM — always accept (mesh-internal)
                                    msg.payload = wrap_trusted_peer(msg.sender, str(msg.payload) if not isinstance(msg.payload, str) else msg.payload)
                                    log.debug(f"Agent DM accepted from {msg.sender} (trust={trust})")
                                elif _chat_user:
                                    # Chat DM from dashboard — always accept, wrap as trusted
                                    msg.payload = wrap_trusted_peer(msg.sender, str(msg.payload) if not isinstance(msg.payload, str) else msg.payload)
                                    log.debug(f"Chat DM accepted from {msg.sender} (trust={trust}, chat_user={_chat_user})")
                                else:
                                    log.warning(f"Rejected message from untrusted peer: {msg.sender}")
                                    continue
                                log.debug(f"Untrusted framing applied to {msg.type} {msg.id[:8]} from {msg.sender}")

                            # ── Per-user chat: trigger wake-agent (BEFORE router.receive) ──
                            # Auto-ack removed — the real LLM response arrives in 15-30s
                            # and serves as the natural acknowledgment.
                            # Debug-level only: at INFO this fired on EVERY inbound
                            # message (heartbeats, acks) — 23k lines per 200k on
                            # Nova, 722MB unbounded launchd stdout log.
                            log.debug(f"🔍 Chat check: msg.type={msg.type} _chat_user={_chat_user!r}")
                            # ── Anti-ping-pong: skip wake-agent for agent replies and agent DMs ──
                            # Agent-generated messages must NOT trigger new wake-agent calls
                            # on peer nodes — that creates infinite reply chains.
                            _skip_wake_types = ("agent_reply", "agent_dm", MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT,
                                                 "skills_announcement", "memory_sync")
                            if msg.type == "a2a_message" and _chat_user and msg.type not in _skip_wake_types:
                                # Check if sender is another mesh agent (not a human user)
                                _agent_senders = ("nova", "morzsa", "runa", "tor")
                                if msg.sender.lower() in _agent_senders:
                                    log.info(f"🔇 Skip wake-agent for agent→agent msg from {msg.sender} (anti-ping-pong)")
                                else:
                                    # ── @mention targeting: in broadcast, only wake if @mentioned ──
                                    _msg_text = ""
                                    try:
                                        _mp = msg.payload
                                        if isinstance(_mp, str):
                                            import json as _mj
                                            _mp = _mj.loads(_mp)
                                        if isinstance(_mp, dict):
                                            _msg_text = _mp.get("text", "") or ""
                                    except Exception:
                                        _msg_text = ""
                                    import re as _re_ment_rx
                                    _mentioned_here = [m.lower() for m in _re_ment_rx.findall(r"@(\w+)", _msg_text)]
                                    _is_broadcast_msg = (msg.recipient or "") in ("", "broadcast", "*")
                                    if _is_broadcast_msg and _mentioned_here and self.node_name.lower() not in _mentioned_here:
                                        log.info(f"🔇 Skip wake-agent: @{'@'.join(_mentioned_here)} mentioned, not me ({self.node_name})")
                                    else:
                                        try:
                                            asyncio.create_task(self._trigger_webhook(msg))
                                            _mention_note = " (@megszólított ÖN)" if _is_broadcast_msg and self.node_name.lower() in _mentioned_here else ""
                                            log.info(f"🔔 Wake-agent triggered for chat DM from {msg.sender}→user:{_chat_user}{_mention_note}")
                                        except Exception as e:
                                            log.warning(f"Wake-agent trigger failed: {e}")

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
                                if msg.type not in (MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT, "skills_announcement", "memory_sync", "diagnostic_report", "config_suggestion", "agent_reply", "agent_dm", "peer_offline", "peer_online",
                                                    "vault_request", "vault_share", "vault_response", "idea_submit", "idea_submit_ack",
                                                    "idea_vote", "idea_vote_ack", "ssh_key_sync", "ssh_key_bundle"):
                                    asyncio.create_task(self._trigger_webhook(msg))

                                # Critical mesh protocol messages must always go to handlers
                                # regardless of priority level (file_transfer, memory_sync, diagnostic)
                                if msg.type in ("file_transfer", "memory_sync", "diagnostic_report", "config_suggestion", "peer_offline", "peer_online",
                                                "vault_request", "vault_share", "vault_response", "idea_submit", "idea_submit_ack",
                                                "idea_vote", "idea_vote_ack"):
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

                            # Process forwarded messages — some broadcasts may be forwarded by the router
                            # (skills_announcement is now broadcast, processed normally)
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
                        log.warning(f"Receive error on {transport_name}: {e}", exc_info=True)

                await asyncio.sleep(0.1)  # 100ms polling interval

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Receive loop error: {e}")
                await asyncio.sleep(1)

    def _get_peer_trust_level(self, peer_name):
        """Get trust level for a peer. Returns 'full', 'limited', or 'none'."""
        try:
            from .core.team_trust import get_trust_level
            return get_trust_level(self.node_name, peer_name)
        except Exception:
            return "full"  # Default: full trust (mesh internal)

    async def _auto_advertise_skills(self):
        """Auto-advertise config skills to mesh_skills table on startup.
        
        For each skill in config.skills, upserts an entry in mesh.mesh_skills
        so the skill marketplace has accurate data without manual API calls.
        """
        if not self._pg_pool:
            return
        config_skills = list(getattr(self.config, 'skills', []) or [])
        if not config_skills:
            return
        
        advertised = 0
        for s in config_skills:
            if isinstance(s, dict):
                skill_name = s.get('id', s.get('name', ''))
                display_name = s.get('name', skill_name)
                description = s.get('description', '')
                tags = s.get('tags', [])
            elif isinstance(s, str):
                skill_name = s
                display_name = skill_name
                description = ''
                tags = []
            else:
                continue
            if not skill_name:
                continue
            
            skill_id = f"skill-{self.node_name}-{skill_name}"
            try:
                await self._pg_pool.execute(
                    """INSERT INTO mesh.mesh_skills
                       (skill_id, agent_name, skill_name, display_name, description,
                        tags, cost, max_concurrent, status, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, 0.0, 3, 'active', now())
                       ON CONFLICT (skill_id)
                       DO UPDATE SET
                         display_name = EXCLUDED.display_name,
                         description = EXCLUDED.description,
                         tags = EXCLUDED.tags,
                         status = 'active',
                         updated_at = now()""",
                    skill_id, self.node_name, skill_name,
                    display_name, description, tags,
                )
                advertised += 1
            except Exception as e:
                log.warning(f"Failed to auto-advertise skill '{skill_name}': {e}")
        
        if advertised:
            log.info(f"📋 Auto-advertised {advertised} skills to marketplace")
        
        # Auto-publish skill FILES to PG for cross-node replication
        try:
            import os as _os
            repo_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
            skills_dir = _os.path.join(repo_root, "skills")
            published = 0
            if _os.path.isdir(skills_dir):
                for skill_name in _os.listdir(skills_dir):
                    skill_dir = _os.path.join(skills_dir, skill_name)
                    if not _os.path.isdir(skill_dir):
                        continue
                    skill_id = f"skill-{self.node_name}-{skill_name}"
                    files_to_publish = {}
                    for fn in _os.listdir(skill_dir):
                        fp = _os.path.join(skill_dir, fn)
                        if _os.path.isfile(fp) and fn.endswith(('.md', '.py', '.sh', '.txt', '.yaml', '.yml', '.json')):
                            try:
                                with open(fp, 'r', errors='replace') as f:
                                    files_to_publish[fn] = f.read()
                            except Exception:
                                pass
                    if files_to_publish:
                        import time as _time
                        now_ts = _time.time()
                        for fn, content in files_to_publish.items():
                            await self._pg_pool.execute(
                                """INSERT INTO mesh.mesh_skill_files (skill_id, filename, content, updated_at)
                                   VALUES ($1, $2, $3, $4)
                                   ON CONFLICT (skill_id, filename)
                                   DO UPDATE SET content = EXCLUDED.content, updated_at = EXCLUDED.updated_at""",
                                skill_id, fn, content, now_ts,
                            )
                        published += 1
            if published:
                log.info(f"📦 Auto-published {published} skill files to PG")
        except Exception as e:
            log.warning(f"Auto-publish skill files failed (non-fatal): {e}")

    async def _auto_sync_skills_delayed(self):
        """Auto-sync published skills from PG 10s after startup (non-blocking)."""
        try:
            await asyncio.sleep(10)
            if not self._pg_pool:
                return
            import os as _os
            rows = await self._pg_pool.fetch("SELECT DISTINCT skill_id FROM mesh.mesh_skill_files")
            if not rows:
                return
            repo_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
            skills_dir = _os.path.join(repo_root, "skills")
            _os.makedirs(skills_dir, exist_ok=True)
            synced = 0
            for row in rows:
                skill_id = row["skill_id"]
                # Only sync skills from OTHER nodes
                if f"-{self.node_name}-" in skill_id:
                    continue
                file_rows = await self._pg_pool.fetch(
                    "SELECT filename, content FROM mesh.mesh_skill_files WHERE skill_id = $1",
                    skill_id,
                )
                parts = skill_id.split("-", 2)
                skill_name = parts[2] if len(parts) > 2 else skill_id
                skill_dir = _os.path.join(skills_dir, skill_name)
                _os.makedirs(skill_dir, exist_ok=True)
                for fr in file_rows:
                    filepath = _os.path.join(skill_dir, fr["filename"])
                    filedir = _os.path.dirname(filepath)
                    if filedir and not _os.path.exists(filedir):
                        _os.makedirs(filedir, exist_ok=True)
                    with open(filepath, "w") as f:
                        f.write(fr["content"])
                synced += 1
            if synced:
                log.info(f"📦 Auto-synced {synced} skills from other nodes")
        except Exception as e:
            log.warning(f"Auto-sync skills failed (non-fatal): {e}")

    async def _broadcast_skill_to_peers(self, skill_name: str):
        """Notify peer nodes to pull a newly auto-generated skill from PG."""
        import aiohttp as aiohttp_lib
        try:
            peers = getattr(self.peer_discovery, '_peers', {})
            if not peers:
                return
            skill_id = f"skill-{self.node_name}-{skill_name}"
            for name, peer in peers.items():
                host = getattr(peer, 'host', None) or ''
                if not host:
                    continue
                url = f"http://{host}:8650/api/skills/auto-sync"
                try:
                    timeout = aiohttp_lib.ClientTimeout(total=5)
                    async with aiohttp_lib.ClientSession(timeout=timeout) as session:
                        async with session.post(url, json={"skill_ids": [skill_id]}) as resp:
                            if resp.status == 200:
                                log.info(f"📦 Skill {skill_name} synced to {name}")
                            else:
                                log.debug(f"Skill sync to {name} failed: {resp.status}")
                except Exception as e:
                    log.debug(f"Skill sync to {name} skipped: {e}")
        except Exception as e:
            log.debug(f"Broadcast skill to peers skipped: {e}")

    async def _heartbeat_loop(self):
        """Send periodic heartbeat messages."""
        while self._running:
            try:
                await asyncio.sleep(self.config.heartbeat.interval)
                if not self._running:
                    break

                uptime = int(time.time() - self._start_time)

                # Provider health check — include in heartbeat payload
                provider_status = {
            "version": self._resolved_version or "",}
                try:
                    try:
                        from core.provider_health import check_provider_health
                        provider_status = await asyncio.to_thread(check_provider_health, self.node_name)
                        log.info(f"Provider health (heartbeat) via import: {provider_status}")
                    except ImportError:
                        import importlib.util as _ilu
                        _this_dir = os.path.dirname(os.path.abspath(__file__))
                        _ph_path = os.path.join(_this_dir, "core", "provider_health.py")
                        if os.path.exists(_ph_path):
                            _spec = _ilu.spec_from_file_location("provider_health", _ph_path)
                            _mod = _ilu.module_from_spec(_spec)
                            _spec.loader.exec_module(_mod)
                            provider_status = await asyncio.to_thread(_mod.check_provider_health, self.node_name)
                            log.info(f"Provider health (heartbeat) via importlib: {provider_status}")
                except Exception as e:
                    log.error(f"Provider health check failed: {e}", exc_info=True)

                msg = A2AMessage.create(
                    sender=self.node_name,
                    recipient="broadcast",
                    msg_type=MSG_TYPE_HEARTBEAT,
                    payload={
                        "uptime": uptime,
                        "transports": list(self.router.transports.keys()),
                        "version": self._resolved_version,
                        "provider_status": provider_status,
                    },
                    priority=1,
                )

                # Persist heartbeat to PG
                await self._update_heartbeat_pg()

                # Cleanup expired dedup cache entries (prevents unbounded growth)
                if hasattr(self.router, 'dedup') and self.router.dedup:
                    removed = self.router.dedup.cleanup()
                    if removed > 0:
                        log.debug(f"Dedup cache cleanup: removed {removed} expired entries ({self.router.dedup.size} remaining)")

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

    def _get_advertise_ip(self) -> str:
        """Get the IP to advertise to other nodes.
        
        For Docker/HA containers, use advertise_host from config
        (the host IP) instead of the container's internal IP.
        VPN (Tailscale) preference: if `discovery.prefer` is vpn|auto and a
        Tailscale IP is available, advertise the VPN IP — encrypted transport
        that works even when the LAN is unreachable (WAN/remote nodes).
        """
        # VPN-beépítés: prefer=vpn kényszeríti, auto pedig VPN-t használ, ha fut
        try:
            from .core.vpn import get_preferred_address, resolve_peer_addresses
            _own_candidates = []
            # A saját node-névhez tartozó static_nodes bejegyzések = saját címek
            for _sn in (getattr(getattr(self.config, "discovery", None), "static_nodes", None) or []):
                try:
                    if (str(_sn.get("name", "")).lower() == str(self.node_name).lower()
                            and _sn.get("ip")):
                        _own_candidates.append({"ip": _sn.get("ip", "")})
                except Exception:
                    continue
            if _own_candidates:
                _addr = get_preferred_address(
                    getattr(self.config, "discovery", None), _own_candidates)
                if _addr and _addr.get("ip"):
                    return _addr["ip"]
        except ImportError:
            pass
        except Exception as _vpn_err:
            log.debug(f"VPN advertise-IP választás kihagyva: {_vpn_err}")
        # Check for advertise_host in P2P config
        if hasattr(self.config, 'p2p') and hasattr(self.config.p2p, 'advertise_host'):
            adv = self.config.p2p.advertise_host
            if adv:
                return adv
        # Check Docker platform config
        if hasattr(self.config, 'docker') and hasattr(self.config, 'docker'):
            docker_cfg = getattr(self.config, 'docker', {})
            if isinstance(docker_cfg, dict) and docker_cfg.get('host_ip'):
                return docker_cfg['host_ip']
        return self._get_local_ip()

    @property
    def pg_pool(self):
        """Expose _pg_pool as pg_pool for dashboard handlers."""
        return self._pg_pool

    def get_status(self) -> dict:
        """Return node status."""
        status = {
            "version": self._resolved_version or "",
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

                # Check PG pool connection and transport flag consistency
                if self._pg_pool:
                    pool_ok = self._pg_pool.is_connected()
                    if not pool_ok:
                        log.warning("PG pool connection lost — attempting reconnect")
                        try:
                            if await self._pg_pool.connect():
                                log.info("PG pool connection restored")
                                pool_ok = True
                        except Exception as e:
                            log.error(f"PG pool reconnect failed: {e}")
                    # Sync PG transport _available flag with actual pool state
                    pg_transport = self.router.transports.get('pg_notify')
                    if pg_transport is not None:
                        pg_flag = pg_transport.is_available()
                        if pool_ok and not pg_flag:
                            pg_transport._available = True
                            log.info("PG transport _available synced to True (pool connected)")
                        elif not pool_ok and pg_flag:
                            pg_transport._available = False
                            log.info("PG transport _available synced to False (pool disconnected)")

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

                # ── Evaluate alert rules ──
                if hasattr(self, 'dashboard') and self.dashboard and hasattr(self.dashboard, 'alert_manager'):
                    try:
                        metrics = self._collect_alert_metrics()
                        fired = self.dashboard.alert_manager.evaluate(metrics)
                        for alert in fired:
                            level = alert.get("autonomy_level", 1)
                            action = alert.get("auto_action", "")
                            if level == 1:
                                # Notify only — just log
                                log.warning(f"🟡 ALERT [L1-NOTIFY]: {alert['name']} — {alert['message']}")
                            elif level == 2:
                                # Suggest — log + suggest action
                                log.warning(f"🟠 ALERT [L2-SUGGEST]: {alert['name']} — {alert['message']} → Suggested: {action}")
                            elif level == 3:
                                # Auto-act — log + take action
                                log.warning(f"🔴 ALERT [L3-AUTO]: {alert['name']} — {alert['message']} → Auto-action: {action}")
                                if action == "reconnect_p2p":
                                    # Force P2P reconnect for all peers
                                    if hasattr(self, 'router'):
                                        for name, transport in self.router.transports.items():
                                            if name == 'p2p' and hasattr(transport, 'reconnect_all'):
                                                try:
                                                    await transport.reconnect_all()
                                                    log.info("🟢 Auto-action: P2P reconnect triggered")
                                                except Exception as e:
                                                    log.error(f"Auto-action P2P reconnect failed: {e}")
                    except Exception as e:
                        log.debug(f"Alert evaluation error: {e}")

                        # ── Channel Monitor Watchdog (Marveen-inspired) ──
                        try:
                            from .core.channel_monitor import watchdog_tick, get_watchdog_status
                            # Run watchdog for each known peer
                            for peer_name in (self.router.peers if hasattr(self, 'router') else {}):
                                async def _get_hb(name):
                                    peers = self.router.peers if hasattr(self, 'router') else {}
                                    p = peers.get(name, {})
                                    last_hb = p.get("last_heartbeat")
                                    if last_hb is None:
                                        return None
                                    import time as _t
                                    return _t.time() - last_hb
                                async def _get_active(name):
                                    if self._pg_pool and self._pg_pool.is_connected():
                                        try:
                                            row = await self._pg_pool.fetchrow(
                                                "SELECT COUNT(*) as n FROM shared_delegations WHERE assigned_agent=$1 AND status IN ('pending','running')",
                                                name)
                                            return row["n"] if row else 0
                                        except Exception:
                                            return 0
                                    return 0
                                async def _get_progress(name):
                                    if self._pg_pool and self._pg_pool.is_connected():
                                        try:
                                            row = await self._pg_pool.fetchrow(
                                                "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) as age FROM shared_delegations WHERE assigned_agent=$1 AND status='running'",
                                                name)
                                            return float(row["age"]) if row and row["age"] else None
                                        except Exception:
                                            return None
                                    return None
                                async def _get_proc_age(name):
                                    import time as _t
                                    return _t.time() - getattr(self, '_start_time', _t.time())
                                async def _restart_node(name):
                                    log.warning(f"Watchdog: restarting peer {name}")
                                    # Trigger P2P reconnect as recovery
                                    if hasattr(self, 'router'):
                                        for tname, transport in self.router.transports.items():
                                            if tname == 'p2p' and hasattr(transport, 'reconnect_all'):
                                                try:
                                                    await transport.reconnect_all()
                                                except Exception:
                                                    pass
                                async def _alert(name, msg):
                                    log.error(f"Watchdog ALERT {name}: {msg}")
                                try:
                                    await watchdog_tick(self._pg_pool, peer_name,
                                        get_heartbeat_age=_get_hb,
                                        get_active_delegations=_get_active,
                                        get_last_progress=_get_progress,
                                        get_process_age=_get_proc_age,
                                        restart_callback=_restart_node,
                                        alert_callback=_alert)
                                except Exception as e:
                                    log.debug(f"Watchdog tick for {peer_name}: {e}")
                        except ImportError:
                            pass
                        except Exception as e:
                            log.debug(f"Watchdog loop error: {e}")

                        # ── Desired State Reconciler (Marveen-inspired) ──
                        try:
                            from .core.desired_state import (
                                reconcile_desired_state,
                                ensure_initialized,
                                set_pg_pool as ds_set_pg_pool,
                            )
                            ds_set_pg_pool(self._pg_pool)
                            await ensure_initialized()
                            # Build known_nodes from registry
                            known = {}
                            for name, info in self._registry._nodes.items():
                                known[name] = {
                                    "last_heartbeat": info.get("last_heartbeat", 0),
                                    "status": info.get("status", "unknown"),
                                    "address": info.get("address", ""),
                                }
                            # Add self
                            known[self.node_name] = {
                                "last_heartbeat": time.time(),
                                "status": "online",
                            }
                            reconcile_result = await reconcile_desired_state(known)
                            if reconcile_result.restarted:
                                log.warning(f"Desired state restarted: {reconcile_result.restarted}")
                            if reconcile_result.failed:
                                log.error(f"Desired state failed: {reconcile_result.failed}")
                        except ImportError:
                            pass
                        except Exception as e:
                            log.debug(f"Desired state reconcile error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Health monitor error: {e}")

    # ─── v0.29: Auto-Bootstrap + Self-Healing Loop ────────────────────

    async def _ssh_key_announce_loop(self):
        """v2 SSH key protocol: keep ourselves registered with the coordinator.

        - Every 10 min (or when never registered): announce our keys to the
          tree root. The root aggregates all peers and broadcasts the bundle.
        - On the coordinator itself: periodically rebundle so new nodes that
          joined while a leaf was restarting converge on the full registry.
        """
        _last_announce = 0.0
        _ANNOUNCE_INTERVAL = 600.0  # 10 minutes
        while self._running:
            try:
                await asyncio.sleep(60)
                if not self._running:
                    break
                sync = getattr(self, 'ssh_key_sync', None)
                if sync is None:
                    continue
                now = time.time()
                # Coordinator: rebundle periodically (covers nodes that missed
                # the last bundle broadcast — e.g. were restarting)
                if sync._is_coordinator():
                    if now - sync._last_bundle_ts > _ANNOUNCE_INTERVAL:
                        await sync.broadcast_bundle()
                        sync._last_bundle_ts = now
                    continue
                # Leaf: announce (re-register) periodically so the coordinator
                # registry has fresh entries even after root failover
                if not sync._self_registered or now - _last_announce > _ANNOUNCE_INTERVAL:
                    await sync.announce_to_coordinator()
                    _last_announce = now
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"ssh_key_announce_loop error: {e}")
                await asyncio.sleep(30)

    async def _auto_bootstrap_heal_loop(self):
        """v0.29: Auto-bootstrap + self-healing loop.

        Runs every 60s and performs:
        1. PG connection health check + auto-reconnect (bootstrap retry)
        2. Node re-registration if PG was lost and restored
        3. P2P peer connection audit — reconnect disconnected peers
        4. Capability re-broadcast if peers are missing caps
        5. Transport recovery — restart failed transports
        """
        _pg_was_down = False
        _last_caps_broadcast = 0
        _last_decay = 0
        _last_dream = 0
        _last_inbox_check = 0
        _last_context_gate = 0
        _last_context_guard = 0
        _last_precompact = 0
        _last_auto_skill = 0
        _last_cap_sync = 0
        _last_gw_watchdog = 0
        _last_oq_flush = 0
        CAPS_REBROADCAST_INTERVAL = 300  # 5 min
        DECAY_INTERVAL = 3600  # 1 hour
        DREAM_INTERVAL = 21600  # 6 hours
        INBOX_CHECK_INTERVAL = 300  # 5 min
        CONTEXT_GATE_INTERVAL = 300  # 5 min
        CONTEXT_GUARD_INTERVAL = 300  # 5 min
        PRECOMPACT_INTERVAL = 1800  # 30 min
        AUTO_SKILL_INTERVAL = 600  # 10 min
        CAP_SYNC_INTERVAL = 600  # 10 min
        OQ_FLUSH_INTERVAL = 300  # 5 min

        while self._running:
            try:
                await asyncio.sleep(60)  # Check every 60s
                if not self._running:
                    break

                log.info("[self-heal] Loop tick — checking node health")

                # 1. PG connection check + auto-reconnect
                pg_ok = False
                if self._pg_pool:
                    # ASYNC check: a szinkron is_connected() csak a pool-objektum
                    # létezését nézi — az elhalt kapcsolatok ("closed mid-operation")
                    # láthatatlanok maradnak neki, és a pool 30+ percig "élőnek"
                    # tűnik. Az async verzió VALÓDI SELECT 1 health-checket futtat.
                    pg_ok = await self._pg_pool.is_connected_async()
                    if not pg_ok:
                        log.warning("[self-heal] PG pool disconnected — attempting reconnect")
                        try:
                            if await self._pg_pool.connect():
                                log.info("[self-heal] PG pool reconnected")
                                pg_ok = True
                        except Exception as e:
                            log.error(f"[self-heal] PG reconnect failed: {e}")
                elif not self._pg_pool:
                    # No PG pool at all (pool init failed at startup) — retry bootstrap.
                    # NOTE: MeshNode has no _pg_conn attribute (only _pg_pool); the old
                    # `elif not self._pg_conn:` reference raised AttributeError in this
                    # loop and silently killed the self-heal cycle on pg-less nodes.
                    if not _pg_was_down:
                        log.warning("[self-heal] No PG pool — attempting bootstrap")
                    if await self._init_pg_write_conn():
                        # Re-inject shared pool into subsystems
                        if self._pg_pool and self._pg_pool.is_connected():
                            self._pg_transport._shared_pool = self._pg_pool
                            self._pg_transport._owns_pool = False
                            self._p2p_transport._shared_pool = self._pg_pool
                        log.info("[self-heal] PG bootstrap successful")
                        pg_ok = True

                # 2. Re-register node if PG was lost and is now restored
                if pg_ok and _pg_was_down:
                    log.info("[self-heal] PG restored — re-registering node")
                    try:
                        await self._register_node()
                        log.info("[self-heal] Node re-registered in PG")
                        _pg_was_down = False
                    except Exception as e:
                        log.error(f"[self-heal] Node re-registration failed: {e}")
                elif not pg_ok:
                    _pg_was_down = True

                # 3. P2P peer connection audit
                if self._p2p_transport and self.peer_discovery:
                    p2p_connected = set(self._p2p_transport._peers.keys())
                    all_peers = set()
                    try:
                        all_peers = set(self.peer_discovery.get_all_peers().keys())
                    except Exception:
                        pass
                    disconnected = all_peers - p2p_connected - {self.node_name}
                    if disconnected:
                        log.info(f"[self-heal] Disconnected peers: {disconnected} — triggering discovery cycle")
                        try:
                            await self.peer_discovery.discover_and_connect()
                        except Exception as e:
                            log.debug(f"[self-heal] Discovery cycle error: {e}")

                # 4. Capability sync — compare PG caps vs registry caps, update if mismatch
                now = time.time()
                if hasattr(self, 'dashboard') and hasattr(self.dashboard, 'registry') and \
                   (now - _last_caps_broadcast > 60):  # Check every 60s
                    try:
                        reg_card = self.dashboard.registry.get(self.node_name)
                        if reg_card and getattr(reg_card, 'capabilities', None):
                            my_cap_count = len(reg_card.capabilities)
                            # Direct PG check — if PG has fewer caps, update
                            if self._pg_pool and pg_ok:
                                try:
                                    async with self._pg_pool.acquire() as conn:
                                        pg_caps = await conn.fetchval(
                                            "SELECT jsonb_array_length(capabilities) FROM mesh.mesh_nodes WHERE node_name=$1",
                                            self.node_name
                                        )
                                    if pg_caps is not None and pg_caps < my_cap_count:
                                        log.info(f"[self-heal] PG caps mismatch: PG={pg_caps}, registry={my_cap_count} — updating PG")
                                        await self._update_pg_capabilities(reg_card.capabilities)
                                        self._last_skills_announcement = 0
                                        await self._auto_advertise_skills()
                                except Exception as e:
                                    log.debug(f"[self-heal] PG caps check error: {e}")
                            _last_caps_broadcast = now
                    except Exception as e:
                        log.debug(f"[self-heal] Capabilities broadcast check error: {e}")

                # 5. Transport recovery — restart failed transports
                for name, transport in self.router.transports.items():
                    if not transport.is_available():
                        log.debug(f"[self-heal] Transport {name} unavailable — checking if restartable")
                        # P2P transport: let the reconnect loop handle it
                        if name == 'p2p':
                            continue
                        # PG transport: health monitor already handles reconnect
                        if name == 'pg_notify':
                            continue
                        # HTTP transport: try health check
                        if name == 'http' and hasattr(transport, 'health_check'):
                            try:
                                await transport.health_check()
                            except Exception:
                                pass

                # 6. Salience decay — fade unused memories hourly
                now_ts = time.time()
                if now_ts - _last_decay > DECAY_INTERVAL:
                    await self._salience_decay_tick()
                    _last_decay = now_ts

                # 7. Dream Engine — nightly analysis (every 6h)
                if now_ts - _last_dream > DREAM_INTERVAL:
                    try:
                        from .core.dream_engine import run_dream_cycle
                        kanban_mgr = getattr(self.dashboard, '_kanban_mgr', None) if hasattr(self, 'dashboard') else None
                        dream_result = await run_dream_cycle(self._pg_pool, node_name=self.node_name, kanban_mgr=kanban_mgr)
                        log.info(f"[self-heal] Dream Engine: {len(dream_result.get('buckets', {}))} buckets analyzed")
                    except Exception as e:
                        log.debug(f"[self-heal] Dream Engine skipped: {e}")
                    _last_dream = now_ts

                # 8. Inbox Nudge — check unread messages, escalate if needed (5 min)
                if now_ts - _last_inbox_check > INBOX_CHECK_INTERVAL:
                    try:
                        from .core.inbox_nudge import check_nudges
                        actions = check_nudges()
                        for action in actions:
                            if action["action"] == "alert":
                                log.warning(f"[inbox] ALERT: {action['to_node']} has unread from {action['from_node']} ({action['age_min']}min old)")
                                # Send alert via alert_manager if available
                                if hasattr(self, 'dashboard') and hasattr(self.dashboard, 'alert_manager'):
                                    try:
                                        await self.dashboard.alert_manager.send_alert(
                                            title=f"Inbox alert: {action['to_node']}",
                                            body=f"Unread message from {action['from_node']} ({action['age_min']}min): {action['preview']}",
                                            severity="warning",
                                        )
                                    except Exception:
                                        pass
                            elif action["action"] == "nudge":
                                log.info(f"[inbox] NUDGE: {action['to_node']} has unread from {action['from_node']} ({action['age_min']}min old)")
                    except Exception as e:
                        log.debug(f"[self-heal] Inbox nudge check skipped: {e}")
                    _last_inbox_check = now_ts

                # 9. Context Gate — check agent context saturation (5 min)
                if now_ts - _last_context_gate > CONTEXT_GATE_INTERVAL:
                    try:
                        from .core.context_gate import context_gate_tick
                        gate_results = await context_gate_tick(self._pg_pool)
                        for gr in gate_results:
                            log.warning(f"[context-gate] {gr['message']}")
                            if gr["severity"] == "critical" and hasattr(self, 'dashboard') and hasattr(self.dashboard, 'alert_manager'):
                                try:
                                    await self.dashboard.alert_manager.send_alert(
                                        title=f"Context gate: {gr['node']}",
                                        body=gr["message"],
                                        severity="critical",
                                    )
                                except Exception:
                                    pass
                    except Exception as e:
                        log.debug(f"[self-heal] Context gate tick skipped: {e}")
                    _last_context_gate = now_ts

                # 10. Auto-Skill — generate skills from completed delegations (10 min)
                if now_ts - _last_auto_skill > AUTO_SKILL_INTERVAL:
                    try:
                        from .core.auto_skill import maybe_generate_skill
                        # Check recently completed tasks from Kanban
                        if self._pg_pool:
                            async with self._pg_pool.acquire() as conn:
                                rows = await conn.fetch(
                                    """SELECT * FROM shared_delegations
                                       WHERE status = 'completed' AND created_at > NOW() - INTERVAL '1 hour'
                                       ORDER BY created_at DESC LIMIT 5"""
                                )
                                for row in rows:
                                    task = dict(row)
                                    skill_name = await maybe_generate_skill(task, self.node_name)
                                    if skill_name:
                                        log.info(f"[auto-skill] Generated: {skill_name}")
                                        # Register in mesh_skills table
                                        try:
                                            skill_id = "skill-" + self.node_name + "-" + skill_name
                                            await conn.execute(
                                                """INSERT INTO mesh.mesh_skills (skill_id, agent_name, skill_name, display_name, description, tags, status)
                                                   VALUES ($1, $2, $3, $4, $5, $6, 'active')
                                                   ON CONFLICT (skill_id) DO UPDATE SET updated_at = NOW()""",
                                                skill_id, self.node_name, skill_name,
                                                skill_name.replace('-', ' ').title(),
                                                "Auto-generated from delegation: " + task.get("subject", "")[:200],
                                                ["auto", "generated"]
                                            )
                                            log.info(f"[auto-skill] Registered in mesh: {skill_id}")
                                        except Exception as re:
                                            log.debug(f"[auto-skill] PG register skipped: {re}")
                                        # Broadcast to mesh
                                        try:
                                            await self._auto_advertise_skills()
                                        except Exception:
                                            pass
                    except Exception as e:
                        log.debug(f"[self-heal] Auto-skill check skipped: {e}")
                    _last_auto_skill = now_ts

                # 11. Capability Registry Sync — update SmartRouter from node capabilities (10 min)
                if now_ts - _last_cap_sync > CAP_SYNC_INTERVAL:
                    try:
                        smart_router = getattr(self, 'smart_router', None) or getattr(self.router, 'smart_router', None)
                        if smart_router and hasattr(smart_router, 'registry') and self._pg_pool and self._pg_pool.is_connected():
                            async with self._pg_pool.acquire() as conn:
                                rows = await conn.fetch(
                                    """SELECT node_name, capabilities FROM mesh_nodes WHERE capabilities IS NOT NULL"""
                                )
                                for row in rows:
                                    name = row["node_name"]
                                    caps = row["capabilities"] if isinstance(row["capabilities"], list) else []
                                    # Update registry with latest capabilities
                                    try:
                                        smart_router.registry.update_capabilities(name, caps)
                                    except Exception:
                                        pass
                            log.debug("[self-heal] Capability registry synced from PG")
                    except Exception as e:
                        log.debug(f"[self-heal] Capability sync skipped: {e}")
                    _last_cap_sync = now_ts

                # 12. Gateway Watchdog — check Hermes gateway health (every 2 min)
                # Process check is PRIMARY. Health endpoint is SECONDARY.
                # Only restart if BOTH are down (prevents false positives from
                # mesh node being briefly slow during self-healing loop).
                if now_ts - _last_gw_watchdog > 120:
                    try:
                        from .core.gateway_watchdog import check_process as _gw_check_process
                        gw_process_ok = _gw_check_process()
                        if gw_process_ok:
                            # Gateway process is running — it's healthy
                            log.debug("[self-heal] Gateway watchdog: process running ✅")
                        else:
                            # No gateway process found — check health endpoint
                            import urllib.request as _urllib
                            try:
                                req = _urllib.request.Request("http://localhost:8650/api/health", method="GET")
                                resp = _urllib.request.urlopen(req, timeout=10)
                                gw_ok = resp.status == 200
                            except Exception:
                                gw_ok = False
                            if gw_ok:
                                # Health endpoint responds but no process — probably
                                # running inside desktop app (macOS) or as child process
                                log.debug("[self-heal] Gateway watchdog: health OK but no separate process — likely inside desktop app")
                            else:
                                # BOTH down — actual gateway failure
                                log.warning("[self-heal] Gateway watchdog: process AND health both down — restarting")
                                try:
                                    from .core.gateway_watchdog import check_cooldown, check_restart_rate, restart_gateway, record_restart
                                    if check_cooldown() and check_restart_rate():
                                        success = restart_gateway(self.node_name)
                                        if success:
                                            record_restart()
                                            log.info(f"[self-heal] Gateway restart dispatched for {self.node_name}")
                                except Exception as gw_err:
                                    log.debug(f"[self-heal] Gateway watchdog restart failed: {gw_err}")
                    except Exception as e:
                        log.debug(f"[self-heal] Gateway watchdog check skipped: {e}")
                    _last_gw_watchdog = now_ts

                # 13. Context Guard — proactive agent context monitoring (5 min)
                if now_ts - _last_context_guard > CONTEXT_GUARD_INTERVAL:
                    try:
                        from .core.context_guard import context_guard_tick
                        guard_results = await context_guard_tick(self._pg_pool, node_name=self.node_name)
                        for gr in guard_results:
                            log.warning(f"[context-guard] {gr['message']}")
                            if gr.get("action") in ("force_restart", "hard_restart") and hasattr(self, 'dashboard') and hasattr(self.dashboard, 'alert_manager'):
                                try:
                                    await self.dashboard.alert_manager.send_alert(
                                        title=f"Context guard: {gr['agent']}",
                                        body=gr["message"],
                                        severity="critical" if gr["action"] == "force_restart" else "warning",
                                    )
                                except Exception:
                                    pass
                    except Exception as e:
                        log.debug(f"[self-heal] Context guard tick skipped: {e}")
                    _last_context_guard = now_ts

                # 14. PreCompact Hook — audit context pressure, save critical info (30 min)
                if now_ts - _last_precompact > PRECOMPACT_INTERVAL:
                    try:
                        from .core.precompact_hook import precompact_audit
                        audit = await precompact_audit(self._pg_pool, node_name=self.node_name)
                        if audit.get("total", 0) > 0:
                            log.info(f"[precompact] {audit['total']} critical context saves in last 24h")
                    except Exception as e:
                        log.debug(f"[self-heal] PreCompact audit skipped: {e}")
                    _last_precompact = now_ts

                # 15. Offline Queue Periodic Flush — deliver queued messages when
                # recipients are back online (previously ONLY ran on transport-recovery
                # events, so messages queued during a brief offline window stayed stuck
                # forever — e.g. 415 ssh_key_sync messages stuck for 3 days while all
                # nodes were actually online).
                if now_ts - _last_oq_flush > OQ_FLUSH_INTERVAL:
                    try:
                        oq = getattr(self.router, "_offline_queue", None)
                        if oq is not None:
                            flushed = await self.router._flush_offline_queue()
                            if flushed:
                                log.info(f"[self-heal] Offline queue flush: {flushed} message(s) delivered")
                    except Exception as e:
                        log.debug(f"[self-heal] Offline queue flush skipped: {e}")
                    _last_oq_flush = now_ts

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[self-heal] Loop error: {e}")

    async def _salience_decay_tick(self):
        """Apply salience decay to Brain memories. Called hourly by self-healing loop."""
        try:
            from .core.salience_decay import apply_decay
            if self._pg_pool and self._pg_pool.is_connected():
                async with self._pg_pool.acquire() as conn:
                    result = await apply_decay(conn, hours=1.0)
                    if result:
                        log.info(f"[self-heal] Salience decay: {result}")
        except Exception as e:
            log.debug(f"[self-heal] Salience decay skipped: {e}")

    # ─── Stats Update Loop ───────────────────────────────────────────

    def _transport_error_delta(self, current_total: int) -> int:
        """v0.41.1: Transport error delta since last collection.

        The router 'errors' stat is a cumulative counter since process start.
        Alert rules using '> 0' on a cumulative counter fire forever (RE-FIRE
        loop). This returns errors since the last collection, so rules only
        fire on NEW errors. First call returns 0 (baseline).
        """
        prev = getattr(self, "_prev_transport_errors", None)
        self._prev_transport_errors = current_total
        if prev is None:
            return 0  # baseline: don't fire on historical errors
        return max(0, current_total - prev)

    def _collect_alert_metrics(self) -> dict:
        """Collect current metrics for alert rule evaluation."""
        t_stats = self.router.get_stats() if self.router else {}
        pd = self.peer_discovery
        peers = pd.get_stats() if pd else {}
        p2p = self.router.transports.get('p2p') if self.router else None
        peer_stats = p2p.get_peer_stats() if p2p else {}
        return {
            "peers_connected": peers.get("connected_peers", 0),
            "peers_known": peers.get("known_peers", 0),
            "messages_sent": t_stats.get("sent", 0),
            "messages_received": t_stats.get("received", 0),
            "messages_forwarded": t_stats.get("forwarded", 0),
            "transport_errors": self._transport_error_delta(t_stats.get("errors", 0)),
            "dedup_cache_size": t_stats.get("dedup", {}).get("size", 0),
            "retry_queue_size": p2p.get_retry_queue_size() if p2p else 0,
            "peer_count": len(peer_stats),
        }

    async def _stats_update_loop(self):
        """Periodically update node stats in PG (messages sent/received, uptime)."""
        import gc
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
                # Cleanup old inbound messages (processed > 1h, stale > 24h)
                self.local_store.cleanup_inbound(max_age_hours=1)
                # Cleanup completed file transfers (> 24h) and abandoned (> 48h)
                self.local_store.cleanup_file_transfers(max_age_hours=24)
                # Cleanup old mesh_messages (retention: 7 days)
                await self._cleanup_old_messages(max_age_days=7)
                # Cleanup old debug logs (retention: 7 days)
                await self._cleanup_debug_logs(max_age_hours=168)
                # Periodic garbage collection to prevent memory buildup
                gc.collect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"Stats update error: {e}")

    async def _log_rotation_loop(self, check_interval: int = 3600):
        """v0.42: Built-in log rotation — gzip + truncate when log exceeds threshold.

        Deterministic, no LLM. Guards against the 2026-09-05 incident
        (231MB log). The node holds fd 1/2 on the log file, so we
        truncate (gzip archive first) instead of move — the fd keeps
        writing to the same inode.
        """
        max_mb = getattr(self.config, 'log_max_mb', 100)
        keep = max(1, getattr(self.config, 'log_archives_keep', 5))
        log_file = os.path.expanduser(getattr(self.config, 'log_file', '') or '')
        # fd 1/2 log (launchd/systemd stdout+stderr capture) — a2a_mesh_node.log
        node_log = os.path.join(os.path.dirname(log_file) or os.path.expanduser('~/.hermes/logs'), 'a2a_mesh_node.log')
        while self._running:
            try:
                await asyncio.sleep(check_interval)
                if not self._running:
                    break
                for target in (log_file, node_log):
                    if not target or not os.path.exists(target):
                        continue
                    try:
                        size_mb = os.path.getsize(target) / (1024 * 1024)
                        if size_mb <= max_mb:
                            continue
                        archive = f"{target}.{int(time.time())}.gz"
                        with open(target, 'rb') as f_in:
                            import gzip as _gz
                            with _gz.open(archive, 'wb') as f_out:
                                while True:
                                    chunk = f_in.read(1024 * 1024)
                                    if not chunk:
                                        break
                                    f_out.write(chunk)
                        # Truncate IN PLACE (fd 1/2 stays valid, keeps writing here)
                        with open(target, 'r+b') as f:
                            f.truncate(0)
                        log.info(f"📦 Log rotation: {os.path.basename(target)} {size_mb:.1f}MB → {os.path.basename(archive)}")
                        # Prune old archives beyond keep-count
                        base = os.path.basename(target)
                        sib = sorted(
                            (f for f in os.listdir(os.path.dirname(target) or '.') if f.startswith(base + '.') and f.endswith('.gz')),
                            key=lambda f: os.path.getmtime(os.path.join(os.path.dirname(target) or '.', f))
                        )
                        for old in sib[:-keep]:
                            try:
                                os.remove(os.path.join(os.path.dirname(target) or '.', old))
                            except OSError:
                                pass
                    except Exception as e:
                        log.debug(f"Log rotation skipped for {target}: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Log rotation loop error: {e}")

    def _prune_node_log_archives(self, keep: int = 3):
        """One-shot prune of old a2a_mesh_node.log.*.gz archives (startup housekeeping)."""
        try:
            log_dir = os.path.expanduser('~/.hermes/logs')
            base = 'a2a_mesh_node.log'
            archs = sorted(
                (f for f in os.listdir(log_dir) if f.startswith(base + '.') and f.endswith('.gz')),
                key=lambda f: os.path.getmtime(os.path.join(log_dir, f))
            )
            for old in archs[:-keep]:
                os.remove(os.path.join(log_dir, old))
        except Exception:
            pass

    async def _memory_maintenance_loop(self):
        """v0.40.4: Periodic capsule promotion + auto skill generation + self-reflection.

        Runs every 5 minutes. Each node:
        1. Fetches recent mesh chat messages and creates capsules from them
        2. Promotes mature capsules to engramms
        3. Generates SKILL.md from well-referenced engramms

        This ensures ALL nodes (not just Nova) generate memory capsules
        and skills from their own perspective.
        """
        from core.capsules import check_and_promote_capsules, check_and_generate_skills, store_capsule
        from core.reflection import run_reflection_cycle
        # Restart-safe cursor: initialize from DB max(id) on the FIRST loop iteration
        # (after PG pool is confirmed connected) so we never re-process old messages
        # after a node restart (prevents reflection/capsule flood).
        last_reflection_msg_id = 0
        _cursor_init_done = False
        while self._running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                if not self._running:
                    break
                pg_pool = getattr(self, '_pg_pool', None)
                if not pg_pool or not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
                    continue

                if not _cursor_init_done:
                    try:
                        _cursor_row = await pg_pool.fetchrow(
                            "SELECT COALESCE(MAX(id), 0) AS max_id FROM mesh.mesh_chat_messages"
                        )
                        if _cursor_row:
                            last_reflection_msg_id = int(_cursor_row['max_id'] or 0)
                            log.info(f"🧠 Memory cursor initialized at msg_id={last_reflection_msg_id} (restart-safe)")
                    except Exception as e:
                        log.warning(f"Memory cursor init failed: {e}")
                    _cursor_init_done = True

                # ── Ollama endpoint for embeddings/deep-reflection (config-driven) ──
                ollama_url = getattr(self.config, 'ollama_url', 'http://localhost:11434')

                # ── Step 1: Self-reflection from recent mesh chat messages ──
                # Each node processes chat messages independently, creating
                # capsules and reflections from their own perspective.
                try:
                    rows = await pg_pool.fetch(
                        """SELECT id, sender, content, created_at
                           FROM mesh.mesh_chat_messages
                           WHERE id > $1
                           ORDER BY id ASC LIMIT 50""",
                        last_reflection_msg_id,
                    )
                    if rows and len(rows) >= 5:
                        # Group messages by topic (simple: use time gap > 10 min as topic boundary)
                        messages = []
                        for r in rows:
                            messages.append({
                                'id': r['id'],
                                'sender': r['sender'],
                                'content': r['content'] or '',
                                'created_at': r['created_at'],
                            })
                            last_reflection_msg_id = max(last_reflection_msg_id, r['id'])

                        # Build agents list
                        agents = list(set(m['sender'] for m in messages if m['sender']))
                        topic = messages[0]['content'][:80] if messages else 'mesh activity'

                        # Run reflection cycle (LLM deep reflection using this node's own model)
                        prompt_text, ref_id = await run_reflection_cycle(
                            pg_pool, messages, topic, agents, ollama_url,
                        )

                        # Also store a capsule if we have enough messages
                        if len(messages) >= 5 and ref_id:
                            msg_ids = [m['id'] for m in messages]
                            await store_capsule(
                                pg_pool, topic,
                                ' '.join(m['content'][:200] for m in messages[:5]),
                                agents,
                                min(msg_ids), max(msg_ids),
                                ollama_url,
                            )
                            log.info(f"🧠 Self-reflection: {len(messages)} msgs, capsule+reflection stored for topic '{topic[:50]}'")
                except Exception as e:
                    log.debug(f"Self-reflection step skipped: {e}")

                # ── Step 2: Promote capsules → engramms ──
                promoted = await check_and_promote_capsules(pg_pool, ollama_url)

                # ── Step 3: Generate skills from engramms ──
                generated = await check_and_generate_skills(pg_pool)

                if promoted or generated:
                    log.info(f"🧠 Memory maintenance: promoted={promoted}, skills_generated={generated}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"Memory maintenance error: {e}")

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
                # Use non-blocking measurement after baseline is established
                # First call ever: interval=0 returns 0.0, so we need a blocking call once
                if not self._cpu_baseline_initialized:
                    psutil.cpu_percent(interval=0.5)  # Establish baseline
                    self._cpu_baseline_initialized = True
                raw_cpu = psutil.cpu_percent(interval=0)  # Non-blocking: returns delta since last call
                # Apply EMA smoothing to reduce spike artifacts from psutil
                if not self._cpu_ema_initialized:
                    self._cpu_ema = raw_cpu
                    self._cpu_ema_initialized = True
                else:
                    self._cpu_ema = 0.3 * raw_cpu + 0.7 * self._cpu_ema  # EMA alpha=0.3
                cpu_pct = self._cpu_ema
                memory_pct = psutil.virtual_memory().percent
                disk_pct = psutil.disk_usage('/').percent
            except ImportError:
                try:
                    import os, multiprocessing
                    load1, load5, load15 = os.getloadavg()
                    cpu_count = multiprocessing.cpu_count() or 1
                    # Use 5-min load average for more stability, cap at 100%
                    # load average > cpu_count means over-subscribed, report 100%
                    raw_pct = (load5 / cpu_count) * 100
                    cpu_pct = min(raw_pct, 100.0)
                    memory_pct = 50.0
                    disk_pct = 50.0
                except Exception:
                    pass
            
            host_ip = self._get_advertise_ip()
            
            # UPSERT into mesh_nodes — only UPDATE if exists (INSERT requires short_addr etc.)
            try:
                await self._pg_pool.execute("""
                    UPDATE mesh.mesh_nodes 
                    SET last_heartbeat = NOW(),
                        status = 'active',
                        host = $1,
                        p2p_port = $3
                    WHERE node_name = $2
                """, host_ip, self.node_name, self.config.p2p.listen_port)
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
        
        LLM-independent: if wake_agent_on_message=False (default), this is a no-op.
        The mesh infrastructure (routing, delivery, storage) works without any LLM calls.
        """
        # LLM wake control — skip entirely if disabled
        if not getattr(self.config, 'wake_agent_on_message', False):
            log.debug(f"Wake-agent skipped (wake_agent_on_message=False) for {message.id[:8]} from {message.sender}")
            return
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
            
            # Extract chat_username from payload; determine chat_type from recipient
            _chat_user = None
            _chat_type = "broadcast" if message.recipient == "broadcast" else "user_dm"
            try:
                import json as _j
                _p = message.payload
                if isinstance(_p, str):
                    try:
                        _p = _j.loads(_p)
                    except Exception:
                        # Wrapped/trusted-peer envelope (not raw JSON) — regex-extract the
                        # chat_username field so per-user history persistence survives framing
                        import re as _re_cu
                        _m = _re_cu.search(r"'chat_username':\s*'([^']+)'", _p)
                        if not _m:
                            _m = _re_cu.search(r'"chat_username":\s*"([^"]+)"', _p)
                        if _m:
                            _chat_user = _m.group(1)
                        _p = {}
                if isinstance(_p, dict):
                    _chat_user = _p.get("chat_username") or _chat_user
            except Exception:
                pass
            if _chat_user:
                payload["chat_username"] = _chat_user
                payload["chat_type"] = _chat_type
            
            # Add mesh_secret for dashboard wake-agent API auth
            if wake_url == dashboard_url:
                payload["mesh_secret"] = "mesh-wake-secret-2026"
                # Build prompt from message content for wake-agent
                if _chat_user:
                    # Extract just the text content, not the full JSON payload
                    _content_text = payload['content']
                    if isinstance(_p, dict):
                        _content_text = _p.get('text', _p.get('subject', str(_p)[:500]))
                    # ── @mention directive: if this node is @mentioned in a broadcast, emphasize ──
                    import re as _re_ment_p
                    _mentions_in_msg = [m.lower() for m in _re_ment_p.findall(r"@(\w+)", _content_text or "")]
                    if _chat_type == "broadcast" and self.node_name.lower() in _mentions_in_msg:
                        prompt_text = f"🔔 NEKED ÍRTÁK a közös szobában! {_chat_user} kifejezetten hozzád intézte: {_content_text[:1500]} — VÁLASZOLNOD KELL. Több agentnek nem kell válaszolnia."
                    else:
                        # [:1500] — a /ideas, /debate parancs-prefixek (~600 char) teljes
                        # átviteléhez kell; a korábbi [:300] levágta a [ÖTLET]-formátum-
                        # utasítást, így az agentek sosem látták és nem küldtek ötleteket.
                        prompt_text = f"Új üzenet érkezett {_chat_user}-tól: {_content_text[:1500]}"
                else:
                    prompt_text = f"[A2A Message from {message.sender}] {payload['content']}"

                # ── Engramm injection: retrieve relevant past conclusions for this prompt ──
                # Makes the engramm system actually feed back into conversations.
                try:
                    from core.capsules import retrieve_engramms, format_engramms_for_prompt
                    _eng_pool = getattr(self, '_pg_pool', None)
                    if _eng_pool and hasattr(_eng_pool, 'is_connected') and _eng_pool.is_connected():
                        _eng_ollama = getattr(self.config, 'ollama_url', 'http://localhost:11434')
                        _engramms = await retrieve_engramms(_eng_pool, (prompt_text or "")[:500], ollama_url=_eng_ollama)
                        _eng_ctx = format_engramms_for_prompt(_engramms)
                        if _eng_ctx:
                            prompt_text = f"{_eng_ctx}\n\n{prompt_text}"
                            log.info(f"🧠 Engramm context injected into wake-agent prompt ({len(_engramms)} relevant)")
                except Exception as _eng_e:
                    log.debug(f"Engramm injection skipped: {_eng_e}")

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
                # Retry on 429 (another wake in progress / cooldown) — chat DMs must not be dropped.
                # Honor retry_after when provided; otherwise back off 8s per attempt, max 4 attempts (~30s window).
                _max_wake_attempts = 4
                for _attempt in range(1, _max_wake_attempts + 1):
                    async with session.post(wake_url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                        if resp.status == 200:
                            log.info(f"Wake-agent triggered for message {message.id[:8]} from {message.sender} via {wake_url}")
                            return  # Success, no need for fallback
                        elif resp.status == 429 and _attempt < _max_wake_attempts:
                            _retry_body = await resp.text()
                            _retry_after = 8
                            try:
                                import json as _rj
                                _retry_after = int(_rj.loads(_retry_body).get("retry_after", 8))
                            except Exception:
                                pass
                            _retry_after = max(2, min(_retry_after, 20))
                            log.info(f"⏳ Wake-agent 429 (busy) — attempt {_attempt}/{_max_wake_attempts}, retrying in {_retry_after}s for {message.id[:8]}")
                            await asyncio.sleep(_retry_after)
                            continue
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
                        async with session.post(fallback, json=payload, timeout=_aiohttp_fallback.ClientTimeout(total=120)) as resp:
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