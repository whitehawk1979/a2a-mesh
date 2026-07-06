"""A2A Mesh TCP P2P Transport — Direct TCP peer-to-peer connections.

Each agent runs a TCP server that other agents can connect to.
Messages use versioned binary framing: [1-byte version][4-byte length][payload].
Discovery via mDNS (zeroconf) or static config.
"""

import asyncio
import json
import os
import ssl
import struct
import logging
import time
from typing import Dict, List, Optional, Set, Tuple, Any

from core.exceptions import ConfigurationError

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult, MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT
from ..core.framing import encode_frame, read_frame, FRAME_VERSION, V1_MARKER

log = logging.getLogger("a2a_mesh.transports.p2p")


class P2PTransport(TransportAdapter):
    """TCP P2P transport for direct agent-to-agent communication.

    Features:
    - Async TCP server (listen for incoming connections)
    - Async TCP client (connect to known peers)
    - Length-prefixed message framing
    - Auto-reconnect on connection loss
    - mDNS discovery integration
    - Connection pooling with Nagle disabled for low-latency
    - Write coalescing for efficient small-message delivery
    - Adaptive backoff with jitter for reconnection
    - Bandwidth-managed file transfer
    """

    name = "p2p"

    # ─── Tuning Constants ─────────────────────────────────────────────
    HEARTBEAT_INTERVAL = 45       # Seconds between heartbeats when idle
    HEARTBEAT_TIMEOUT = 180       # Seconds before declaring a peer dead
    CONNECT_TIMEOUT = 10          # Seconds for TCP+TLS connect timeout
    CONNECTION_AGE_LIMIT = 3600   # Reconnect stale connections after 1 hour
    MAX_RETRY_QUEUE = 1000        # Max messages in retry queue
    MAX_CONNECTIONS = 100         # Hard cap on peer connections
    BASE_RECONNECT = 5            # Base reconnect interval (seconds)
    MAX_BACKOFF = 300             # Max exponential backoff (5 minutes)
    NAGLE_DISABLED = True         # Disable Nagle for low-latency
    WRITE_BATCH_SIZE = 8          # Max frames to coalesce per drain()
    WRITE_BATCH_TIMEOUT = 0.005   # Max seconds to wait before draining batched writes
    FILE_CHUNK_DELAY = 0.01       # Inter-chunk delay for file transfers
    FILE_BANDWIDTH_LIMIT = 0     # Max bytes/sec for file transfers (0 = unlimited)
    INITIAL_BACKOFF_JITTER = 0.5 # Jitter factor for backoff (0-1)

    def __init__(self, config):
        self.config = config
        self._available = False
        self._server: Optional[asyncio.Server] = None
        self._peers: Dict[str, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._peer_addresses: Dict[str, str] = {}  # peer_name → host:port
        self._peer_backoff: Dict[str, float] = {}  # peer_name → next retry timestamp
        self._peer_retry_count: Dict[str, int] = {}  # peer_name → consecutive failure count
        self._peer_connected_at: Dict[str, float] = {}  # peer_name → connection start timestamp
        self._peer_latency: Dict[str, float] = {}  # peer_name → estimated RTT in ms (EWMA)
        self._reconnect_task: Optional[asyncio.Task] = None
        self._incoming_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._listen_host = config.p2p.listen_host
        self._listen_port = config.p2p.listen_port
        self._message_callback = None
        self._max_retries = min(getattr(config.p2p, 'max_connections', 50), self.MAX_CONNECTIONS)
        self._reconnect_interval = max(5, getattr(config.p2p, 'reconnect_interval', self.BASE_RECONNECT))
        self._max_connections = self._max_retries  # reuse as max_connections cap
        self._connection_age_limit = self.CONNECTION_AGE_LIMIT
        self._connection_tasks: Dict[str, asyncio.Task] = {}  # Per-peer connection tasks
        self._ssl_context: Optional[ssl.SSLContext] = None  # TLS server context
        self._ssl_client_context: Optional[ssl.SSLContext] = None  # TLS client context

        # Retry queue: messages that failed to send, retried on reconnect
        self._retry_queue: List[Tuple[A2AMessage, float]] = []  # (message, queued_at)
        self._retry_interval = 10
        self._max_retry_queue_size = self.MAX_RETRY_QUEUE

        # Priority send queues: per-peer async priority queues for ordered delivery
        # Priority: 1=heartbeat, 2=ACK, 3=control, 5=data, 7=file_chunk
        self._send_queues: Dict[str, asyncio.PriorityQueue] = {}
        self._send_tasks: Dict[str, asyncio.Task] = {}
        self._send_seq: int = 0  # Monotonically increasing sequence counter for FIFO

        # Write coalescing: batch small writes per peer for efficiency
        self._write_batch: Dict[str, List[bytes]] = {}  # peer_name → list of encoded frames
        self._write_batch_task: Dict[str, asyncio.Task] = {}  # peer_name → flush timer

        # ACK callback
        self._ack_callback = None  # Callable[[str, str], Awaitable[None]]

        # Peer connected callback
        self._peer_connected_callback = None  # Callable[[str], Awaitable[None]]

        # Peer address resolver (set by node.py)
        self._peer_address_resolver = None  # Callable[[str], Optional[Tuple[str, int]]]

        # Dynamic connection cache — avoid duplicate attempts
        self._connecting_peers: Set[str] = set()

        # Reverse-lookup: writer → peer_name
        self._writer_to_peer: Dict[int, str] = {}

        # Adaptive backoff state
        self._backoff_random = __import__('random').Random()  # Per-instance RNG for jitter

        # Setup TLS if configured
        p2p_config = getattr(config, 'p2p', None)
        tls_enabled = getattr(p2p_config, 'tls_enabled', False) if p2p_config else False
        tls_cert = os.path.expanduser(getattr(p2p_config, 'tls_cert', '') or '') if p2p_config else ''
        tls_key = os.path.expanduser(getattr(p2p_config, 'tls_key', '') or '') if p2p_config else ''
        tls_ca = os.path.expanduser(getattr(p2p_config, 'tls_ca', '') or '') if p2p_config else ''
        tls_verify_peer = getattr(p2p_config, 'tls_verify_peer', False) if p2p_config else False

        log.info(f"P2P TLS config: enabled={tls_enabled} cert={tls_cert} key={tls_key} ca={tls_ca} verify={tls_verify_peer}")
        print(f"[P2P] TLS config: enabled={tls_enabled} cert={tls_cert} key={tls_key} ca={tls_ca} verify={tls_verify_peer}", flush=True)

        if tls_enabled and tls_cert and tls_key:
            import ssl as _ssl
            try:
                # P1: Separate server and client SSL contexts for proper TLS
                # Server context (for incoming connections)
                self._ssl_context = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
                self._ssl_context.load_cert_chain(tls_cert, tls_key)
                # P1: Remove DHE ciphers, use only ECDHE+AESGCM (TLS 1.3 compatible)
                self._ssl_context.set_ciphers('ECDHE+AESGCM')
                self._ssl_context.minimum_version = _ssl.TLSVersion.TLSv1_2
                # Load CA for peer verification if configured
                if tls_ca:
                    self._ssl_context.load_verify_locations(tls_ca)
                    log.info(f"P2P TLS: loaded CA from {tls_ca}")
                if tls_verify_peer:
                    self._ssl_context.verify_mode = _ssl.CERT_REQUIRED
                    log.info("P2P TLS: peer certificate verification ENABLED")
                else:
                    self._ssl_context.verify_mode = _ssl.CERT_NONE
                    log.info("P2P TLS: peer certificate verification DISABLED")

                # P1: Client context (for outgoing connections) — trust server cert
                self._ssl_client_context = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
                self._ssl_client_context.load_cert_chain(tls_cert, tls_key)
                self._ssl_client_context.minimum_version = _ssl.TLSVersion.TLSv1_2
                self._ssl_client_context.set_ciphers('ECDHE+AESGCM')
                if tls_ca:
                    self._ssl_client_context.load_verify_locations(tls_ca)
                if tls_verify_peer:
                    self._ssl_client_context.verify_mode = _ssl.CERT_REQUIRED
                else:
                    self._ssl_client_context.check_hostname = False
                    self._ssl_client_context.verify_mode = _ssl.CERT_NONE

                log.info(f"P2P TLS enabled with cert={tls_cert}")
                print(f"[P2P] TLS enabled with cert={tls_cert}", flush=True)
            except Exception as e:
                log.error(f"P2P TLS init FAILED: {e}")
                print(f"[P2P] TLS init FAILED: {e}", flush=True)
                self._ssl_context = None
        elif tls_enabled:
            # P2 SECURITY: fail-closed — do NOT fall back to plain TCP
            log.error("P2P TLS enabled but no cert/key configured — REFUSING to start without TLS")
            print("[P2P] TLS enabled but no cert/key configured — REFUSING to start without TLS", flush=True)
            raise ConfigurationError("TLS is enabled but no certificate/key files are configured. "
                                     "Refusing to fall back to plain TCP. "
                                     "Either configure TLS cert/key or set tls_enabled=false.")

    async def start(self) -> bool:
        """Start TCP server and connect to known peers."""
        try:
            self._server = await asyncio.start_server(
                self._handle_connection,
                self._listen_host,
                self._listen_port,
                ssl=self._ssl_context,  # None = plain TCP, SSLContext = TLS
            )
            self._running = True
            self._available = True
            log.info(f"P2P transport started on {self._listen_host}:{self._listen_port}")

            # Connect to static peers
            for node in self.config.discovery.static_nodes:
                host = node.get("host", node.get("ip", ""))
                port = node.get("p2p_port", self._listen_port)
                name = node.get("name", "")
                if host and name:
                    asyncio.create_task(self._connect_to_peer(name, host, port))

            # Start reconnection monitor
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

            return True
        except OSError as e:
            if "address already in use" in str(e).lower() or getattr(e, 'errno', None) in (48, 98):
                # Port already bound — try alternative ports before giving up
                original_port = self._listen_port
                for offset in [1, -1, 2, -2, 5, -5]:
                    alt_port = original_port + offset
                    if alt_port <= 0 or alt_port > 65535:
                        continue
                    try:
                        self._server = await asyncio.start_server(
                            self._handle_connection, self._listen_host, alt_port
                        )
                        self._listen_port = alt_port
                        self._running = True
                        self._available = True
                        log.info(f"P2P port {original_port} in use, bound to alternative port {alt_port}")
                        # Start reconnection monitor
                        self._reconnect_task = asyncio.create_task(self._reconnect_loop())
                        return True
                    except OSError:
                        continue
                # All alternative ports failed — assume existing server from previous process
                log.warning(f"P2P port {original_port} and alternatives in use — assuming existing server")
                self._running = True
                self._available = True
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())
                return True
            log.error(f"P2P transport start failed: {e}")
            return False
        except Exception as e:
            log.error(f"P2P transport start failed: {e}")
            return False

    async def stop(self) -> bool:
        """Shutdown TCP server and close all connections."""
        self._running = False

        # Cancel all send loop tasks
        for peer_name, task in list(self._send_tasks.items()):
            if not task.done():
                task.cancel()
        self._send_tasks.clear()
        self._send_queues.clear()

        # Close all peer connections
        for peer_name, (reader, writer) in list(self._peers.items()):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self._peers.clear()

        # Close server
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        self._available = False
        log.info("P2P transport stopped")
        return True

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming/outgoing TCP connection with heartbeat keepalive.

        Heartbeat: If no message received within HEARTBEAT_INTERVAL (45s),
        send a heartbeat ping. If no response within HEARTBEAT_TIMEOUT (180s),
        close the connection.
        
        Optimizations:
        - Tracks per-peer latency (EWMA) for adaptive routing decisions
        - Uses class-level constants for tunable parameters
        - Handles incoming connections from known peers (reconnection)
        """
        peer_addr = writer.get_extra_info('peername')
        log.debug(f"New connection from {peer_addr}")

        # Try to identify which peer this connection is from
        connected_peer_name = None
        for pname, (pr, pw) in self._peers.items():
            if pw is writer:
                connected_peer_name = pname
                break

        # Also check if an existing peer reconnects (new socket replaces old)
        last_received = time.time()
        last_heartbeat_sent = time.time()

        try:
            while self._running:
                # Read versioned frame with timeout for heartbeat check
                try:
                    frame_data = await asyncio.wait_for(read_frame(reader), timeout=self.HEARTBEAT_INTERVAL)
                    if frame_data is None:
                        log.debug(f"Connection from {peer_addr} closed (read_frame returned None)")
                        break
                    version, data = frame_data
                    message = A2AMessage.from_bytes(data)

                    last_received = time.time()

                    # Handle heartbeat messages
                    if message.type == MSG_TYPE_HEARTBEAT:
                        log.debug(f"Heartbeat received from {message.sender}")
                        # Don't re-queue heartbeats for normal processing
                        continue

                    # Handle ACK messages: don't re-queue, just invoke callback
                    if message.type == MSG_TYPE_ACK:
                        payload = message.payload if isinstance(message.payload, dict) else {}
                        ack_for_id = payload.get("ack_for", "")
                        ack_type = payload.get("ack_type", "delivered")
                        log.info(f"P2P ACK received for message {ack_for_id[:8]} from {message.sender}: {ack_type}")
                        # Track per-peer latency (EWMA) for adaptive routing
                        ts = payload.get("timestamp", 0)
                        if ts and connected_peer_name:
                            rtt_ms = (time.time() - ts) * 1000
                            old_rtt = self._peer_latency.get(connected_peer_name, 0) or rtt_ms
                            self._peer_latency[connected_peer_name] = old_rtt * 0.7 + rtt_ms * 0.3  # EWMA
                        if self._ack_callback and ack_for_id:
                            try:
                                asyncio.create_task(self._ack_callback(ack_for_id, ack_type))
                            except Exception as e:
                                log.error(f"ACK callback error: {e}")
                        # Also enqueue so node-level handlers can process it
                        await self._incoming_queue.put((message, "p2p"))
                        continue

                    # On first message from a peer, fire peer_connected callback (for incoming connections)
                    if connected_peer_name is None and message.sender and message.sender != getattr(self.config, 'node_name', ''):
                        connected_peer_name = message.sender
                        # Register writer for this peer so future messages route correctly
                        self._peers[connected_peer_name] = (reader, writer)
                        self._writer_to_peer[id(writer)] = connected_peer_name
                        # FIX: Clear backoff and retry count for incoming connections too
                        self._peer_backoff.pop(connected_peer_name, None)
                        self._peer_retry_count[connected_peer_name] = 0
                        # Start priority send queue for this peer
                        self._ensure_send_queue(connected_peer_name)
                        log.info(f"Incoming P2P connection identified as peer: {connected_peer_name} (backoff cleared)")
                        if self._peer_connected_callback:
                            try:
                                asyncio.create_task(self._peer_connected_callback(connected_peer_name))
                            except Exception as e:
                                log.debug(f"Peer connected callback error for incoming {connected_peer_name}: {e}")

                    # Auto-ACK: send ACK back for non-heartbeat messages via P2P
                    if message.sender != getattr(self.config, 'node_name', ''):
                        asyncio.create_task(self._send_ack(message, writer, connected_peer_name))

                    # Queue for processing
                    await self._incoming_queue.put((message, "p2p"))

                except asyncio.TimeoutError:
                    # No data received within HEARTBEAT_INTERVAL — send heartbeat
                    idle_time = time.time() - last_received
                    if idle_time > self.HEARTBEAT_TIMEOUT:
                        log.warning(f"Peer {connected_peer_name or peer_addr} idle for {idle_time:.0f}s — closing connection")
                        break
                    # Send heartbeat if we have a peer name
                    if self._running:
                        await self._send_heartbeat(writer, connected_peer_name)

        except asyncio.IncompleteReadError:
            log.debug(f"Connection closed by peer {peer_addr}")
        except ConnectionResetError:
            log.debug(f"Connection reset by peer {peer_addr}")
        except Exception as e:
            log.error(f"Connection error from {peer_addr}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            # Remove dead peer from _peers dict so send() doesn't try to use it
            removed = None
            for pname, (pr, pw) in list(self._peers.items()):
                if pw is writer:
                    removed = self._peers.pop(pname, None)
                    self._writer_to_peer.pop(id(writer), None)
                    self._peer_connected_at.pop(pname, None)  # P2: Clean up age tracking
                    # P2: Clean up connection task tracking
                    self._connection_tasks.pop(pname, None)
                    # Clean up priority send queue for disconnected peer
                    send_task = self._send_tasks.pop(pname, None)
                    if send_task and not send_task.done():
                        send_task.cancel()
                    self._send_queues.pop(pname, None)
                    log.info(f"Peer {pname} disconnected, removed from peers dict")
                    break
            if removed is None:
                log.debug(f"Connection from {peer_addr} closed (was not in peers dict)")

    async def _send_heartbeat(self, writer: asyncio.StreamWriter, peer_name: Optional[str]):
        """Send a heartbeat ping to keep the connection alive."""
        try:
            hb_msg = A2AMessage.create(
                sender=getattr(self.config, 'node_name', ''),
                recipient=peer_name or '',
                msg_type=MSG_TYPE_HEARTBEAT,
                priority=0,
                payload={"ts": time.time()},
            )
            data = hb_msg.to_bytes()
            writer.write(encode_frame(data))
            await writer.drain()
            log.debug(f"Heartbeat sent to {peer_name or 'unknown'}")
        except Exception as e:
            log.warning(f"Heartbeat send failed to {peer_name}: {e}")
            raise  # Let the caller handle the broken connection

    async def _send_ack(self, original_message: A2AMessage, writer: asyncio.StreamWriter, peer_name: Optional[str]):
        """Send an ACK message back via P2P to the sender."""
        try:
            ack_msg = A2AMessage.create(
                sender=getattr(self.config, 'node_name', ''),
                recipient=original_message.sender,
                msg_type=MSG_TYPE_ACK,
                priority=min(original_message.priority + 1, 10),
                payload={
                    "ack_for": original_message.id,
                    "ack_type": "delivered",
                    "original_type": original_message.type,
                    "original_sender": original_message.sender,
                    "timestamp": time.time(),
                    "error": "",
                },
            )

            data = ack_msg.to_bytes()
            payload_bytes = encode_frame(data)

            writer.write(payload_bytes)
            await writer.drain()
            log.info(f"P2P ACK sent for {original_message.id[:8]} → {original_message.sender}")
        except Exception as e:
            log.warning(f"Failed to send P2P ACK for {original_message.id[:8]}: {e}")

    def set_ack_callback(self, callback):
        """Set callback invoked when an ACK is received.

        Callback signature: async def callback(ack_for_id: str, ack_type: str)
        """
        self._ack_callback = callback

    def set_peer_connected_callback(self, callback):
        """Set callback invoked when a P2P connection is established (including reconnects).

        Callback signature: async def callback(peer_name: str)
        """
        self._peer_connected_callback = callback

    async def _connect_to_peer(self, name: str, host: str, port: int):
        """Connect to a known peer with exponential backoff + jitter.
        
        Optimizations:
        - Nagle disabled (TCP_NODELAY) for low-latency message delivery
        - TCP keepalive for faster dead-connection detection
        - Connection timeout to prevent hanging on unreachable peers
        - Max connections enforced
        - Connection age tracked for periodic reconnection
        """
        import socket
        try:
            # Enforce max connections limit
            if len(self._peers) >= self._max_connections and name not in self._peers:
                log.warning(f"P2P max connections ({self._max_connections}) reached, cannot connect to {name}")
                return
            # Use TLS ssl_context for client connections if configured
            ssl_ctx = self._ssl_client_context or self._ssl_context
            # Connection timeout to prevent hanging on unreachable peers
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl_ctx),
                timeout=self.CONNECT_TIMEOUT
            )

            # ── Socket tuning for low-latency P2P ──
            sock = writer.get_extra_info('socket')
            if sock:
                try:
                    # Disable Nagle's algorithm — send small messages immediately
                    # Critical for heartbeat/ACK latency (~40ms improvement per hop)
                    if self.NAGLE_DISABLED:
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    
                    # TCP keepalive for faster dead-connection detection
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    if hasattr(socket, 'TCP_KEEPIDLE'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)   # Start probes after 30s idle (was 60)
                    if hasattr(socket, 'TCP_KEEPINTVL'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)  # Probe every 10s
                    if hasattr(socket, 'TCP_KEEPCNT'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)    # 3 failed probes = dead
                    
                    # Increase send/receive buffer sizes for throughput
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 256 * 1024)  # 256KB send buffer
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256 * 1024)  # 256KB recv buffer
                    
                    log.debug(f"Socket tuned for peer {name}: TCP_NODELAY, keepalive, buffers")
                except (OSError, AttributeError) as e:
                    log.debug(f"Could not tune socket for {name}: {e}")
            self._peers[name] = (reader, writer)
            self._peer_addresses[name] = f"{host}:{port}"
            self._writer_to_peer[id(writer)] = name
            # P2: Track connection start time for age-based reconnection
            self._peer_connected_at[name] = time.time()
            # Reset retry count on success
            self._peer_retry_count[name] = 0
            self._peer_backoff.pop(name, None)
            # Start priority send queue for this peer
            self._ensure_send_queue(name)
            log.info(f"Connected to peer {name} at {host}:{port}")

            # Notify peer_discovery that a peer connected (for registry registration)
            if self._peer_connected_callback:
                try:
                    asyncio.create_task(self._peer_connected_callback(name))
                except Exception as e:
                    log.debug(f"Peer connected callback error for {name}: {e}")

            # Start reading from this peer
            # P2 FIX: Cancel any existing connection task for this peer to prevent duplicates
            old_task = self._connection_tasks.get(name)
            if old_task and not old_task.done():
                old_task.cancel()
                log.debug(f"Cancelled existing connection task for {name}")
            self._connection_tasks[name] = asyncio.create_task(self._handle_connection(reader, writer))

            # Flush retry queue: resend any queued messages to this peer
            self._flush_retry_queue_for_peer(name)

        except asyncio.TimeoutError:
            retry_count = self._peer_retry_count.get(name, 0) + 1
            self._peer_retry_count[name] = retry_count
            # Exponential backoff with jitter: 5s ± 2.5s, 10s ± 5s, 20s ± 10s, ... max 5min
            base_backoff = min(self._reconnect_interval * (2 ** (retry_count - 1)), self.MAX_BACKOFF)
            jitter = self._backoff_random.uniform(0, base_backoff * self.INITIAL_BACKOFF_JITTER)
            backoff = base_backoff + jitter
            next_retry = time.time() + backoff
            self._peer_backoff[name] = next_retry
            log.warning(f"P2P connection to {name} at {host}:{port} TIMED OUT (retry #{retry_count}, next in {backoff:.1f}s)")
        except Exception as e:
            retry_count = self._peer_retry_count.get(name, 0) + 1
            self._peer_retry_count[name] = retry_count
            # Exponential backoff with jitter: prevents thundering herd on network recovery
            base_backoff = min(self._reconnect_interval * (2 ** (retry_count - 1)), self.MAX_BACKOFF)
            jitter = self._backoff_random.uniform(0, base_backoff * self.INITIAL_BACKOFF_JITTER)
            backoff = base_backoff + jitter
            next_retry = time.time() + backoff
            self._peer_backoff[name] = next_retry
            log.warning(f"Failed to connect to {name} at {host}:{port} (retry #{retry_count}, next in {backoff:.1f}s): {e}")

    # ─── Priority Send Queue ────────────────────────────────────────

    # Message priority mapping (lower = higher priority, sent first)
    MSG_PRIORITY = {
        MSG_TYPE_HEARTBEAT: 1,   # Heartbeats must go first
        MSG_TYPE_ACK: 2,        # ACKs second (delivery confirmations)
        "discovery": 3,          # Discovery/registry
        "election": 3,           # Leader election
        "mesh": 3,               # Mesh control
        "chat": 5,               # Normal data messages
        "directive": 5,
        "task": 5,
        "result": 5,
        "a2a_message": 5,
        "delegation": 5,
        "context": 5,
        "broadcast": 5,
        "agent_reply": 5,
        "steer": 5,
        "skills_announcement": 5,
        "file_transfer": 7,      # File chunks lowest priority (bulk data)
    }
    DEFAULT_PRIORITY = 5

    @classmethod
    def _message_priority(cls, message: A2AMessage) -> int:
        """Get send priority for a message (lower = higher priority)."""
        # Use message.priority if explicitly set and not default
        if hasattr(message, 'priority') and message.priority is not None:
            # Map message.priority (1-10 scale) to send priority
            # Lower message.priority = more important = lower send priority number
            if message.priority <= 2:
                return 1  # Critical: heartbeats, ACKs
            elif message.priority <= 4:
                return 3  # High: control messages
            elif message.priority <= 6:
                return 5  # Normal: data messages
            else:
                return 7  # Low: bulk data (file chunks)
        # Fall back to type-based priority
        return cls.MSG_PRIORITY.get(message.type, cls.DEFAULT_PRIORITY)

    def _ensure_send_queue(self, peer_name: str):
        """Create a send queue for a peer if it doesn't exist."""
        if peer_name not in self._send_queues:
            self._send_queues[peer_name] = asyncio.PriorityQueue()
            task = asyncio.create_task(self._send_loop(peer_name))
            self._send_tasks[peer_name] = task
            log.debug(f"Created priority send queue for peer {peer_name}")

    async def _send_loop(self, peer_name: str):
        """Background task: continuously send messages from the peer's priority queue.

        This ensures ordered delivery per-priority-tier while allowing
        high-priority messages (heartbeats, ACKs) to preempt bulk data.
        
        Optimizations:
        - Write batching: collects up to WRITE_BATCH_SIZE frames before draining
        - Nagle-friendly: only batch non-urgent messages, drain immediately for priority 1-2
        """
        queue = self._send_queues.get(peer_name)
        if queue is None:
            return

        seq = 0  # Sequence counter for FIFO ordering within same priority
        while self._running:
            try:
                # Wait for next message with timeout (check running flag periodically)
                try:
                    priority, _seq, message = await asyncio.wait_for(
                        queue.get(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    # Flush any pending write batch even if no new messages
                    await self._flush_write_batch(peer_name)
                    continue

                # Get writer for this peer
                peer = self._peers.get(peer_name)
                if peer is None:
                    # Peer disconnected — put message back in retry queue
                    self._enqueue_for_retry(message)
                    continue

                _, writer = peer
                try:
                    data = message.to_bytes()
                    payload = encode_frame(data)
                    
                    # High-priority messages (heartbeat=1, ACK=2): send immediately
                    # Lower-priority messages (3-7): batch for efficiency
                    if priority <= 2:
                        # Drain any pending batch first (to maintain order)
                        await self._flush_write_batch(peer_name)
                        writer.write(payload)
                        await writer.drain()
                    else:
                        # Add to write batch for this peer
                        if peer_name not in self._write_batch:
                            self._write_batch[peer_name] = []
                        self._write_batch[peer_name].append(payload)
                        
                        # Flush if batch is full or this is a file chunk (priority 7)
                        if len(self._write_batch[peer_name]) >= self.WRITE_BATCH_SIZE or priority >= 7:
                            await self._flush_write_batch(peer_name)
                        elif len(self._write_batch[peer_name]) == 1:
                            # First in batch — schedule a flush after short delay
                            # This collects more messages before draining
                            if peer_name not in self._write_batch_task or self._write_batch_task[peer_name].done():
                                self._write_batch_task[peer_name] = asyncio.create_task(
                                    self._delayed_flush(peer_name)
                                )
                    
                    log.debug(f"Sent msg {message.id[:8]} type={message.type} pri={priority} to {peer_name}")
                except Exception as e:
                    log.warning(f"Failed to send msg {message.id[:8]} to {peer_name}: {e}")
                    # Remove broken connection
                    self._peers.pop(peer_name, None)
                    self._writer_to_peer.pop(id(writer), None)
                    # Queue for retry
                    self._enqueue_for_retry(message)
                    break  # Exit send loop for this peer — _handle_connection cleanup will handle

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Send loop error for {peer_name}: {e}")
                await asyncio.sleep(1)

        log.debug(f"Send loop ended for peer {peer_name}")

    async def _delayed_flush(self, peer_name: str):
        """Flush write batch after a short delay to collect more messages."""
        await asyncio.sleep(self.WRITE_BATCH_TIMEOUT)
        await self._flush_write_batch(peer_name)

    async def _flush_write_batch(self, peer_name: str):
        """Flush all pending write frames for a peer in a single drain()."""
        batch = self._write_batch.pop(peer_name, None)
        if not batch:
            return
        
        # Cancel pending flush task if any
        task = self._write_batch_task.pop(peer_name, None)
        if task and not task.done():
            task.cancel()
        
        peer = self._peers.get(peer_name)
        if peer is None:
            return  # Peer disconnected
        _, writer = peer
        
        try:
            for payload in batch:
                writer.write(payload)
            await writer.drain()
        except Exception as e:
            log.warning(f"Flush write batch failed for {peer_name}: {e}")

    def _enqueue_send(self, peer_name: str, message: A2AMessage):
        """Enqueue a message for priority-based sending to a peer.

        Thread-safe: can be called from any asyncio task.
        """
        self._ensure_send_queue(peer_name)
        priority = self._message_priority(message)
        # Monotonically increasing sequence number for FIFO within same priority
        self._send_seq += 1
        self._send_queues[peer_name].put_nowait((priority, self._send_seq, message))

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message to peer(s) via priority queue.

        Uses per-peer priority queues to ensure heartbeats and ACKs are sent
        before bulk data (file chunks). Messages are enqueued and a background
        send loop delivers them in priority order.

        P2: If the recipient is not currently connected, attempt dynamic connection
        via peer_address_resolver (which queries peer_discovery for known addresses).
        """
        if not self._available:
            return SendResult(transport="p2p", success=False, error="not started")

        # If directed message, enqueue to specific peer's priority queue
        if not message.is_broadcast():
            recipient = message.recipient

            # P2: If recipient is connected, enqueue to their priority queue
            if recipient in self._peers:
                self._enqueue_send(recipient, message)
                return SendResult(transport="p2p", success=True, latency_ms=1.0)

            # P2: Recipient not connected — try dynamic connection via peer_address_resolver
            if self._peer_address_resolver and recipient not in self._connecting_peers:
                addr = self._peer_address_resolver(recipient)
                if addr:
                    host, port = addr
                    log.info(f"P2 dynamic connect: resolving {recipient} → {host}:{port}")
                    self._connecting_peers.add(recipient)
                    try:
                        await self._connect_to_peer(recipient, host, port)
                    except Exception as e:
                        log.warning(f"P2 dynamic connect to {recipient} failed: {e}")
                        self._enqueue_for_retry(message)
                        return SendResult(transport="p2p", success=False, error=f"dynamic connect failed: {e}")
                    finally:
                        self._connecting_peers.discard(recipient)
                    # After successful connect, enqueue to priority queue
                    if recipient in self._peers:
                        self._enqueue_send(recipient, message)
                        return SendResult(transport="p2p", success=True, latency_ms=5.0)

            # No address resolver or address not found — queue for retry
            self._enqueue_for_retry(message)
            return SendResult(transport="p2p", success=False, error=f"peer {recipient} not connected, no address")

        # Broadcast: enqueue to all connected peers' priority queues
        peer_names = list(self._peers.keys())
        for peer_name in peer_names:
            self._enqueue_send(peer_name, message)

        if peer_names:
            return SendResult(transport="p2p", success=True, latency_ms=2.0)

        # No peers connected — queue for retry only if NOT a broadcast
        if not message.is_broadcast():
            self._enqueue_for_retry(message)
        return SendResult(transport="p2p", success=False, error="no peers connected")

    def _enqueue_for_retry(self, message: A2AMessage):
        """Add a message to the retry queue for later delivery.
        
        P2 fix: Dedup by message ID to prevent exponential queue growth.
        P2 fix: Cap queue size to prevent unbounded memory growth.
        """
        # Don't queue heartbeats or ACKs
        if message.type in (MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK):
            return
        # P2 cap: drop oldest if queue is full
        if len(self._retry_queue) >= self._max_retry_queue_size:
            oldest_msg, oldest_age = self._retry_queue.pop(0)
            log.warning(f"Retry queue full ({self._max_retry_queue_size}), dropping oldest {oldest_msg.id[:8]}")
        # P2 dedup: skip if message is already in the queue
        for existing_msg, _ in self._retry_queue:
            if existing_msg.id == message.id:
                return
        self._retry_queue.append((message, time.time()))
        log.debug(f"Queued message {message.id[:8]} for retry ({len(self._retry_queue)} queued)")

    def _flush_retry_queue_for_peer(self, peer_name: str):
        """Attempt to resend queued messages to a newly connected peer via priority queue."""
        if not self._retry_queue:
            return

        # Only flush if peer is actually connected
        if peer_name not in self._peers:
            return

        # Filter messages for this peer (directed to this peer, or broadcasts)
        to_resend = []
        remaining = []
        for msg, queued_at in self._retry_queue:
            if msg.recipient == peer_name or msg.is_broadcast():
                to_resend.append((msg, queued_at))
            else:
                remaining.append((msg, queued_at))

        self._retry_queue = remaining

        # Enqueue all matching messages into the peer's priority queue
        # The send loop will handle actual delivery
        for msg, queued_at in to_resend:
            age = time.time() - queued_at
            log.info(f"Retry: enqueuing queued message {msg.id[:8]} to {peer_name} (was queued {age:.0f}s ago)")
            self._enqueue_send(peer_name, msg)

    async def receive(self) -> list:
        """Get received messages from the queue."""
        messages = []
        while not self._incoming_queue.empty():
            msg, transport = await self._incoming_queue.get()
            messages.append((msg, transport))
        return messages

    async def discover(self) -> list:
        """Return connected peer info with P2 connection age metrics."""
        from .base import TransportStatus
        now = time.time()
        peers = []
        for name, (reader, writer) in self._peers.items():
            connected_at = self._peer_connected_at.get(name, 0)
            age = now - connected_at if connected_at else 0
            peers.append({
                "name": name,
                "address": self._peer_addresses.get(name, "unknown"),
                "transport": "p2p",
                "connected_s": round(age, 1),
                "max_age_s": self._connection_age_limit,
            })
        return peers

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        avg_latency = 1.0  # Default estimate for P2P
        if self._peer_latency:
            avg_latency = sum(self._peer_latency.values()) / len(self._peer_latency)
        return TransportStatus(
            available=self._available,
            latency_ms=avg_latency if self._available else float('inf'),
            error="" if self._available else "not started",
        )

    def get_peer_count(self) -> int:
        """Return number of connected peers."""
        return len(self._peers)

    def get_peer_latency(self, peer_name: str) -> float:
        """Return estimated RTT in ms for a peer (0 if unknown)."""
        return self._peer_latency.get(peer_name, 0.0)

    def get_peer_stats(self) -> dict:
        """Return detailed per-peer stats for routing decisions."""
        now = time.time()
        stats = {}
        for name in self._peers:
            connected_at = self._peer_connected_at.get(name, 0)
            stats[name] = {
                "address": self._peer_addresses.get(name, "unknown"),
                "connected_s": round(now - connected_at, 1) if connected_at else 0,
                "rtt_ms": round(self._peer_latency.get(name, 0), 1),
                "queue_size": self._send_queues[name].qsize() if name in self._send_queues else 0,
            }
        return stats

    def get_retry_queue_size(self) -> int:
        """Return number of messages waiting in the retry queue."""
        return len(self._retry_queue)

    async def _reconnect_loop(self):
        """Periodically check for disconnected peers and attempt reconnection.

        P2: Also resolves peers via peer_address_resolver for peers that were
        discovered but never connected (e.g., via PG/mDNS discovery).
        """
        while self._running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds for reconnect
                if not self._running:
                    break

                now = time.time()

                # P2: Collect all known peers that should be reconnected
                # Start with known addresses (previously connected)
                peers_to_check = set(self._peer_addresses.keys())

                # P2: Also check peers known via peer_discovery (resolver)
                if self._peer_address_resolver:
                    try:
                        all_peers = self._peer_address_resolver.__self__.get_all_peers() if hasattr(self._peer_address_resolver, '__self__') else {}
                        for pname in all_peers:
                            if pname != getattr(self.config, 'node_name', ''):
                                peers_to_check.add(pname)
                    except Exception:
                        pass

                for name in peers_to_check:
                    if name in self._peers:
                        continue  # Already connected
                    if name in self._connecting_peers:
                        continue  # Connection attempt in progress

                    # Check backoff
                    next_retry = self._peer_backoff.get(name, 0)
                    if next_retry < 0:
                        log.warning(f"P2P backoff for {name} is negative ({next_retry:.0f}s), resetting to now")
                        self._peer_backoff.pop(name, None)
                        next_retry = 0
                    if now < next_retry:
                        continue  # Not time yet

                    # Get address: try known addresses first, then resolver
                    address = self._peer_addresses.get(name)
                    if not address and self._peer_address_resolver:
                        resolved = self._peer_address_resolver(name)
                        if resolved:
                            r_host, r_port = resolved
                            address = f"{r_host}:{r_port}"
                    
                    if not address:
                        continue  # No address available

                    addr_host, addr_port_str = address.rsplit(":", 1)
                    addr_port = int(addr_port_str)
                    log.info(f"Reconnecting to peer {name} at {address}")
                    await self._connect_to_peer(name, addr_host, addr_port)

                # P2: Check connection age — reconnect stale connections
                now = time.time()
                for name in list(self._peer_connected_at.keys()):
                    if name not in self._peers:
                        self._peer_connected_at.pop(name, None)
                        continue
                    connected_at = self._peer_connected_at[name]
                    age = now - connected_at
                    if age > self._connection_age_limit:
                        log.info(f"P2P connection to {name} is {age:.0f}s old (limit: {self._connection_age_limit}s) — reconnecting")
                        # Close old connection — reconnect loop will re-establish
                        peer_data = self._peers.pop(name, None)
                        self._peer_connected_at.pop(name, None)
                        if peer_data:
                            _, old_writer = peer_data
                            try:
                                old_writer.close()
                                await old_writer.wait_closed()
                            except Exception:
                                pass

                # Process retry queue: try sending queued messages if any peers are connected
                if self._retry_queue and self._peers:
                    await self._process_retry_queue()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Reconnect loop error: {e}")

    async def _process_retry_queue(self):
        """Try to resend queued messages to any available peer.
        
        P2 fix: Skip messages already in the queue (dedup by message ID)
        to prevent exponential growth when send() re-enqueues.
        """
        if not self._retry_queue:
            return
        queue = self._retry_queue
        self._retry_queue = []
        seen_ids = set()  # P2: dedup — don't retry if already re-queued
        for msg, queued_at in queue:
            age = time.time() - queued_at
            # Don't retry messages older than 1 hour
            if age > 3600:
                log.warning(f"Discarding queued message {msg.id[:8]} (too old: {age:.0f}s)")
                continue
            if msg.id in seen_ids:
                continue  # P2: already re-queued in this cycle
            result = await self.send(msg)
            if not result.success:
                log.debug(f"Retry still failing for {msg.id[:8]}: {result.error}")
                # P2 FIX: Use _enqueue_for_retry() which handles dedup and cap
                # Old code manually appended, causing duplicates with send() re-enqueue
                self._enqueue_for_retry(msg)
                seen_ids.add(msg.id)