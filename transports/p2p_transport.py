"""A2A Mesh TCP P2P Transport — Direct TCP peer-to-peer connections.

Each agent runs a TCP server that other agents can connect to.
Messages are length-prefixed (4 bytes big-endian) + msgpack/JSON.
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

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult, MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT

log = logging.getLogger("a2a_mesh.transports.p2p")


class P2PTransport(TransportAdapter):
    """TCP P2P transport for direct agent-to-agent communication.

    Features:
    - Async TCP server (listen for incoming connections)
    - Async TCP client (connect to known peers)
    - Length-prefixed message framing
    - Auto-reconnect on connection loss
    - mDNS discovery integration
    """

    name = "p2p"

    def __init__(self, config):
        self.config = config
        self._available = False
        self._server: Optional[asyncio.Server] = None
        self._peers: Dict[str, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._peer_addresses: Dict[str, str] = {}  # peer_name → host:port
        self._peer_backoff: Dict[str, float] = {}  # peer_name → next retry timestamp
        self._peer_retry_count: Dict[str, int] = {}  # peer_name → consecutive failure count
        self._reconnect_task: Optional[asyncio.Task] = None
        self._incoming_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._listen_host = config.p2p.listen_host
        self._listen_port = config.p2p.listen_port
        self._message_callback = None
        self._max_retries = config.p2p.max_connections  # reuse as max reconnect attempts
        self._reconnect_interval = config.p2p.idle_timeout  # base retry interval in seconds
        self._ssl_context: Optional[ssl.SSLContext] = None  # TLS server context (for incoming connections)
        self._ssl_client_context: Optional[ssl.SSLContext] = None  # TLS client context (for outgoing connections)

        # Retry queue: messages that failed to send, will be retried on reconnect
        self._retry_queue: List[Tuple[A2AMessage, float]] = []  # (message, queued_at)
        self._retry_interval = 10  # seconds between retry attempts

        # ACK callback: called when an ACK message is received, so sender can update PG
        self._ack_callback = None  # Callable[[str, str], Awaitable[None]] (ack_for_id, ack_type)

        # Reverse-lookup: map writer → peer_name for incoming ACK routing
        self._writer_to_peer: Dict[int, str] = {}  # id(writer) → peer_name

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
            log.warning("P2P TLS enabled but no cert/key configured — falling back to plain TCP")

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
                # Port already bound — likely by peer discovery or another instance
                log.warning(f"P2P port {self._listen_port} already in use — assuming existing server")
                self._running = True
                self._available = True
                # Start reconnection monitor anyway
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
        """Handle incoming TCP connection."""
        peer_addr = writer.get_extra_info('peername')
        log.debug(f"New connection from {peer_addr}")

        # Try to identify which peer this connection is from
        connected_peer_name = None
        for pname, (pr, pw) in self._peers.items():
            if pw is writer:
                connected_peer_name = pname
                break

        try:
            while self._running:
                # Read length prefix (4 bytes, big-endian)
                length_data = await reader.readexactly(4)
                length = struct.unpack('>I', length_data)[0]

                # Sanity check: max 10MB message
                if length > 10 * 1024 * 1024:
                    log.warning(f"Message too large ({length} bytes), dropping connection")
                    break

                # Read message data
                data = await reader.readexactly(length)
                message = A2AMessage.from_bytes(data)

                # Handle ACK messages: don't re-queue, just invoke callback
                if message.type == MSG_TYPE_ACK:
                    payload = message.payload if isinstance(message.payload, dict) else {}
                    ack_for_id = payload.get("ack_for", "")
                    ack_type = payload.get("ack_type", "delivered")
                    log.info(f"P2P ACK received for message {ack_for_id[:8]} from {message.sender}: {ack_type}")
                    if self._ack_callback and ack_for_id:
                        try:
                            asyncio.create_task(self._ack_callback(ack_for_id, ack_type))
                        except Exception as e:
                            log.error(f"ACK callback error: {e}")
                    # Also enqueue so node-level handlers can process it
                    await self._incoming_queue.put((message, "p2p"))
                    continue

                # Auto-ACK: send ACK back for non-heartbeat messages via P2P
                if message.type != MSG_TYPE_HEARTBEAT and message.sender != getattr(self.config, 'node_name', ''):
                    asyncio.create_task(self._send_ack(message, writer, connected_peer_name))

                # Queue for processing
                await self._incoming_queue.put((message, "p2p"))

        except asyncio.IncompleteReadError:
            log.debug(f"Connection closed by peer {peer_addr}")
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
                    log.info(f"Peer {pname} disconnected, removed from peers dict")
                    break
            if removed is None:
                log.debug(f"Connection from {peer_addr} closed (was not in peers dict)")

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
            length_prefix = struct.pack('>I', len(data))
            payload_bytes = length_prefix + data

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

    async def _connect_to_peer(self, name: str, host: str, port: int):
        """Connect to a known peer with exponential backoff.
        P1: Sets TCP keepalive on the socket for faster dead-connection detection."""
        import socket
        try:
            # Use TLS ssl_context for client connections if configured
            reader, writer = await asyncio.open_connection(host, port, ssl=self._ssl_client_context or self._ssl_context)

            # P1: Set TCP keepalive for faster dead-connection detection
            sock = writer.get_extra_info('socket')
            if sock:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    # macOS/Linux TCP keepalive settings (may not exist on all platforms)
                    if hasattr(socket, 'TCP_KEEPIDLE'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)   # Start probes after 60s idle
                    if hasattr(socket, 'TCP_KEEPINTVL'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)  # Probe every 10s
                    if hasattr(socket, 'TCP_KEEPCNT'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)    # 3 failed probes = dead
                    log.debug(f"TCP keepalive enabled for peer {name}")
                except (OSError, AttributeError) as e:
                    log.debug(f"Could not set TCP keepalive for {name}: {e}")
            self._peers[name] = (reader, writer)
            self._peer_addresses[name] = f"{host}:{port}"
            self._writer_to_peer[id(writer)] = name
            # Reset retry count on success
            self._peer_retry_count[name] = 0
            self._peer_backoff.pop(name, None)
            log.info(f"Connected to peer {name} at {host}:{port}")

            # Start reading from this peer
            asyncio.create_task(self._handle_connection(reader, writer))

            # Flush retry queue: resend any queued messages to this peer
            self._flush_retry_queue_for_peer(name)

        except Exception as e:
            retry_count = self._peer_retry_count.get(name, 0) + 1
            self._peer_retry_count[name] = retry_count
            # Exponential backoff: 5s, 10s, 20s, 40s... max 5min
            backoff = min(self._reconnect_interval * (2 ** (retry_count - 1)), 300)
            next_retry = time.time() + backoff
            self._peer_backoff[name] = next_retry
            log.debug(f"Failed to connect to {name} at {host}:{port} (retry #{retry_count}, next in {backoff:.0f}s): {e}")

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message to peer(s) via TCP."""
        if not self._available:
            return SendResult(transport="p2p", success=False, error="not started")

        data = message.to_bytes()
        length_prefix = struct.pack('>I', len(data))
        payload = length_prefix + data

        # If directed message, send to specific peer
        if not message.is_broadcast() and message.recipient in self._peers:
            _, writer = self._peers[message.recipient]
            try:
                writer.write(payload)
                await writer.drain()
                return SendResult(transport="p2p", success=True, latency_ms=1.0)
            except Exception as e:
                log.warning(f"Failed to send to {message.recipient}: {e}")
                # Remove broken connection
                self._peers.pop(message.recipient, None)
                # Queue for retry
                self._enqueue_for_retry(message)
                return SendResult(transport="p2p", success=False, error=str(e))

        # Broadcast: send to all connected peers
        successes = 0
        for peer_name, (reader, writer) in list(self._peers.items()):
            try:
                writer.write(payload)
                await writer.drain()
                successes += 1
            except Exception as e:
                log.warning(f"Failed to send to {peer_name}: {e}")
                self._peers.pop(peer_name, None)

        if successes > 0:
            return SendResult(transport="p2p", success=True, latency_ms=2.0)

        # No peers connected — queue for retry only if NOT a broadcast
        # (broadcast already succeeded via PG, don't queue duplicates)
        if not message.is_broadcast():
            self._enqueue_for_retry(message)
        return SendResult(transport="p2p", success=False, error="no peers connected")

    def _enqueue_for_retry(self, message: A2AMessage):
        """Add a message to the retry queue for later delivery."""
        # Don't queue heartbeats or ACKs
        if message.type in (MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK):
            return
        self._retry_queue.append((message, time.time()))
        log.info(f"Queued message {message.id[:8]} for retry ({len(self._retry_queue)} queued)")

    def _flush_retry_queue_for_peer(self, peer_name: str):
        """Attempt to resend queued messages to a newly connected peer."""
        if not self._retry_queue:
            return

        # Get writer for this peer
        if peer_name not in self._peers:
            return
        _, writer = self._peers[peer_name]

        # Filter messages for this peer (directed to this peer, or broadcasts)
        to_resend = []
        remaining = []
        for msg, queued_at in self._retry_queue:
            if msg.recipient == peer_name or msg.is_broadcast():
                to_resend.append((msg, queued_at))
            else:
                remaining.append((msg, queued_at))

        self._retry_queue = remaining

        if to_resend:
            async def _resend():
                for msg, queued_at in to_resend:
                    try:
                        data = msg.to_bytes()
                        length_prefix = struct.pack('>I', len(data))
                        writer.write(length_prefix + data)
                        await writer.drain()
                        age = time.time() - queued_at
                        log.info(f"Retry: sent queued message {msg.id[:8]} to {peer_name} (was queued {age:.0f}s ago)")
                    except Exception as e:
                        log.warning(f"Retry failed for {msg.id[:8]} to {peer_name}: {e}")
                        # Re-queue if still failing
                        self._enqueue_for_retry(msg)
                        break

            asyncio.create_task(_resend())

    async def receive(self) -> list:
        """Get received messages from the queue."""
        messages = []
        while not self._incoming_queue.empty():
            msg, transport = await self._incoming_queue.get()
            messages.append((msg, transport))
        return messages

    async def discover(self) -> list:
        """Return connected peer info."""
        from .base import TransportStatus
        peers = []
        for name, (reader, writer) in self._peers.items():
            peers.append({
                "name": name,
                "address": self._peer_addresses.get(name, "unknown"),
                "transport": "p2p",
            })
        return peers

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        return TransportStatus(
            available=self._available,
            latency_ms=1.0 if self._available else float('inf'),
            error="" if self._available else "not started",
        )

    def get_peer_count(self) -> int:
        """Return number of connected peers."""
        return len(self._peers)

    def get_retry_queue_size(self) -> int:
        """Return number of messages waiting in the retry queue."""
        return len(self._retry_queue)

    async def _reconnect_loop(self):
        """Periodically check for disconnected peers and attempt reconnection."""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds for fast reconnect
                if not self._running:
                    break

                now = time.time()
                for name, address in list(self._peer_addresses.items()):
                    if name not in self._peers:
                        # Peer is disconnected — check backoff
                        next_retry = self._peer_backoff.get(name, 0)
                        if now >= next_retry:
                            host, port_str = address.rsplit(":", 1)
                            port = int(port_str)
                            log.info(f"Reconnecting to peer {name} at {address}")
                            await self._connect_to_peer(name, host, port)

                # Process retry queue: try sending queued messages if any peers are connected
                if self._retry_queue and self._peers:
                    await self._process_retry_queue()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Reconnect loop error: {e}")

    async def _process_retry_queue(self):
        """Try to resend queued messages to any available peer."""
        if not self._retry_queue:
            return
        queue = self._retry_queue
        self._retry_queue = []
        for msg, queued_at in queue:
            age = time.time() - queued_at
            # Don't retry messages older than 1 hour
            if age > 3600:
                log.warning(f"Discarding queued message {msg.id[:8]} (too old: {age:.0f}s)")
                continue
            result = await self.send(msg)
            if not result.success:
                log.debug(f"Retry still failing for {msg.id[:8]}: {result.error}")
                # Re-queue for next cycle
                self._retry_queue.append((msg, queued_at))