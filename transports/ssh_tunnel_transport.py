"""A2A Mesh SSH Tunnel Transport — P2P over SSH port forwarding.

When direct P2P TLS fails (e.g. asyncio TLS quirks on some platforms),
this transport creates an SSH tunnel to the peer and runs the same P2P
frame protocol over the tunnel's local TCP endpoint.

Architecture:
  Local node → SSH (-L local_port:localhost:remote_port) → Remote node P2P port
  Local asyncio.open_connection(localhost, local_port) → tunnel → peer

The frame protocol (v2/v3) is identical to P2P transport — only the
transport layer differs (SSH tunnel vs direct TCP+TLS).
"""

import asyncio
import logging
import os
import socket
import struct
import time
import zlib
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult, MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT

log = logging.getLogger("a2a_mesh.transports.ssh_tunnel")

# Frame format constants (must match p2p_transport.py)
FRAME_V2_MAGIC = 0x02
FRAME_V3_MAGIC = 0x03
FRAME_V3_COMPRESSED = 0x01
MAX_FRAME_SIZE = 1024 * 1024  # 1MB


@dataclass
class TunnelPeer:
    """Represents an SSH tunnel connection to a peer."""
    name: str
    ssh_host: str
    ssh_user: str
    ssh_port: int
    remote_port: int
    identity_file: str
    local_port: int
    process: Optional[asyncio.subprocess.Process] = None
    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None
    connected: bool = False
    connected_since: float = 0.0
    retry_count: int = 0
    last_connect_attempt: float = 0.0
    backoff: float = 10.0
    remote_name: str = ""
    # Per-peer SSH connect timeout (overrides global config.connect_timeout
    # when set — needed for slow links e.g. tor/HAOS where 15s is too short)
    connect_timeout: int = 0
    # Forward target host inside the SSH server's network namespace. Normally
    # 127.0.0.1 (sshd and node share the host). On multi-agent HAOS hosts the
    # peer's P2P listener is NOT on the sshd's loopback — the forward must
    # target the host IP instead (sshd runs in the tor container, node in mano).
    forward_host: str = ""


class SSHTunnelTransport(TransportAdapter):
    """SSH tunnel transport — P2P frame protocol over SSH port forwarding.

    Config example (mesh_config.yaml):
      transports:
        ssh_tunnel:
          enabled: true
          default_ssh_user: "zsolt"
          peers:
            runa:
              ssh_host: "192.168.1.100"
              ssh_user: "zsolt"
              ssh_port: 22
              remote_port: 8645
              identity_file: "~/.ssh/id_mesh"
            morzsa:
              ssh_host: "192.168.1.30"
              ssh_user: "openclaw"
              ssh_port: 22
              remote_port: 8645
    """

    def __init__(self, config, node_name: str = "", peer_discovery=None,
                 node_version: str = "",
                 on_message_callback=None, peer_connected_callback=None,
                 mesh_config=None):
        from ..core.config import SSHTunnelConfig
        self._config: SSHTunnelConfig = config
        self._node_name = node_name
        self._node_version = node_version
        self._peer_discovery = peer_discovery
        self._on_message_callback = on_message_callback
        self._peer_connected_callback = peer_connected_callback
        self._mesh_config = mesh_config  # Full MeshConfig for TLS settings

        self._started = False
        self._tunnels: Dict[str, TunnelPeer] = {}
        self._receive_queue: asyncio.Queue = asyncio.Queue()
        self._connection_tasks: Dict[str, asyncio.Task] = {}
        self._local_port_counter = config.local_port_start
        self._lock = asyncio.Lock()

        # TLS client context (reuse P2P TLS settings if available)
        self._ssl_client_context = None
        if mesh_config and getattr(mesh_config.p2p, 'tls_enabled', False):
            import ssl as _ssl
            tls_cert = os.path.expanduser(getattr(mesh_config.p2p, 'tls_cert', '') or '')
            tls_key = os.path.expanduser(getattr(mesh_config.p2p, 'tls_key', '') or '')
            tls_ca = os.path.expanduser(getattr(mesh_config.p2p, 'tls_ca', '') or '')
            tls_verify_peer = getattr(mesh_config.p2p, 'tls_verify_peer', False)
            if tls_cert and tls_key:
                try:
                    self._ssl_client_context = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
                    self._ssl_client_context.load_cert_chain(tls_cert, tls_key)
                    self._ssl_client_context.minimum_version = _ssl.TLSVersion.TLSv1_2
                    self._ssl_client_context.set_ciphers('ECDHE+AESGCM:DHE+AESGCM:ECDHE+CHACHA20')
                    if tls_ca:
                        self._ssl_client_context.load_verify_locations(tls_ca)
                    # SSH tunnel: always disable peer verification — SSH already
                    # authenticates the endpoint, and the tunnel's local port
                    # presents the peer's cert with a mismatched hostname.
                    self._ssl_client_context.check_hostname = False
                    self._ssl_client_context.verify_mode = _ssl.CERT_NONE
                    log.info("SSH tunnel TLS client context initialized (reusing P2P certs)")
                except Exception as e:
                    log.error(f"SSH tunnel TLS init failed: {e}")
                    self._ssl_client_context = None

        # Build tunnel peer configs from config
        for peer_name, peer_cfg in (config.peers or {}).items():
            self._tunnels[peer_name] = TunnelPeer(
                name=peer_name,
                ssh_host=peer_cfg.get("ssh_host", ""),
                ssh_user=peer_cfg.get("ssh_user", config.default_ssh_user),
                ssh_port=peer_cfg.get("ssh_port", 22),
                remote_port=peer_cfg.get("remote_port", 8645),
                identity_file=os.path.expanduser(
                    peer_cfg.get("identity_file", config.default_identity_file)
                ),
                local_port=0,  # assigned on connect
                connect_timeout=int(peer_cfg.get("connect_timeout", 0) or 0),
                forward_host=str(peer_cfg.get("forward_host", "") or ""),
            )

    @property
    def name(self) -> str:
        return "ssh_tunnel"

    async def start(self) -> bool:
        """Start SSH tunnel transport — establish tunnels to all configured peers."""
        if not self._config.enabled:
            log.info("SSH tunnel transport disabled, skipping start")
            return False

        if not self._tunnels:
            log.info("SSH tunnel transport enabled but no peers configured")
            return False

        self._started = True
        log.info(f"SSH tunnel transport starting with {len(self._tunnels)} peers")

        # Start connection tasks for each peer
        for peer_name in self._tunnels:
            self._connection_tasks[peer_name] = asyncio.create_task(
                self._maintain_tunnel_wrapper(peer_name)
            )

        return True

    async def stop(self) -> bool:
        """Shutdown all SSH tunnels cleanly."""
        log.info("SSH tunnel transport stopping...")
        self._started = False

        # Cancel connection maintenance tasks
        for task in self._connection_tasks.values():
            task.cancel()
        self._connection_tasks.clear()

        # Close all tunnel connections and SSH processes
        for peer in self._tunnels.values():
            await self._close_tunnel(peer)

        self._tunnels.clear()
        log.info("SSH tunnel transport stopped")
        return True

    async def _close_tunnel(self, peer: TunnelPeer):
        """Close a single tunnel connection + SSH process."""
        peer.connected = False

        # Close writer
        if peer.writer:
            try:
                peer.writer.close()
                await peer.writer.wait_closed()
            except Exception:
                pass
            peer.writer = None
            peer.reader = None

        # Kill SSH process
        if peer.process:
            try:
                peer.process.terminate()
                await asyncio.wait_for(peer.process.wait(), timeout=5)
            except Exception:
                try:
                    peer.process.kill()
                except Exception:
                    pass
            peer.process = None

    async def _maintain_tunnel(self, peer_name: str):
        """Maintain SSH tunnel connection with exponential backoff."""
        peer = self._tunnels.get(peer_name)
        if not peer:
            return

        while self._started:
            if peer.connected:
                # Check if connection is still alive
                if peer.writer and peer.writer.is_closing():
                    log.warning(f"SSH tunnel to {peer_name} writer closing, reconnecting")
                    peer.connected = False
                    await self._close_tunnel(peer)
                else:
                    await asyncio.sleep(5)
                    continue

            # Check max retries
            if peer.retry_count >= self._config.max_retries:
                log.error(f"SSH tunnel to {peer_name} exhausted {self._config.max_retries} retries, backing off 60s")
                await asyncio.sleep(60)
                peer.retry_count = 0  # Reset after cooldown
                continue

            # Exponential backoff
            now = time.time()
            elapsed = now - peer.last_connect_attempt
            if elapsed < peer.backoff:
                await asyncio.sleep(peer.backoff - elapsed)
                continue

            peer.last_connect_attempt = now
            peer.retry_count += 1

            try:
                success = await self._establish_tunnel(peer)
                if success:
                    peer.connected = True
                    peer.connected_since = time.time()
                    peer.retry_count = 0
                    peer.backoff = self._config.reconnect_interval
                    log.info(f"SSH tunnel to {peer_name} established on local port {peer.local_port}")

                    # Notify peer_discovery
                    if self._peer_connected_callback:
                        try:
                            asyncio.create_task(self._peer_connected_callback(peer_name))
                        except Exception as e:
                            log.debug(f"Peer connected callback error for {peer_name}: {e}")

                    # Start reading from tunnel
                    asyncio.create_task(self._read_loop(peer_name))
                else:
                    # Increase backoff
                    peer.backoff = min(peer.backoff * 2, 300)  # Max 5 min
                    log.warning(f"SSH tunnel to {peer_name} failed (attempt {peer.retry_count}), backoff {peer.backoff:.0f}s")
            except Exception as e:
                log.error(f"SSH tunnel to {peer_name} error: {e}")
                peer.backoff = min(peer.backoff * 2, 300)
                await self._close_tunnel(peer)

            await asyncio.sleep(1)

    async def _maintain_tunnel_wrapper(self, peer_name: str):
        """Wrapper ensuring CancelledError also kills the SSH subprocess.

        Without this, task.cancel() at stop() leaves the child ssh process
        running (orphan leak — the transport's stop() clears _tunnels but a
        cancellation mid-loop skips _close_tunnel).
        """
        try:
            await self._maintain_tunnel(peer_name)
        except asyncio.CancelledError:
            peer = self._tunnels.get(peer_name)
            if peer:
                try:
                    await self._close_tunnel(peer)
                except Exception:
                    pass
            raise

    async def _establish_tunnel(self, peer: TunnelPeer) -> bool:
        """Establish SSH tunnel + TCP connection to peer.

        1. Find a free local port
        2. Start SSH process with -L forwarding
        3. Wait for tunnel to be ready
        4. Open asyncio TCP connection through the tunnel
        """
        # Find free local port
        peer.local_port = self._find_free_port()
        if peer.local_port == 0:
            log.error(f"Could not find free local port for {peer.name}")
            return False

        # Kill any previous SSH process for this peer (orphan prevention).
        # If a previous attempt left a live process, it would leak: peer.process
        # is overwritten below and the old process keeps running forever
        # (392 orphans accumulated on tor before this fix, exhausting the
        # Mac's sshd MaxStartups and blocking reverse tunnels).
        if peer.process is not None and peer.process.returncode is None:
            log.warning(f"SSH tunnel to {peer.name}: killing leftover SSH process (pid {peer.process.pid}) before reconnect")
            await self._close_tunnel(peer)

        # Build SSH command
        ssh_cmd = self._build_ssh_command(peer)
        log.info(f"Starting SSH tunnel to {peer.name}: {ssh_cmd}")

        # Start SSH process
        try:
            peer.process = await asyncio.create_subprocess_exec(
                *ssh_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            log.error(f"Failed to start SSH process for {peer.name}: {e}")
            peer.process = None
            return False

        # Wait for tunnel to be ready (SSH connects to remote)
        # POLL instead of fixed sleep: slow containers (e.g. HAOS addon) take 5-15s
        # for the SSH handshake + local forward bind. A single attempt after 2s fails
        # with Errno 111 → retry storm → backoff, while the ssh process itself is fine.
        # Try connecting repeatedly until connect_timeout elapses or process dies.
        await asyncio.sleep(1)
        if peer.process is None:
            log.error(f"SSH process for {peer.name} is None (already closed)")
            return False
        if peer.process.returncode is not None:
            stderr = ""
            try:
                if peer.process.stderr:
                    stderr_bytes = await asyncio.wait_for(peer.process.stderr.read(), timeout=3)
                    stderr = stderr_bytes.decode('utf-8', errors='replace')[:200]
            except Exception:
                pass
            log.error(f"SSH process for {peer.name} exited early (code {peer.process.returncode}): {stderr}")
            peer.process = None
            return False

        # Connect to local tunnel endpoint (with TLS if P2P uses TLS)
        # RETRY LOOP: the ssh -L forward may bind the local port up to several
        # seconds after process start (handshake latency on slow hosts). Retry
        # connection until the process dies or connect_timeout elapses.
        ssl_ctx = self._ssl_client_context
        # Per-peer timeout override (tor/HAOS links are slow: 15s default is
        # too short — SSH there takes 60-90s under load). 0 = global default.
        _timeout = peer.connect_timeout or self._config.connect_timeout
        _deadline = asyncio.get_event_loop().time() + _timeout
        _last_err = None
        peer.reader = peer.writer = None
        while peer.process is not None and peer.process.returncode is None \
                and asyncio.get_event_loop().time() < _deadline:
            try:
                peer.reader, peer.writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", peer.local_port, ssl=ssl_ctx),
                    timeout=max(1.0, _deadline - asyncio.get_event_loop().time()),
                )
                _last_err = None
                break
            except Exception as e:
                _last_err = e
                await asyncio.sleep(0.5)
        if peer.reader is None or peer.writer is None:
            log.error(f"Could not connect to SSH tunnel local endpoint for {peer.name}: {_last_err}")
            await self._close_tunnel(peer)
            return False

        # Send initial handshake — a heartbeat A2AMessage so P2P listener can process it
        # The P2P transport's _handle_connection expects A2AMessage.from_bytes()
        try:
            import time as _time
            hb_msg = A2AMessage.create(
                sender=self._node_name,
                recipient=peer.name,
                msg_type=MSG_TYPE_HEARTBEAT,
                payload={
                    "version": self._node_version or "unknown",
                    "frame_version": 3,
                    "timestamp": _time.time(),
                },
                priority=5,
            )
            frame_data = hb_msg.to_bytes()
            await self._write_frame_v3(peer.writer, frame_data, compressed=False)
            peer.remote_name = peer.name
            log.info(f"SSH tunnel handshake sent (heartbeat): {self._node_name} → {peer.name}")
            await asyncio.sleep(0.5)  # Brief pause for server to process
        except Exception as e:
            log.error(f"SSH tunnel handshake failed for {peer.name}: {e}")
            await self._close_tunnel(peer)
            return False

        return True

    def _find_free_port(self) -> int:
        """Find a free TCP port in the configured range."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            if port < self._config.local_port_start:
                # Use the OS-assigned port anyway — it's guaranteed free
                pass
            return port

    def _build_ssh_command(self, peer: TunnelPeer) -> List[str]:
        """Build SSH command with port forwarding and keepalive."""
        # Forward target: peer.forward_host (multi-agent HAOS) or loopback
        fwd_host = peer.forward_host or "127.0.0.1"
        cmd = [
            "ssh",
            "-N",  # No command, just forwarding
            "-L", f"127.0.0.1:{peer.local_port}:{fwd_host}:{peer.remote_port}",
            "-p", str(peer.ssh_port),
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", f"ConnectTimeout={peer.connect_timeout or self._config.connect_timeout}",
            "-o", f"ServerAliveInterval={self._config.keepalive_interval}",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "BatchMode=yes",  # Never prompt for password
        ]

        if peer.identity_file:
            cmd.extend(["-i", peer.identity_file])

        # User@host
        if peer.ssh_user:
            cmd.append(f"{peer.ssh_user}@{peer.ssh_host}")
        else:
            cmd.append(peer.ssh_host)

        return cmd

    async def _read_loop(self, peer_name: str):
        """Read frames from tunnel and enqueue messages."""
        peer = self._tunnels.get(peer_name)
        if not peer or not peer.reader:
            return

        last_keepalive = time.time()
        KEEPALIVE_INTERVAL = 30  # Send keepalive every 30s to prevent P2P 90s idle timeout

        while self._started and peer.connected:
            try:
                frame_data = await self._read_frame_v3(peer.reader, timeout=15)
                if frame_data is None:
                    log.warning(f"SSH tunnel to {peer_name} closed by remote")
                    peer.connected = False
                    await self._close_tunnel(peer)
                    break

                # Parse A2AMessage from frame data
                # Use from_bytes() to handle both msgpack and JSON serialization
                # (P2P transport uses to_bytes() which prefers msgpack)
                try:
                    msg = A2AMessage.from_bytes(frame_data)
                    await self._receive_queue.put(msg)
                    log.debug(f"SSH tunnel received message from {peer_name}: {getattr(msg, 'type', '?')}")
                except Exception as e:
                    log.error(f"SSH tunnel parse error from {peer_name}: {e}")

            except asyncio.TimeoutError:
                # No data received in 15s — send keepalive if needed
                now = time.time()
                if now - last_keepalive >= KEEPALIVE_INTERVAL:
                    try:
                        hb_msg = A2AMessage.create(
                            sender=self._node_name,
                            recipient=peer_name,
                            msg_type=MSG_TYPE_HEARTBEAT,
                            payload={"node_name": self._node_name, "version": self._node_version or "unknown", "keepalive": True},
                            priority=10,
                            ttl=60,
                        )
                        await self._write_frame_v3(peer.writer, hb_msg.to_bytes())
                        last_keepalive = now
                        log.debug(f"SSH tunnel keepalive sent to {peer_name}")
                    except Exception as e:
                        log.warning(f"SSH tunnel keepalive failed for {peer_name}: {e}")
                continue
            except Exception as e:
                log.error(f"SSH tunnel read error from {peer_name}: {e}")
                peer.connected = False
                await self._close_tunnel(peer)
                break

    async def send(self, message: A2AMessage) -> SendResult:
        """Send a message via SSH tunnel to the recipient."""
        if not self._started:
            return SendResult(transport="ssh_tunnel", success=False, error="not started")

        recipient = message.recipient
        if recipient == "broadcast":
            # Send to all connected tunnel peers
            sent_any = False
            for peer_name, peer in self._tunnels.items():
                if peer.connected and peer.writer:
                    try:
                        await self._send_message(peer.writer, message)
                        sent_any = True
                    except Exception as e:
                        log.error(f"SSH tunnel broadcast to {peer_name} failed: {e}")
            if sent_any:
                return SendResult(transport="ssh_tunnel", success=True, latency_ms=5.0)
            return SendResult(transport="ssh_tunnel", success=False, error="no tunnel peers connected")

        # Direct message to specific peer
        peer = self._tunnels.get(recipient)
        if not peer:
            return SendResult(transport="ssh_tunnel", success=False, error=f"no tunnel configured for {recipient}")

        if not peer.connected or not peer.writer:
            return SendResult(transport="ssh_tunnel", success=False, error=f"tunnel to {recipient} not connected")

        try:
            start = time.time()
            await self._send_message(peer.writer, message)
            latency = (time.time() - start) * 1000
            return SendResult(transport="ssh_tunnel", success=True, latency_ms=latency)
        except Exception as e:
            log.error(f"SSH tunnel send to {recipient} failed: {e}")
            peer.connected = False
            return SendResult(transport="ssh_tunnel", success=False, error=str(e))

    async def _send_message(self, writer: asyncio.StreamWriter, message: A2AMessage):
        """Serialize and send a message as a v3 frame.

        Uses A2AMessage.to_bytes() for serialization (msgpack if available,
        JSON fallback) — consistent with P2P transport's serialization.
        """
        data = message.to_bytes()
        await self._write_frame_v3(writer, data, compressed=len(data) > 1024)

    async def receive(self) -> list:
        """Drain all received messages from the queue.

        Returns list of (A2AMessage, transport_name) tuples —
        matching the interface expected by _receive_loop in node.py.
        """
        messages = []
        while not self._receive_queue.empty():
            try:
                msg = self._receive_queue.get_nowait()
                messages.append((msg, "ssh_tunnel"))
            except asyncio.QueueEmpty:
                break
        return messages

    async def discover(self) -> list:
        """SSH tunnel doesn't do peer discovery — peers are statically configured."""
        return []

    def is_available(self) -> bool:
        """Check if any tunnel is connected."""
        return any(p.connected for p in self._tunnels.values())

    def get_status(self) -> TransportStatus:
        """Return aggregate tunnel status."""
        connected = sum(1 for p in self._tunnels.values() if p.connected)
        total = len(self._tunnels)
        if connected > 0:
            return TransportStatus(available=True, latency_ms=5.0, error="")
        return TransportStatus(available=False, latency_ms=0, error=f"0/{total} tunnels connected")

    def get_connected_peers(self) -> List[str]:
        """Return list of peer names with active tunnel connections."""
        return [name for name, peer in self._tunnels.items() if peer.connected]

    def get_peer_status(self) -> Dict[str, Dict]:
        """Return detailed status for each tunnel peer."""
        status = {}
        for name, peer in self._tunnels.items():
            connected_s = 0
            if peer.connected and peer.connected_since:
                import time as _t
                connected_s = round(_t.time() - peer.connected_since, 1)
            status[name] = {
                "connected": peer.connected,
                "ssh_host": peer.ssh_host,
                "local_port": peer.local_port,
                "remote_port": peer.remote_port,
                "retry_count": peer.retry_count,
                "backoff": peer.backoff,
                "uptime_seconds": connected_s,
                "remote_name": peer.remote_name,
            }
        return status

    async def add_dynamic_peer(self, name: str, ssh_host: str, ssh_port: int,
                               remote_port: int, ssh_user: str = "",
                               identity_file: Optional[str] = None,
                               forward_host: str = "") -> bool:
        """Add and connect a tunnel peer at runtime (key-sync auto-register).

        Used when a peer's ssh_key_sync offer includes tunnel info — lets
        bidirectional tunnels self-organize without config edits. Safe on
        multi-agent hosts: each node may listen on its own ssh_port.
        Returns True if a new peer was added (and a maintain task started).
        """
        if name in self._tunnels:
            return False  # config-registered peers win
        try:
            peer = TunnelPeer(
                name=name,
                ssh_host=ssh_host,
                ssh_user=ssh_user or self._config.default_ssh_user,
                ssh_port=int(ssh_port) or 22,
                remote_port=int(remote_port) or 8645,
                identity_file=os.path.expanduser(
                    identity_file or self._config.default_identity_file or "~/.ssh/id_ed25519_openclaw"
                ),
                local_port=0,
                forward_host=forward_host or "",
            )
            self._tunnels[name] = peer
            self._connection_tasks[name] = asyncio.create_task(
                self._maintain_tunnel_wrapper(name)
            )
            log.info(f"SSH tunnel: dynamic peer {name} added ({ssh_host}:{ssh_port} → remote :{remote_port})")
            return True
        except Exception as e:
            log.warning(f"SSH tunnel: add_dynamic_peer({name}) failed: {e}")
            return False

    # ── Frame Protocol (v3) ──────────────────────────────────────────

    async def _write_frame_v3(self, writer: asyncio.StreamWriter, data: bytes, compressed: bool = False):
        """Write a v3 frame: [magic][4-byte length][1-byte flags][payload].
        
        IMPORTANT: length includes the flags byte (compatible with P2P read_frame).
        """
        flags = 0
        payload = data

        if compressed and len(data) > 1024:
            payload = zlib.compress(data, level=6)
            flags |= FRAME_V3_COMPRESSED

        # Length includes flags byte + payload (matching P2P transport's read_frame)
        inner = bytes([flags]) + payload
        header = struct.pack(
            f'>BI',  # magic (uint8), length (uint32)
            FRAME_V3_MAGIC,
            len(inner),
        )
        writer.write(header + inner)
        await writer.drain()

    async def _read_frame_v3(self, reader: asyncio.StreamReader, timeout: float = 30) -> Optional[bytes]:
        """Read a versioned frame — supports v0, v1, v2, v3 (compatible with P2P transport)."""
        # Read magic byte
        magic = await asyncio.wait_for(reader.readexactly(1), timeout=timeout)
        magic_val = magic[0]

        if magic_val == FRAME_V3_MAGIC:
            # v3 frame: [magic][4-byte length][1-byte flags][payload]
            length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            length = struct.unpack('>I', length_bytes)[0]
            if length > MAX_FRAME_SIZE:
                raise ValueError(f"Frame too large: {length} bytes")
            inner = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
            if len(inner) < 1:
                raise ValueError("v3 frame missing flags byte")
            flags = inner[0]
            payload = inner[1:]
            if flags & FRAME_V3_COMPRESSED:
                payload = zlib.decompress(payload)
            return payload

        elif magic_val == FRAME_V2_MAGIC:
            # v2 frame: [magic][4-byte length][payload]
            length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            length = struct.unpack('>I', length_bytes)[0]
            if length > MAX_FRAME_SIZE:
                raise ValueError(f"Frame too large: {length} bytes")
            payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
            return payload

        elif magic_val == 0x01:
            # v1 frame: [0x01][4-byte length][payload]
            length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            length = struct.unpack('>I', length_bytes)[0]
            if length > MAX_FRAME_SIZE:
                raise ValueError(f"Frame too large: {length} bytes")
            payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
            return payload

        else:
            # v0 legacy frame: first byte is part of 4-byte length (big-endian)
            remaining = await asyncio.wait_for(reader.readexactly(3), timeout=timeout)
            length = struct.unpack('>I', magic + remaining)[0]
            if length > MAX_FRAME_SIZE:
                raise ValueError(f"Frame too large: {length} bytes")
            payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
            return payload