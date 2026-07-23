"""A2A Mesh Discovery — UDP broadcast for local network and Tailscale.

Sends periodic UDP broadcast messages announcing this node's presence
and listens for broadcasts from other nodes. Works across subnets
that allow UDP broadcast (including Tailscale's 100.x.x.x range).

Complements mDNS discovery — mDNS is better for local LAN, while
UDP broadcast works reliably on Tailscale and other overlay networks.
"""

import asyncio
import json
import logging
import socket
import struct
import time
from typing import Callable, Dict, List, Optional

log = logging.getLogger("a2a_mesh.discovery.udp")

# Standard A2A mesh discovery port (separate from P2P data port)
DEFAULT_DISCOVERY_PORT = 8646
BROADCAST_INTERVAL = 15  # seconds between announcements
ANNOUNCE_TTL = 45  # seconds before a discovered node is considered stale


class UDPBroadcastDiscovery:
    """UDP broadcast discovery for A2A mesh nodes.

    Announces this node's presence via UDP broadcast every 15 seconds
    and listens for announcements from other nodes. Discovered nodes
    are reported via callback, enabling PG-independent mesh formation.

    Works on:
    - Local network (255.255.255.255 broadcast)
    - Tailscale (100.x.x.x subnet broadcast)
    - Any subnet that allows UDP broadcast
    """

    def __init__(self, node_name: str, p2p_port: int = 8645,
                 health_port: int = 8650,
                 discovery_port: int = DEFAULT_DISCOVERY_PORT,
                 broadcast_addr: str = "255.255.255.255",
                 interfaces: Optional[List[str]] = None,
                 version: str = ""):
        self.node_name = node_name
        self.p2p_port = p2p_port
        self.health_port = health_port
        self.discovery_port = discovery_port
        self.broadcast_addr = broadcast_addr
        self.interfaces = interfaces  # Specific interfaces to broadcast on
        self.version = version

        self._running = False
        self._sock: Optional[socket.socket] = None
        self._discovered_nodes: Dict[str, dict] = {}
        self._on_discover: Optional[Callable] = None
        self._announce_task: Optional[asyncio.Task] = None
        self._listen_task: Optional[asyncio.Task] = None

    def on_discover(self, callback: Callable):
        """Register callback for discovered nodes."""
        self._on_discover = callback

    async def start(self) -> bool:
        """Start UDP broadcast announce and listen."""
        try:
            # Create UDP socket for receiving
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            # Allow broadcast on all interfaces
            try:
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except (AttributeError, OSError):
                pass  # SO_REUSEPORT not available on all platforms

            self._sock.bind(("", self.discovery_port))
            self._sock.setblocking(False)

            self._running = True

            # Start announce and listen tasks
            self._announce_task = asyncio.create_task(self._announce_loop())
            self._listen_task = asyncio.create_task(self._listen_loop())

            log.info(f"UDP broadcast discovery started on port {self.discovery_port}")
            return True

        except Exception as e:
            log.error(f"UDP broadcast discovery start failed: {e}")
            return False

    async def stop(self):
        """Stop UDP broadcast discovery."""
        self._running = False
        if self._announce_task:
            self._announce_task.cancel()
            try:
                await self._announce_task
            except asyncio.CancelledError:
                pass
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        log.info("UDP broadcast discovery stopped")

    async def _announce_loop(self):
        """Periodically broadcast this node's presence."""
        while self._running:
            try:
                await self._send_announcement()
                # Also send on specific interfaces if configured (e.g., Tailscale)
                if self.interfaces:
                    for iface_ip in self.interfaces:
                        await self._send_announcement(iface_ip)
            except Exception as e:
                log.debug(f"UDP announce error: {e}")
            await asyncio.sleep(BROADCAST_INTERVAL)

    async def _send_announcement(self, broadcast_addr: Optional[str] = None):
        """Send a single UDP broadcast announcement."""
        if not self._sock:
            return

        announcement = {
            "type": "a2a_mesh_announce",
            "node": self.node_name,
            "p2p_port": self.p2p_port,
            "health_port": self.health_port,
            "ts": time.time(),
            "version": self.version or "unknown",
        }

        data = json.dumps(announcement).encode("utf-8")
        addr = broadcast_addr or self.broadcast_addr

        try:
            self._sock.sendto(data, (addr, self.discovery_port))
            log.debug(f"UDP announce sent to {addr}:{self.discovery_port}")
        except Exception as e:
            log.debug(f"UDP announce send failed to {addr}: {e}")

    async def _listen_loop(self):
        """Listen for UDP broadcast announcements from other nodes."""
        loop = asyncio.get_event_loop()

        while self._running:
            try:
                data, addr = await asyncio.wait_for(
                    loop.run_in_executor(None, self._recv_from),
                    timeout=2.0
                )
                await self._handle_announcement(data, addr)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                if self._running:
                    log.debug(f"UDP listen error: {e}")
                await asyncio.sleep(0.5)

    def _recv_from(self):
        """Blocking recvfrom call (runs in executor)."""
        if not self._sock:
            raise Exception("Socket closed")
        return self._sock.recvfrom(4096)

    async def _handle_announcement(self, data: bytes, addr: tuple):
        """Handle a received UDP announcement."""
        try:
            announcement = json.loads(data.decode("utf-8"))

            if announcement.get("type") != "a2a_mesh_announce":
                return  # Not our protocol

            name = announcement.get("node", "")
            p2p_port = announcement.get("p2p_port", 8645)
            health_port = announcement.get("health_port", 8650)
            ts = announcement.get("ts", 0)

            if not name or name == self.node_name:
                return  # Skip self

            # Stale check — ignore very old announcements
            if time.time() - ts > 120:
                log.debug(f"UDP: Ignoring stale announcement from {name} (age={time.time()-ts:.0f}s)")
                return

            host = addr[0]  # IP from the sender

            # Update or add discovered node
            peer_version = announcement.get("version", "unknown")
            is_new = name not in self._discovered_nodes
            self._discovered_nodes[name] = {
                "name": name,
                "host": host,
                "port": p2p_port,
                "health_port": health_port,
                "transport": "p2p",
                "version": peer_version,
                "ts": ts,
                "last_seen": time.time(),
            }

            if is_new:
                log.info(f"UDP discovered peer: {name} at {host}:{p2p_port}")
                if self._on_discover:
                    await self._on_discover(self._discovered_nodes[name])
            else:
                log.debug(f"UDP: Updated peer {name} at {host}:{p2p_port}")

        except (json.JSONDecodeError, KeyError) as e:
            log.debug(f"UDP: Invalid announcement from {addr}: {e}")

    def get_discovered_nodes(self) -> dict:
        """Return currently discovered nodes."""
        return dict(self._discovered_nodes)

    @property
    def is_running(self) -> bool:
        return self._running