"""A2A Mesh Discovery — mDNS/Bonjour service discovery."""

import asyncio
import logging
from typing import List, Optional, Callable

try:
    from zeroconf import Zeroconf, ServiceBrowser, ServiceInfo, ServiceStateChange
    from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser, AsyncServiceInfo
    HAS_ZEROCONF = True
except ImportError:
    HAS_ZEROCONF = False

log = logging.getLogger("a2a_mesh.discovery.mdns")

A2A_SERVICE_TYPE = "_a2a._tcp.local."


class MeshDiscovery:
    """mDNS service discovery for A2A mesh nodes.

    Advertises this node's presence on the local network
    and discovers other nodes via Bonjour/mDNS.
    """

    def __init__(self, node_name: str, port: int = 8645,
                 service_type: str = A2A_SERVICE_TYPE):
        self.node_name = node_name
        self.port = port
        self.service_type = service_type
        self._zeroconf: Optional[object] = None
        self._browser: Optional[object] = None
        self._discovered_nodes = {}  # name → {host, port, ...}
        self._running = False
        self._on_discover: Optional[Callable] = None

    async def start(self, host_ip: str = "") -> bool:
        """Start mDNS advertising and browsing."""
        if not HAS_ZEROCONF:
            log.warning("zeroconf not installed, mDNS discovery disabled")
            return False

        try:
            self._zeroconf = Zeroconf()
            aiozc = AsyncZeroconf(self._zeroconf)
            self._aiozc = aiozc

            # Register our service
            properties = {
                "node": self.node_name.encode('utf-8'),
                "transport": "p2p".encode('utf-8'),
            }

            info = ServiceInfo(
                self.service_type,
                f"{self.node_name}.{self.service_type}",
                addresses=[host_ip.encode() if host_ip else b""],
                port=self.port,
                properties=properties,
            )
            await aiozc.async_register_service(info)
            log.info(f"mDNS: Registered {self.node_name} on port {self.port}")

            # Start browsing for other services
            self._browser = AsyncServiceBrowser(
                aiozc,
                self.service_type,
                handlers=[self._on_service_state_change],
            )

            self._running = True
            return True

        except Exception as e:
            log.error(f"mDNS start failed: {e}")
            return False

    async def stop(self) -> bool:
        """Stop mDNS advertising and browsing."""
        if self._aiozc:
            try:
                await self._aiozc.async_close()
            except Exception:
                pass
        elif self._zeroconf:
            try:
                await self._zeroconf.async_close()
            except Exception:
                pass
        self._running = False
        return True

    def _on_service_state_change(self, zeroconf, service_type, name, state_change):
        """Handle discovered services."""
        if state_change == ServiceStateChange.Added:
            asyncio.ensure_future(self._resolve_service(name))
        elif state_change == ServiceStateChange.Removed:
            # Remove from discovered nodes
            node_name = name.split(".")[0]
            self._discovered_nodes.pop(node_name, None)
            log.info(f"mDNS: Node {node_name} removed")

    async def _resolve_service(self, name: str):
        """Resolve a discovered service to get host/port."""
        try:
            info = AsyncServiceInfo(self.service_type, name)
            info = await info.async_request(self._zeroconf, 3000)

            if info:
                addresses = info.addresses
                port = info.port
                properties = info.properties

                node_name = properties.get(b"node", b"unknown").decode()
                host = socket.inet_ntoa(addresses[0]) if addresses else ""

                self._discovered_nodes[node_name] = {
                    "host": host,
                    "port": port,
                    "name": node_name,
                    "transport": properties.get(b"transport", b"p2p").decode(),
                }

                log.info(f"mDNS: Discovered {node_name} at {host}:{port}")

                if self._on_discover:
                    await self._on_discover(self._discovered_nodes[node_name])

        except Exception as e:
            log.debug(f"mDNS: Failed to resolve {name}: {e}")

    def on_discover(self, callback: Callable):
        """Register callback for discovered nodes."""
        self._on_discover = callback

    def get_discovered_nodes(self) -> dict:
        """Return currently discovered nodes."""
        return dict(self._discovered_nodes)

    @property
    def is_running(self) -> bool:
        return self._running