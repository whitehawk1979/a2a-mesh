"""A2A Mesh Discovery — mDNS/Bonjour service discovery.

Compatible with zeroconf v0.149+ (sync API).
Uses synchronous Zeroconf in a background thread for event-loop compatibility.
"""

import asyncio
import logging
import socket
import time
from typing import Dict, Callable, Optional

try:
    from zeroconf import Zeroconf, ServiceBrowser, ServiceInfo, ServiceStateChange, InterfaceChoice
    HAS_ZEROCONF = True
except ImportError:
    HAS_ZEROCONF = False

log = logging.getLogger("a2a_mesh.discovery.mdns")

A2A_SERVICE_TYPE = "_a2a._tcp.local."


class MeshDiscovery:
    """mDNS service discovery for A2A mesh nodes.

    Advertises this node's presence on the local network
    and discovers other nodes via Bonjour/mDNS.

    Optimizations:
    - Caches resolved peers to avoid redundant re-resolution
    - Limits concurrent resolution tasks to prevent event loop starvation
    - Faster initial discovery by resolving immediately on service add
    - Periodic re-resolution of stale cached entries

    Uses synchronous zeroconf API in a background thread
    to avoid blocking the asyncio event loop.
    """

    def __init__(self, node_name: str, port: int = 8645,
                 service_type: str = A2A_SERVICE_TYPE):
        self.node_name = node_name
        self.port = port
        self.service_type = service_type
        self._running = False
        self._discovered_nodes: Dict[str, dict] = {}
        self._zeroconf: Optional[Zeroconf] = None
        self._browser: Optional[ServiceBrowser] = None
        self._service_info: Optional[ServiceInfo] = None
        self._callbacks: list = []
        self._on_discover: Optional[Callable] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # Optimization: cache resolved service info to avoid redundant lookups
        self._resolved_cache: Dict[str, dict] = {}  # name → {host, port, last_resolved}
        self._resolve_semaphore = asyncio.Semaphore(5)  # Limit concurrent resolutions
        self._stale_threshold = 120  # Seconds before a cached entry is considered stale

    async def start(self, host_ip: str = "") -> bool:
        """Start mDNS advertising and browsing (runs in background thread)."""
        if not HAS_ZEROCONF:
            log.warning("zeroconf not installed, mDNS discovery disabled")
            return False

        try:
            self._loop = asyncio.get_running_loop()
            local_ip = host_ip or self._get_local_ip()

            # Create Zeroconf instance with InterfaceChoice.Default
            self._zeroconf = Zeroconf(interfaces=InterfaceChoice.Default)

            # Build TXT records with node metadata
            properties = {
                "node": self.node_name,
                "transport": "p2p",
                "port": str(self.port),
            }

            # Build addresses list
            addresses = []
            if local_ip:
                try:
                    addresses = [socket.inet_pton(socket.AF_INET, local_ip)]
                except (socket.error, OSError):
                    pass  # Will use auto-detection

            # Register our service
            self._service_info = ServiceInfo(
                self.service_type,
                f"{self.node_name}.{self.service_type}",
                addresses=addresses if addresses else None,
                port=self.port,
                properties=properties,
            )
            self._zeroconf.register_service(self._service_info)
            log.info(f"mDNS: Registered {self.node_name} at {local_ip}:{self.port} as {self.service_type}")

            # Start browsing for other services
            self._browser = ServiceBrowser(
                self._zeroconf,
                self.service_type,
                handlers=[self._on_service_state_change],
            )
            log.info(f"mDNS: Browsing for {self.service_type} services on local network")

            self._running = True
            return True

        except Exception as e:
            log.error(f"mDNS start failed: {e}")
            # Cleanup on failure
            if self._browser:
                self._browser.cancel()
                self._browser = None
            if self._zeroconf:
                try:
                    self._zeroconf.unregister_all_services()
                    self._zeroconf.close()
                except Exception:
                    pass
                self._zeroconf = None
            return False

    async def stop(self) -> bool:
        """Stop mDNS advertising and browsing."""
        if self._browser:
            try:
                self._browser.cancel()
            except Exception:
                pass
            self._browser = None

        if self._zeroconf:
            try:
                if self._service_info:
                    self._zeroconf.unregister_service(self._service_info)
                else:
                    self._zeroconf.unregister_all_services()
            except Exception:
                pass
            try:
                self._zeroconf.close()
            except Exception:
                pass
            self._zeroconf = None

        self._running = False
        log.info("mDNS: Discovery stopped")
        return True

    def _on_service_state_change(self, zeroconf, service_type, name, state_change):
        """Handle discovered services (called from zeroconf thread).
        
        Optimization: on service Added, resolve immediately.
        On service Removed, clean up cache.
        """
        if state_change == ServiceStateChange.Added:
            # Schedule async resolution in the event loop
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self._resolve_service(name), self._loop
                )

        elif state_change == ServiceStateChange.Removed:
            node_name = name.split(".")[0]
            if node_name != self.node_name:
                self._discovered_nodes.pop(node_name, None)
                self._resolved_cache.pop(node_name, None)
                log.info(f"mDNS: Node {node_name} removed from network")

    async def _resolve_service(self, name: str):
        """Resolve a discovered service to get host/port.
        
        Optimization: uses cache to avoid redundant DNS resolution.
        Limits concurrent resolutions with semaphore.
        """
        async with self._resolve_semaphore:
            try:
                # Check cache first — skip resolution if recently resolved
                node_name = name.split(".")[0]
                cached = self._resolved_cache.get(node_name)
                if cached and (time.time() - cached.get("last_resolved", 0)) < self._stale_threshold:
                    log.debug(f"mDNS: Using cached resolution for {node_name}")
                    # Still fire callback to trigger P2P connection if not connected
                    node_info = cached.copy()
                    node_info.pop("last_resolved", None)
                    if self._on_discover and node_name != self.node_name:
                        await self._on_discover(node_info)
                    return

                # Use synchronous get_service_info in a thread
                info = await asyncio.get_running_loop().run_in_executor(
                    None, self._zeroconf.get_service_info, self.service_type, name
                )
                if info is None:
                    return

                # Extract node metadata
                if node_name == self.node_name:
                    return  # Skip self

                addresses = info.addresses
                if not addresses:
                    return

                peer_host = socket.inet_ntoa(addresses[0])
                peer_port = info.port

                # Parse TXT properties
                properties = info.properties or {}
                peer_role = properties.get(b"role", b"router").decode("utf-8", errors="ignore") if b"role" in properties else "router"

                node_info = {
                    "name": node_name,
                    "host": peer_host,
                    "port": peer_port,
                    "transport": properties.get(b"transport", b"p2p").decode("utf-8", errors="ignore"),
                }

                self._discovered_nodes[node_name] = node_info
                # Update cache
                self._resolved_cache[node_name] = {
                    **node_info,
                    "last_resolved": time.time(),
                }
                log.info(f"mDNS: Discovered {node_name} at {peer_host}:{peer_port}")

                # Fire callback
                if self._on_discover:
                    await self._on_discover(node_info)

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

    @staticmethod
    def _get_local_ip() -> str:
        """Get the local IP address for mDNS registration."""
        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return ""