"""A2A Mesh Node — Main mesh node that ties everything together.

MeshNode is the central orchestrator that:
- Manages all transports (PG, P2P, HTTP)
- Routes messages via the MeshRouter
- Handles discovery via mDNS
- Provides CLI interface
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Optional, Dict, List, Callable

from .core.message import A2AMessage, SendResult, ProcessResult, MSG_TYPE_HEARTBEAT
from .core.config import MeshConfig
from .core.router import MeshRouter
from .core.encryption import MeshEncryption
from .core.topology import NodeRole, MeshAddress, AddressManager
from .core.tree_router import TreeRouter
from .transports.pg_transport import PGTransport
from .transports.p2p_transport import P2PTransport
from .transports.http_transport import HTTPTransport
from .discovery.mdns import MeshDiscovery

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
        self.router = MeshRouter(self.node_name, self.config)

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
            self.address_manager = AddressManager(
                max_children=topo.max_children,
                max_routers=topo.max_routers,
                max_depth=topo.max_depth,
            )
            log.info(f"Router mode: will join network and get address")
        else:
            # End device — lightweight, connects via parent
            log.info(f"End device mode: will join network via parent router")

        # Initialize transports
        self._pg_transport = PGTransport(self.config)
        self._p2p_transport = P2PTransport(self.config)
        self._http_transport = HTTPTransport(self.config)

        # Register transports with router
        self.router.register_transport("pg_notify", self._pg_transport)
        self.router.register_transport("p2p", self._p2p_transport)
        self.router.register_transport("http", self._http_transport)

        # Initialize discovery
        self._discovery = MeshDiscovery(
            node_name=self.node_name,
            port=self.config.p2p.listen_port,
        )

        # State
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._start_time = 0

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
        self.router.add_handler(handler)

    async def start(self) -> bool:
        """Start all transports and discovery."""
        log.info(f"Starting mesh node '{self.node_name}'")
        self._start_time = time.time()

        # Start transports in priority order
        results = {}

        # 1. PG NOTIFY (primary)
        results["pg_notify"] = await self._pg_transport.start()
        if results["pg_notify"]:
            log.info("✅ PG NOTIFY transport started")
        else:
            log.warning("❌ PG NOTIFY transport failed")

        # 2. P2P TCP (secondary)
        results["p2p"] = await self._p2p_transport.start()
        if results["p2p"]:
            log.info("✅ P2P TCP transport started")
        else:
            log.warning("❌ P2P TCP transport failed")

        # 3. HTTP/MCP (tertiary)
        results["http"] = await self._http_transport.start()
        if results["http"]:
            log.info("✅ HTTP/MCP transport started")
        else:
            log.warning("❌ HTTP/MCP transport failed")

        # 4. mDNS discovery
        if self.config.discovery.mdns_enabled:
            host_ip = self._get_local_ip()
            disc_ok = await self._discovery.start(host_ip=host_ip)
            if disc_ok:
                log.info("✅ mDNS discovery started")
            else:
                log.warning("❌ mDNS discovery failed")

        # Start receive loop
        self._running = True
        self._tasks.append(asyncio.create_task(self._receive_loop()))
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

        # At least one transport must be working
        any_ok = any(results.values())
        if any_ok:
            log.info(f"Mesh node '{self.node_name}' started ({sum(results.values())}/3 transports)")
        else:
            log.error("All transports failed!")

        return any_ok

    async def stop(self):
        """Stop all transports and discovery."""
        log.info(f"Stopping mesh node '{self.node_name}'")
        self._running = False

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
        await self._discovery.stop()

        log.info("Mesh node stopped")

    async def send(self, message: A2AMessage) -> SendResult:
        """Send a message via the best available transport."""
        message.sender = self.node_name
        message.sender_node_id = self.node_name

        # Sign if encryption is available
        if self.encryption and not message.signature:
            content = message.sign_content()
            message.signature = self.encryption.sign_message(content)

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

    async def _receive_loop(self):
        """Main receive loop — polls all transports for incoming messages."""
        while self._running:
            try:
                # Check each transport for messages
                for transport_name, transport in self.router.transports.items():
                    if not transport.is_available():
                        continue
                    try:
                        messages = await transport.receive()
                        for msg, from_transport in messages:
                            result = await self.router.receive(msg, from_transport)
                            if result.status == "processed":
                                log.debug(f"Processed message {msg.id[:8]} from {msg.sender}")
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
                result = await self.router.send(msg)
                if not result.success:
                    log.warning(f"Heartbeat send failed: {result.error}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Heartbeat error: {e}")

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
        return {
            "node_name": self.node_name,
            "running": self._running,
            "uptime": int(time.time() - self._start_time) if self._start_time else 0,
            "transports": self.router.get_stats(),
            "discovery": self._discovery.get_discovered_nodes() if self._discovery._running else {},
            "encryption": "enabled" if self.encryption else "disabled",
            "dedup_cache_size": self.router.dedup.size,
        }


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

    # Create and start node
    node = MeshNode(config)
    node.add_handler(lambda msg: print(f"📨 {msg.sender} → {msg.recipient}: {msg.type}"))

    try:
        if await node.start():
            print(f"🟢 Mesh node '{args.name}' started")
            print(f"   PG: {'✅' if node._pg_transport.is_available() else '❌'}")
            print(f"   P2P: {'✅' if node._p2p_transport.is_available() else '❌'}")
            print(f"   HTTP: {'✅' if node._http_transport.is_available() else '❌'}")

            # Run forever
            try:
                while True:
                    await asyncio.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                pass
        else:
            print("🔴 Failed to start mesh node")
            sys.exit(1)
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())