"""A2A Mesh Plugin Base — Abstract base class for all mesh plugins.

Plugins extend the mesh node with additional functionality:
- Gateway bridges (Telegram, Discord, Slack, WhatsApp, etc.)
- Custom message handlers (workflow triggers, notifications, etc.)
- Transport adapters (new communication channels)
- Discovery methods (mDNS extensions, custom registries)

Architecture:
    Plugin lifecycle:
    1. Discovery: plugins/ directory scanned for *_plugin.py files
    2. Loading: PluginLoader imports and instantiates each plugin
    3. Registration: plugin.register(node) — hooks into mesh events
    4. Running: plugin.start() — begins plugin operation
    5. Shutdown: plugin.stop() — clean shutdown

    Hook system:
    - on_start(node): called after node starts
    - on_stop(node): called before node stops
    - on_message_received(message): called for every incoming message
    - on_message_sent(message, result): called after message is sent
    - on_peer_connected(peer_name, info): called when a peer connects
    - on_peer_disconnected(peer_name): called when a peer disconnects
    - on_health_change(peer_name, old_health, new_health): health status changes

Usage:
    class MyPlugin(MeshPlugin):
        name = "my_plugin"
        version = "1.0.0"

        def on_start(self, node):
            self.log.info(f"My plugin starting on {node.node_name}")

        async def on_message_received(self, message):
            if message.type == "directive":
                self.log.info(f"Directive from {message.sender}: {message.content}")
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any, Callable
from dataclasses import dataclass, field


@dataclass
class PluginInfo:
    """Plugin metadata."""
    name: str = ""
    version: str = "0.0.0"
    description: str = ""
    author: str = ""
    capabilities: List[str] = field(default_factory=list)
    config_defaults: Dict[str, Any] = field(default_factory=dict)


class MeshPlugin(ABC):
    """Base class for A2A Mesh plugins.

    Subclass this to create a plugin. Override hooks as needed.
    The plugin loader will discover and instantiate subclasses.
    """

    # Override these in your subclass
    name: str = "base_plugin"
    version: str = "0.0.0"
    description: str = ""
    author: str = ""
    capabilities: List[str] = []
    config_defaults: Dict[str, Any] = {}

    def __init__(self):
        self.log = logging.getLogger(f"a2a_mesh.plugin.{self.name}")
        self._node = None
        self._running = False
        self._config: Dict[str, Any] = {}
        self._tasks: List[asyncio.Task] = []

    @property
    def info(self) -> PluginInfo:
        """Get plugin metadata."""
        return PluginInfo(
            name=self.name,
            version=self.version,
            description=self.description,
            author=self.author,
            capabilities=self.capabilities,
            config_defaults=self.config_defaults,
        )

    def register(self, node):
        """Register this plugin with a mesh node.

        Called during plugin loading. Store the node reference for later use.
        """
        self._node = node
        self.log.info(f"Plugin '{self.name}' v{self.version} registered with node '{node.node_name}'")

    def configure(self, config: Dict[str, Any]):
        """Apply configuration to this plugin.

        Called after register() but before start().
        Config values override defaults.
        """
        self._config = {**self.config_defaults, **config}
        self.log.info(f"Plugin '{self.name}' configured: {list(self._config.keys())}")

    # ── Lifecycle hooks ────────────────────────────────────────

    async def on_start(self):
        """Called when the node starts. Initialize resources here."""
        self._running = True
        self.log.info(f"Plugin '{self.name}' started")

    async def on_stop(self):
        """Called when the node stops. Clean up resources here."""
        self._running = False
        # Cancel any background tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        self.log.info(f"Plugin '{self.name}' stopped")

    # ── Message hooks ──────────────────────────────────────────

    async def on_message_received(self, message) -> Optional[Any]:
        """Called for every incoming message.

        Return a response A2AMessage to send it back, or None.
        """
        pass

    async def on_message_sent(self, message, result) -> None:
        """Called after a message is sent (with the send result)."""
        pass

    # ── Peer hooks ─────────────────────────────────────────────

    async def on_peer_connected(self, peer_name: str, info: Dict) -> None:
        """Called when a new peer connects to the mesh."""
        self.log.info(f"Peer connected: {peer_name}")

    async def on_peer_disconnected(self, peer_name: str) -> None:
        """Called when a peer disconnects from the mesh."""
        self.log.info(f"Peer disconnected: {peer_name}")

    # ── Health hooks ────────────────────────────────────────────

    async def on_health_change(self, peer_name: str, old_health: float, new_health: float) -> None:
        """Called when a peer's health score changes."""
        pass

    # ── Utility methods ────────────────────────────────────────

    def create_task(self, coro):
        """Create a managed async task that will be cancelled on stop."""
        task = asyncio.create_task(coro)
        self._tasks.append(task)
        task.add_done_callback(lambda t: self._tasks.remove(t) if t in self._tasks else None)
        return task

    async def send_message(self, recipient: str, content: str, msg_type: str = "directive",
                           priority: int = 5, payload: Dict = None):
        """Send a message through the mesh node."""
        if not self._node:
            self.log.error("Cannot send message: plugin not registered with a node")
            return None

        from .message import A2AMessage
        msg = A2AMessage.create(
            sender=self._node.node_name,
            recipient=recipient,
            msg_type=msg_type,
            content=content,
            priority=priority,
            payload=payload or {},
        )
        result = await self._node.router.send(msg)
        return result

    async def broadcast_message(self, content: str, msg_type: str = "directive",
                                 priority: int = 5, payload: Dict = None):
        """Broadcast a message to all peers."""
        return await self.send_message(
            recipient="broadcast",
            content=content,
            msg_type=msg_type,
            priority=priority,
            payload=payload,
        )

    def get_peers(self) -> Dict:
        """Get all known peers from the registry."""
        if not self._node or not hasattr(self._node, 'dashboard'):
            return {}
        if hasattr(self._node.dashboard, 'registry'):
            return self._node.dashboard.registry.list_all()
        return {}

    def get_status(self) -> Dict:
        """Get the node's current status."""
        if not self._node:
            return {}
        return self._node.get_status()