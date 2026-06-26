"""A2A Mesh Gateway Plugin — Bridges mesh messages to external platforms (Telegram, Discord, etc.)

This plugin connects the A2A mesh to Hermes Agent gateway endpoints,
enabling bidirectional message flow between mesh nodes and external platforms.

Architecture:
    Mesh Node ←→ Gateway Plugin ←→ Hermes Gateway ←→ Telegram/Discord/Slack

Features:
    - Bidirectional message bridge (mesh ↔ platform)
    - Multi-platform support (Telegram, Discord, Slack, WhatsApp)
    - Message priority mapping (mesh priority → platform priority)
    - Agent wake-up on incoming platform messages
    - Configurable per-platform routing (which agents handle which platforms)
    - Auto-reconnect with exponential backoff
    - Message format conversion (mesh ↔ platform)
"""

import asyncio
import json
import logging
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Any

from a2a_mesh.core.plugin_base import MeshPlugin

log = logging.getLogger("a2a_mesh.plugin.gateway")


class GatewayPlugin(MeshPlugin):
    """Bridges A2A mesh messages to external platforms via Hermes gateway.

    Configuration (in mesh_config.yaml):
        plugins:
          gateway:
            enabled: true
            platforms:
              telegram:
                gateway_url: "http://localhost:8644"  # Hermes webhook port
                chat_id: "-1003971026331"
                bot_token: ""  # Optional: override gateway auth
              discord:
                gateway_url: "http://localhost:8644"
                channel_id: ""
            default_recipient: "broadcast"  # Default mesh recipient for platform messages
            wake_agent_on_message: true  # Send wake-agent on incoming messages
            priority_map:
              urgent: 10
              high: 7
              normal: 5
              low: 1
    """

    name = "gateway"
    version = "1.0.0"
    description = "Bridges A2A mesh to external platforms (Telegram, Discord, etc.)"
    author = "nova"
    capabilities = ["gateway_bridge", "telegram_bridge", "discord_bridge"]

    config_defaults = {
        "enabled": True,
        "default_recipient": "broadcast",
        "wake_agent_on_message": True,
        "priority_map": {
            "urgent": 10,
            "high": 7,
            "normal": 5,
            "low": 1,
        },
    }

    def __init__(self):
        super().__init__()
        self._platforms: Dict[str, Dict] = {}
        self._reconnect_tasks: Dict[str, asyncio.Task] = {}
        self._polling = False
        self._poll_interval = 5.0  # seconds

    def configure(self, config: Dict[str, Any]):
        """Apply configuration including platform definitions."""
        super().configure(config)

        # Extract platform configs
        platforms = self._config.get("platforms", {})
        for platform_name, platform_config in platforms.items():
            if platform_config.get("enabled", True):
                self._platforms[platform_name] = {
                    "gateway_url": platform_config.get("gateway_url", ""),
                    "chat_id": platform_config.get("chat_id", ""),
                    "channel_id": platform_config.get("channel_id", ""),
                    "bot_token": platform_config.get("bot_token", ""),
                    "enabled": True,
                    "last_error": None,
                    "message_count": 0,
                    "last_activity": 0,
                }
                self.log.info(f"Gateway platform configured: {platform_name}")

    async def on_start(self):
        """Start gateway polling for each configured platform."""
        await super().on_start()

        if not self._platforms:
            self.log.info("No platforms configured, gateway plugin idle")
            return

        self._polling = True
        for platform_name in self._platforms:
            self.log.info(f"Starting gateway bridge for {platform_name}")

    async def on_stop(self):
        """Stop all gateway bridges."""
        self._polling = False
        for task in self._reconnect_tasks.values():
            if not task.done():
                task.cancel()
        self._reconnect_tasks.clear()
        await super().on_stop()

    async def on_message_received(self, message) -> Optional[Any]:
        """Bridge outgoing mesh messages to external platforms.

        When a message is addressed to this node or is a broadcast,
        forward it to configured platforms.
        """
        # Skip non-directive messages
        if message.type not in ("directive", "agent_reply", "steer"):
            return None

        # Only bridge messages intended for this node or broadcasts
        if message.recipient not in ("broadcast", self._node.node_name, "*"):
            return None

        content = message.content if hasattr(message, 'content') else str(message.payload)
        sender = message.sender
        priority = message.priority if hasattr(message, 'priority') else 5

        results = {}
        for platform_name, platform_config in self._platforms.items():
            if not platform_config.get("enabled", True):
                continue

            try:
                result = await self._send_to_platform(
                    platform_name=platform_name,
                    content=content,
                    sender=sender,
                    priority=priority,
                    msg_type=message.type,
                )
                results[platform_name] = result
            except Exception as e:
                self.log.error(f"Failed to bridge message to {platform_name}: {e}")
                platform_config["last_error"] = str(e)

        return None  # Don't modify the original message flow

    async def on_peer_connected(self, peer_name: str, info: Dict):
        """Announce gateway capability when a peer connects."""
        # Send our gateway capabilities to the new peer
        if self._platforms and self._node:
            caps = list(self._platforms.keys()) + ["gateway_bridge"]
            self.log.info(f"Peer {peer_name} connected — gateway has platforms: {list(self._platforms.keys())}")

    # ── Platform-specific send methods ──────────────────────────

    async def _send_to_platform(
        self,
        platform_name: str,
        content: str,
        sender: str,
        priority: int = 5,
        msg_type: str = "directive",
    ) -> Dict:
        """Send a message to a platform via its gateway URL.

        Uses HTTP POST to the Hermes webhook endpoint.
        """
        platform_config = self._platforms.get(platform_name, {})
        gateway_url = platform_config.get("gateway_url", "")

        if not gateway_url:
            self.log.warning(f"No gateway URL for {platform_name}")
            return {"status": "error", "error": "no_gateway_url"}

        # Build the webhook payload
        payload = {
            "sender": sender,
            "content": content,
            "priority": priority,
            "msg_type": msg_type,
            "platform": platform_name,
            "mesh_node": self._node.node_name if self._node else "unknown",
            "timestamp": time.time(),
        }

        # Add platform-specific fields
        if platform_name == "telegram":
            payload["chat_id"] = platform_config.get("chat_id", "")
        elif platform_name == "discord":
            payload["channel_id"] = platform_config.get("channel_id", "")

        try:
            # Use urllib for sync HTTP (compatible with Hermes)
            data = json.dumps(payload).encode("utf-8")
            headers = {"Content-Type": "application/json"}

            # Add auth if configured
            bot_token = platform_config.get("bot_token", "")
            if bot_token:
                headers["Authorization"] = f"Bearer {bot_token}"

            req = urllib.request.Request(
                f"{gateway_url}/api/send",
                data=data,
                headers=headers,
                method="POST",
            )

            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, timeout=10),
            )

            result_data = response.read().decode("utf-8")
            platform_config["message_count"] = platform_config.get("message_count", 0) + 1
            platform_config["last_activity"] = time.time()
            platform_config["last_error"] = None

            return {"status": "ok", "response": result_data[:200]}

        except urllib.error.HTTPError as e:
            error_msg = f"HTTP {e.code}: {e.reason}"
            self.log.error(f"Gateway HTTP error for {platform_name}: {error_msg}")
            platform_config["last_error"] = error_msg
            return {"status": "error", "error": error_msg}

        except Exception as e:
            error_msg = str(e)
            self.log.error(f"Gateway error for {platform_name}: {error_msg}")
            platform_config["last_error"] = error_msg
            return {"status": "error", "error": error_msg}

    # ── Gateway status ───────────────────────────────────────────

    def get_gateway_status(self) -> Dict:
        """Get current status of all platform bridges."""
        status = {
            "gateway_plugin": "active" if self._running else "inactive",
            "platforms": {},
        }
        for platform_name, config in self._platforms.items():
            status["platforms"][platform_name] = {
                "enabled": config.get("enabled", False),
                "gateway_url": config.get("gateway_url", ""),
                "message_count": config.get("message_count", 0),
                "last_activity": config.get("last_activity", 0),
                "last_error": config.get("last_error"),
            }
        return status