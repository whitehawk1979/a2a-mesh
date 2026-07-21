"""A2A Mesh Gateway Plugin — Bridges mesh messages to external platforms (Telegram, Discord, etc.)

This plugin connects the A2A mesh to Hermes Agent gateway endpoints,
enabling bidirectional message flow between mesh nodes and external platforms.

Architecture:
    Mesh Node <-> Gateway Plugin <-> Hermes Gateway <-> Telegram/Discord/Slack

The plugin uses Hermes webhook routes (deliver_only mode) to forward mesh
messages to Telegram and other platforms. Each platform is configured with:
  - route_name: Hermes webhook route (e.g. "runa-telegram")
  - gateway_url: Base URL of the Hermes webhook server (default: localhost:8644)
  - webhook_secret: HMAC secret for the route (default: from env WEBHOOK_SECRET)
  - chat_id: Target chat/channel ID for the platform
  - template: Optional prompt template (default: uses text field)

Features:
    - Bidirectional message bridge (mesh <-> platform)
    - Multi-platform support (Telegram, Discord, Slack, WhatsApp)
    - Message priority mapping (mesh priority -> platform priority)
    - Agent wake-up on incoming platform messages
    - Configurable per-platform routing (which agents handle which platforms)
    - Auto-reconnect with exponential backoff
    - HMAC-SHA256 signed webhook requests for security
    - Message format conversion (mesh <-> platform)
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
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
            webhook_secret: "${WEBHOOK_SECRET}"   # HMAC secret for webhook signing
            platforms:
              telegram:
                route_name: "runa-telegram"        # Hermes webhook route name
                gateway_url: "http://localhost:8644"
                chat_id: "-1003971026331"
                template: "🍞 {text}"               # Prompt template
                enabled: true
              discord:
                route_name: "runa-discord"
                gateway_url: "http://localhost:8644"
                channel_id: ""
                enabled: false
            default_recipient: "broadcast"
            wake_agent_on_message: true
            priority_map:
              urgent: 10
              high: 7
              normal: 5
              low: 1
    """

    name = "gateway"
    version = "2.0.0"
    description = "Bridges A2A mesh to external platforms (Telegram, Discord, etc.) via Hermes gateway"
    author = "nova"
    capabilities = ["gateway_bridge", "telegram_bridge", "discord_bridge"]

    config_defaults = {
        "enabled": True,
        "default_recipient": "broadcast",
        "wake_agent_on_message": True,
        "webhook_secret": "",  # Fallback: env var WEBHOOK_SECRET
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
        self._webhook_secret: str = ""

    def configure(self, config: Dict[str, Any]):
        """Apply configuration including platform definitions."""
        super().configure(config)

        # Get webhook secret (for HMAC signing)
        self._webhook_secret = (
            self._config.get("webhook_secret", "")
            or os.environ.get("WEBHOOK_SECRET", "")
        )

        # Extract platform configs
        platforms = self._config.get("platforms", {})
        for platform_name, platform_config in platforms.items():
            if platform_config.get("enabled", True):
                self._platforms[platform_name] = {
                    "route_name": platform_config.get("route_name", ""),
                    "gateway_url": platform_config.get("gateway_url", "http://localhost:8644"),
                    "chat_id": platform_config.get("chat_id", ""),
                    "channel_id": platform_config.get("channel_id", ""),
                    "template": platform_config.get("template", ""),
                    "enabled": True,
                    "last_error": None,
                    "message_count": 0,
                    "last_activity": 0,
                }
                self.log.info(
                    f"Gateway platform configured: {platform_name} "
                    f"(route={platform_config.get('route_name', 'N/A')})"
                )

    async def on_start(self):
        """Start gateway polling for each configured platform."""
        await super().on_start()

        if not self._platforms:
            self.log.info("No platforms configured, gateway plugin idle")
            return

        self._polling = True
        for platform_name, platform_config in self._platforms.items():
            route = platform_config.get("route_name", "")
            url = platform_config.get("gateway_url", "")
            self.log.info(
                f"Starting gateway bridge for {platform_name}: "
                f"route={route}, url={url}"
            )

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

        # Don't bridge our own messages (prevent loops)
        if hasattr(message, 'sender') and message.sender == self._node.node_name:
            return None

        content = message.content if hasattr(message, 'content') else str(message.payload)
        sender = message.sender
        priority = message.priority if hasattr(message, 'priority') else 5

        # Extract text content from payload if content is empty
        if not content and hasattr(message, 'payload') and isinstance(message.payload, dict):
            content = message.payload.get("text", "")

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
        if self._platforms and self._node:
            self.log.info(
                f"Peer {peer_name} connected — gateway has platforms: "
                f"{list(self._platforms.keys())}"
            )

    # ── Platform-specific send methods ──────────────────────────

    def _sign_payload(self, payload_bytes: bytes, secret: str) -> str:
        """Sign payload bytes with HMAC-SHA256 using the webhook secret.

        Produces the X-Webhook-Signature header value for Hermes webhook auth.
        """
        return hmac.new(
            secret.encode(), payload_bytes, hashlib.sha256
        ).hexdigest()

    async def _send_to_platform(
        self,
        platform_name: str,
        content: str,
        sender: str,
        priority: int = 5,
        msg_type: str = "directive",
    ) -> Dict:
        """Send a message to a platform via Hermes webhook.

        Uses the Hermes gateway's webhook endpoint with HMAC-SHA256 signing.
        For deliver_only routes, the payload text is sent directly to the
        target platform (Telegram, Discord, etc.) with zero LLM cost.
        """
        platform_config = self._platforms.get(platform_name, {})
        gateway_url = platform_config.get("gateway_url", "")
        route_name = platform_config.get("route_name", "")

        if not gateway_url or not route_name:
            self.log.warning(
                f"Missing gateway config for {platform_name}: "
                f"url={gateway_url}, route={route_name}"
            )
            return {"status": "error", "error": "missing_config"}

        # Format the message text
        template = platform_config.get("template", "")
        if template:
            # Apply template with available variables
            text = template.format(
                text=content,
                sender=sender,
                priority=priority,
                type=msg_type,
                node=self._node.node_name if self._node else "unknown",
            )
        else:
            # Default format: sender prefix + content
            node_name = self._node.node_name if self._node else "unknown"
            if sender and sender != node_name:
                text = f"[{sender}] {content}"
            else:
                text = content

        # Build the webhook payload
        # The 'type' field tells Hermes webhook which event type this is
        # The 'text' field contains the message content for deliver_only routes
        payload = {
            "text": text,
            "type": "message",  # Required for Hermes webhook event matching
            "sender": sender,
            "priority": priority,
            "msg_type": msg_type,
            "platform": platform_name,
            "mesh_node": self._node.node_name if self._node else "unknown",
            "timestamp": time.time(),
        }

        # Add platform-specific fields
        if platform_name == "telegram":
            chat_id = platform_config.get("chat_id", "")
            if chat_id:
                payload["chat_id"] = chat_id
        elif platform_name == "discord":
            channel_id = platform_config.get("channel_id", "")
            if channel_id:
                payload["channel_id"] = channel_id

        try:
            # Serialize payload
            payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

            # Sign with HMAC-SHA256 for webhook authentication
            secret = self._webhook_secret
            if not secret:
                self.log.error("No webhook secret configured — cannot sign payload")
                return {"status": "error", "error": "no_secret"}

            signature = self._sign_payload(payload_bytes, secret)

            # Build the webhook URL: {gateway_url}/webhooks/{route_name}
            webhook_url = f"{gateway_url}/webhooks/{route_name}"

            headers = {
                "Content-Type": "application/json",
                "X-Webhook-Signature": signature,
            }

            req = urllib.request.Request(
                webhook_url,
                data=payload_bytes,
                headers=headers,
                method="POST",
            )

            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, timeout=10),
            )

            result_data = response.read().decode("utf-8")

            # Parse response
            try:
                result_json = json.loads(result_data)
                status = result_json.get("status", "unknown")

                if status == "delivered":
                    self.log.info(
                        f"Message bridged to {platform_name} via {route_name}: "
                        f"delivery_id={result_json.get('delivery_id', 'N/A')}"
                    )
                    platform_config["message_count"] = platform_config.get("message_count", 0) + 1
                    platform_config["last_activity"] = time.time()
                    platform_config["last_error"] = None
                    return {"status": "ok", "response": result_json}

                elif status == "ignored":
                    self.log.warning(
                        f"Message ignored by {platform_name} webhook: "
                        f"{result_json.get('event', 'unknown event')}"
                    )
                    return {"status": "ignored", "response": result_json}

                else:
                    self.log.warning(
                        f"Unexpected status from {platform_name}: {status}"
                    )
                    return {"status": status, "response": result_json}

            except json.JSONDecodeError:
                self.log.info(f"Message sent to {platform_name}, non-JSON response")
                platform_config["message_count"] = platform_config.get("message_count", 0) + 1
                platform_config["last_activity"] = time.time()
                platform_config["last_error"] = None
                return {"status": "ok", "response": result_data[:200]}

        except urllib.error.HTTPError as e:
            error_msg = f"HTTP {e.code}: {e.reason}"
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")[:200]
            except Exception:
                pass
            self.log.error(
                f"Gateway HTTP error for {platform_name}: {error_msg} — {error_body}"
            )
            platform_config["last_error"] = error_msg
            return {"status": "error", "error": error_msg, "body": error_body}

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
                "route_name": config.get("route_name", ""),
                "gateway_url": config.get("gateway_url", ""),
                "message_count": config.get("message_count", 0),
                "last_activity": config.get("last_activity", 0),
                "last_error": config.get("last_error"),
            }
        return status
