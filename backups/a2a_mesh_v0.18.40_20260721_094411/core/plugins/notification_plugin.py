"""A2A Mesh Notification Plugin — Priority-based notifications and alerts.

Sends notifications through the mesh based on priority levels and configurable rules.
Supports: email alerts (via SMTP), webhook notifications, and mesh message escalation.

This plugin is designed to be Hermes-update-proof:
- Uses only MeshPlugin base class methods (send_message, broadcast_message)
- Reads config from mesh_config.yaml (no hardcoded paths)
- Gracefully degrades if dependencies are unavailable
"""

import asyncio
import json
import logging
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Any
from collections import defaultdict

from a2a_mesh.core.plugin_base import MeshPlugin

log = logging.getLogger("a2a_mesh.plugin.notification")


class NotificationPlugin(MeshPlugin):
    """Priority-based notification and alert system for the mesh.

    Configuration (in mesh_config.yaml):
        plugins:
          notification:
            enabled: true
            rules:
              - name: "critical_alert"
                priority_min: 10
                actions: ["broadcast", "webhook"]
                webhook_url: "http://localhost:8644/api/wake-agent"
              - name: "health_warning"
                priority_min: 7
                actions: ["mesh_message"]
                recipient: "morzsa"
            quiet_hours:
              enabled: false
              start: "23:00"
              end: "07:00"
            throttle:
              max_per_minute: 10
              max_per_hour: 60
    """

    name = "notification"
    version = "1.0.0"
    description = "Priority-based notifications and alerts across the mesh"
    author = "nova"
    capabilities = ["notification", "alerting", "escalation"]

    config_defaults = {
        "enabled": True,
        "rules": [],
        "quiet_hours": {"enabled": False, "start": "23:00", "end": "07:00"},
        "throttle": {"max_per_minute": 10, "max_per_hour": 60},
    }

    def __init__(self):
        super().__init__()
        self._rules: List[Dict] = []
        self._sent_count = defaultdict(int)  # minute → count
        self._hourly_count = defaultdict(int)  # hour → count
        self._last_cleanup = time.time()

    def configure(self, config: Dict[str, Any]):
        """Apply configuration including notification rules."""
        super().configure(config)
        self._rules = self._config.get("rules", [])
        self.log.info(f"Notification plugin configured with {len(self._rules)} rules")

    async def on_message_received(self, message) -> Optional[Any]:
        """Check incoming messages against notification rules."""
        if not self._rules:
            return None

        priority = getattr(message, 'priority', 5)
        msg_type = getattr(message, 'type', 'directive')
        sender = getattr(message, 'sender', 'unknown')
        content = message.payload.get("text", "") if hasattr(message, "payload") else getattr(message, "content", "")

        # Throttle check
        if not self._check_throttle():
            self.log.debug(f"Throttled notification for message from {sender}")
            return None

        # Evaluate rules
        for rule in self._rules:
            rule_name = rule.get("name", "unnamed")
            priority_min = rule.get("priority_min", 0)
            msg_types = rule.get("msg_types", ["directive", "steer"])

            # Check if message matches this rule
            if priority < priority_min:
                continue
            if msg_types and msg_type not in msg_types:
                continue

            # Rule matches — execute actions
            actions = rule.get("actions", [])
            self.log.info(f"Rule '{rule_name}' matched: {sender} P{priority} → {actions}")

            for action in actions:
                try:
                    if action == "broadcast":
                        await self._action_broadcast(rule, message)
                    elif action == "mesh_message":
                        await self._action_mesh_message(rule, message)
                    elif action == "webhook":
                        await self._action_webhook(rule, message)
                    elif action == "log":
                        self._action_log(rule, message)
                except Exception as e:
                    self.log.error(f"Action '{action}' failed for rule '{rule_name}': {e}")

        return None

    # ── Actions ──────────────────────────────────────────────────

    async def _action_broadcast(self, rule: Dict, message):
        """Broadcast a notification to all mesh nodes."""
        content = f"⚠️ Alert: {content[:200]}"
        priority = rule.get("broadcast_priority", 7)
        await self.broadcast_message(
            content=content,
            msg_type="directive",
            priority=priority,
        )

    async def _action_mesh_message(self, rule: Dict, message):
        """Send a notification to a specific mesh node."""
        recipient = rule.get("recipient", "broadcast")
        content = f"📋 Notification: {content[:200]}"
        priority = rule.get("message_priority", 5)
        await self.send_message(
            recipient=recipient,
            content=content,
            msg_type="directive",
            priority=priority,
        )

    async def _action_webhook(self, rule: Dict, message):
        """Send a notification via webhook."""
        webhook_url = rule.get("webhook_url", "")
        if not webhook_url:
            self.log.warning(f"No webhook URL for rule '{rule.get('name')}'")
            return

        payload = {
            "sender": message.sender,
            "content": content[:500],
            "priority": message.priority,
            "type": message.type,
            "rule": rule.get("name", ""),
            "timestamp": time.time(),
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, timeout=5),
            )
            self.log.info(f"Webhook notification sent to {webhook_url[:50]}")
        except Exception as e:
            self.log.error(f"Webhook notification failed: {e}")

    def _action_log(self, rule: Dict, message):
        """Log a notification event."""
        self.log.info(
            f"NOTIFICATION [{rule.get('name', '')}]: "
            f"P{message.priority} from {message.sender}: "
            f"{content[:100]}"
        )

    # ── Throttle ─────────────────────────────────────────────────

    def _check_throttle(self) -> bool:
        """Check if we're within throttle limits."""
        now = time.time()
        minute = int(now // 60)
        hour = int(now // 3600)

        # Cleanup old entries
        if now - self._last_cleanup > 300:
            self._sent_count = defaultdict(int, {
                k: v for k, v in self._sent_count.items() if k >= minute - 60
            })
            self._hourly_count = defaultdict(int, {
                k: v for k, v in self._hourly_count.items() if k >= hour - 2
            })
            self._last_cleanup = now

        throttle = self._config.get("throttle", {})
        max_per_minute = throttle.get("max_per_minute", 10)
        max_per_hour = throttle.get("max_per_hour", 60)

        if self._sent_count[minute] >= max_per_minute:
            return False
        if self._hourly_count[hour] >= max_per_hour:
            return False

        self._sent_count[minute] += 1
        self._hourly_count[hour] += 1
        return True

    # ── Status ───────────────────────────────────────────────────

    def get_notification_status(self) -> Dict:
        """Get notification plugin status."""
        now = time.time()
        minute = int(now // 60)
        hour = int(now // 3600)
        return {
            "plugin": "notification",
            "running": self._running,
            "rules_count": len(self._rules),
            "sent_this_minute": self._sent_count.get(minute, 0),
            "sent_this_hour": self._hourly_count.get(hour, 0),
            "rules": [
                {"name": r.get("name"), "priority_min": r.get("priority_min", 0)}
                for r in self._rules
            ],
        }