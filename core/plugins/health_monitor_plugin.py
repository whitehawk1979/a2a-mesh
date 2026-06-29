"""A2A Mesh Health Monitor Plugin — Periodic health checks and alerts.

Monitors mesh node health and sends alerts when:
- A peer goes offline or becomes unresponsive
- Health scores drop below thresholds
- Transport connections are degraded

Hermes-update-proof: uses only MeshPlugin base class methods.
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Any

from a2a_mesh.core.plugin_base import MeshPlugin

log = logging.getLogger("a2a_mesh.plugin.health_monitor")


class HealthMonitorPlugin(MeshPlugin):
    """Periodic health monitoring and alerting for mesh nodes.

    Configuration (in mesh_config.yaml):
        plugins:
          health_monitor:
            enabled: true
            check_interval: 60        # seconds between checks
            offline_threshold: 300     # seconds before declaring offline
            health_warning: 0.7        # health score below this = warning
            health_critical: 0.3       # health score below this = critical
            alert_channels:            # where to send alerts
              - mesh_broadcast
            alert_recipient: "broadcast"
    """

    name = "health_monitor"
    version = "1.0.0"
    description = "Periodic health monitoring and alerting for mesh nodes"
    author = "nova"
    capabilities = ["health_monitor", "alerting"]

    config_defaults = {
        "enabled": True,
        "check_interval": 60,
        "offline_threshold": 300,
        "health_warning": 0.7,
        "health_critical": 0.3,
        "alert_channels": ["mesh_broadcast"],
        "alert_recipient": "broadcast",
    }

    def __init__(self):
        super().__init__()
        self._check_task = None
        self._peer_health: Dict[str, Dict] = {}  # peer_name → {last_seen, health, alerts_sent}
        self._stats = {
            "checks_run": 0,
            "alerts_sent": 0,
            "peers_offline": 0,
        }

    async def on_start(self):
        """Start the health monitoring loop."""
        await super().on_start()
        interval = self._config.get("check_interval", 60)
        self._check_task = self.create_task(self._health_check_loop(interval))
        self.log.info(f"Health monitor started — checking every {interval}s")

    async def on_stop(self):
        """Stop the health monitoring loop."""
        if self._check_task and not self._check_task.done():
            self._check_task.cancel()
        await super().on_stop()

    async def on_peer_connected(self, peer_name: str, info: Dict):
        """Track peer connections for health monitoring."""
        self._peer_health[peer_name] = {
            "last_seen": time.time(),
            "health": 1.0,
            "alerts_sent": 0,
            "info": info,
        }
        self.log.info(f"Health monitor: peer {peer_name} connected")

    async def on_peer_disconnected(self, peer_name: str):
        """Track peer disconnections."""
        if peer_name in self._peer_health:
            self._peer_health[peer_name]["last_seen"] = time.time()
            self._peer_health[peer_name]["health"] = 0.0

    async def on_health_change(self, peer_name: str, old_health: float, new_health: float):
        """React to health score changes."""
        health_warning = self._config.get("health_warning", 0.7)
        health_critical = self._config.get("health_critical", 0.3)

        if peer_name in self._peer_health:
            self._peer_health[peer_name]["health"] = new_health

        # Alert on critical health
        if new_health < health_critical and old_health >= health_critical:
            await self._send_alert(
                f"🔴 CRITICAL: {peer_name} health dropped to {new_health:.2f} (was {old_health:.2f})",
                priority=10,
            )
        elif new_health < health_warning and old_health >= health_warning:
            await self._send_alert(
                f"🟡 WARNING: {peer_name} health degraded to {new_health:.2f} (was {old_health:.2f})",
                priority=7,
            )

    # ── Health check loop ────────────────────────────────────────

    async def _health_check_loop(self, interval: int):
        """Periodically check peer health and send alerts."""
        while self._running:
            try:
                await self._run_health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Health check error: {e}")

            await asyncio.sleep(interval)

    async def _run_health_check(self):
        """Run a single health check cycle — actively probes peers via HTTP/P2P."""
        self._stats["checks_run"] += 1
        offline_threshold = self._config.get("offline_threshold", 300)
        now = time.time()
        peers_offline = 0

        # Check registry for known peers
        if self._node and hasattr(self._node, 'dashboard'):
            registry = getattr(self._node.dashboard, 'registry', None)
            if registry:
                all_agents = registry.list_agents()
                for card, health in all_agents:
                    peer_name = card.name
                    if peer_name == self._node.node_name:
                        continue  # Skip self

                    # Active health check: probe peer via HTTP/P2P to update last_health_check
                    try:
                        await registry.check_agent_health(peer_name)
                    except Exception as e:
                        self.log.debug(f"Health monitor: active check failed for {peer_name}: {e}")

                    # Re-fetch updated health record after active check
                    updated_health = registry.health_records.get(peer_name)
                    if updated_health:
                        health = updated_health

                    last_seen_raw = health.last_health_check
                    health_val = health.health_score

                    # Fallback: if last_health_check is None/0, try peer_discovery last_seen
                    if not last_seen_raw or last_seen_raw <= 0:
                        try:
                            all_peers = self._node.peer_discovery.get_all_peers()
                            peer_info = all_peers.get(peer_name)
                            if peer_info and peer_info.last_seen and peer_info.last_seen > 0:
                                last_seen_raw = peer_info.last_seen
                                self.log.debug(f"Health monitor: using peer_discovery last_seen for {peer_name}: {last_seen_raw}")
                        except Exception as e:
                            self.log.debug(f"Health monitor: peer_discovery fallback failed for {peer_name}: {e}")

                    # Handle "never seen" case: None, 0, or negative values
                    never_seen = not last_seen_raw or last_seen_raw <= 0

                    # Update tracking
                    if peer_name not in self._peer_health:
                        self._peer_health[peer_name] = {"alerts_sent": 0}

                    self._peer_health[peer_name]["last_seen"] = last_seen_raw or 0
                    self._peer_health[peer_name]["health"] = health_val

                    # Check if peer is offline
                    if never_seen:
                        time_since = None  # never seen
                        is_offline = True
                        time_desc = "never"
                    else:
                        time_since = now - last_seen_raw
                        is_offline = time_since > offline_threshold
                        time_desc = f"{int(time_since)}s"

                    if is_offline:
                        peers_offline += 1
                        alerts_sent = self._peer_health[peer_name].get("alerts_sent", 0)
                        # Only alert once per offline period (max every 5 minutes)
                        if alerts_sent == 0 or (now - self._peer_health[peer_name].get("last_alert", 0)) > 300:
                            await self._send_alert(
                                f"🔴 OFFLINE: {peer_name} not seen for {time_desc}",
                                priority=10,
                            )
                            self._peer_health[peer_name]["alerts_sent"] = alerts_sent + 1
                            self._peer_health[peer_name]["last_alert"] = now
                            self._stats["alerts_sent"] += 1

        self._stats["peers_offline"] = peers_offline

    # ── Alert sending ────────────────────────────────────────────

    async def _send_alert(self, content: str, priority: int = 7):
        """Send an alert through configured channels."""
        alert_channels = self._config.get("alert_channels", ["mesh_broadcast"])

        for channel in alert_channels:
            try:
                if channel == "mesh_broadcast":
                    await self.broadcast_message(
                        content=content,
                        msg_type="directive",
                        priority=priority,
                    )
                elif channel == "mesh_message":
                    recipient = self._config.get("alert_recipient", "broadcast")
                    await self.send_message(
                        recipient=recipient,
                        content=content,
                        msg_type="directive",
                        priority=priority,
                    )
            except Exception as e:
                self.log.error(f"Failed to send alert via {channel}: {e}")

    # ── Status ───────────────────────────────────────────────────

    def get_health_status(self) -> Dict:
        """Get health monitor status."""
        return {
            "plugin": "health_monitor",
            "running": self._running,
            "peers_tracked": len(self._peer_health),
            "stats": self._stats,
            "peer_health": {
                name: {
                    "last_seen": info.get("last_seen", 0),
                    "health": info.get("health", 1.0),
                    "alerts_sent": info.get("alerts_sent", 0),
                }
                for name, info in self._peer_health.items()
            },
        }