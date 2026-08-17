"""
Channel Health Monitor — monitor communication channel liveness.

Inspired by Marveen's channel-health-monitor:
  - Periodic check of Telegram/Discord/webhook channels
  - Detect dead channels → alert
  - Auto-reconnect attempts

For A2A Mesh:
  - Check peer node connections (P2P, SSH, Tailscale)
  - Monitor Telegram bot polling status
  - Alert on dead channels
"""

import time
import logging
import asyncio

log = logging.getLogger("channel_health")

_health_cache = {}
_last_check = 0


async def check_channel_health(node):
    """Check health of a node's channels."""
    channels = {
        "p2p": {"status": "unknown", "latency_ms": 0},
        "ssh": {"status": "unknown", "latency_ms": 0},
        "telegram": {"status": "unknown"},
    }
    
    # P2P check — try to connect to peer's port
    try:
        if hasattr(node, 'router') and node.router:
            for name, transport in node.router.transports.items():
                if transport.is_available():
                    channels[name] = {"status": "ok", "latency_ms": getattr(transport, '_last_latency', 0)}
                else:
                    channels[name] = {"status": "down", "latency_ms": 0}
    except Exception as e:
        log.debug(f"Channel health check error: {e}")
    
    # Telegram check — is the bot polling?
    try:
        if hasattr(node, 'dashboard') and node.dashboard:
            tg_status = getattr(node.dashboard, '_telegram_status', 'unknown')
            channels["telegram"] = {"status": tg_status}
    except Exception:
        pass
    
    return channels


async def health_monitor_tick(node):
    """Periodic health check tick."""
    global _last_check, _health_cache
    
    results = await check_channel_health(node)
    _health_cache = results
    _last_check = time.time()
    
    # Alert on down channels
    for name, status in results.items():
        if status.get("status") == "down":
            log.warning(f"Channel {name} is DOWN")
    
    return results


def get_health_status():
    """Get cached health status for dashboard."""
    return {
        "channels": _health_cache,
        "last_check": _last_check,
        "last_check_ago_sec": int(time.time() - _last_check) if _last_check else None,
    }