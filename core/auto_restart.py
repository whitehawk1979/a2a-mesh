"""
Auto-Restart — periodic session restart for lean context.

Inspired by Marveen's auto-restart:
  - Long-lived sessions accumulate context → slower + costlier
  - Restart periodically (fresh: drop conversation, continue: keep it)
  - Default: after nightly dream consolidation

For A2A Mesh:
  - Tracks node uptime
  - Suggests restart after configurable interval (default 24h)
  - Two modes: 'fresh' (full restart) vs 'continue' (graceful reload)
  - Integrates with self-healing loop
"""

import time
import json
import os
import logging

log = logging.getLogger("auto_restart")

STATE_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/auto_restart_state.json")
DEFAULT_RESTART_INTERVAL = 86400  # 24 hours
DEFAULT_MODE = "continue"  # "fresh" or "continue"


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def record_start(node_name, mode=None):
    """Record a node start/restart."""
    state = load_state()
    state[node_name] = {
        "started_at": time.time(),
        "mode": mode or DEFAULT_MODE,
        "restart_count": state.get(node_name, {}).get("restart_count", 0),
    }
    save_state(state)
    log.info(f"Auto-restart: {node_name} started at {state[node_name]['started_at']}")


def check_restart_needed(node_name, interval=None):
    """Check if a node needs restart."""
    state = load_state()
    node_state = state.get(node_name, {})
    started_at = node_state.get("started_at", time.time())
    uptime = time.time() - started_at
    restart_interval = interval or DEFAULT_RESTART_INTERVAL
    
    if uptime >= restart_interval:
        return {
            "node": node_name,
            "needs_restart": True,
            "uptime_hours": round(uptime / 3600, 1),
            "uptime_seconds": int(uptime),
            "mode": node_state.get("mode", DEFAULT_MODE),
            "restart_count": node_state.get("restart_count", 0),
            "reason": f"Uptime {round(uptime/3600,1)}h >= {restart_interval/3600}h threshold"
        }
    return {
        "node": node_name,
        "needs_restart": False,
        "uptime_hours": round(uptime / 3600, 1),
        "uptime_seconds": int(uptime),
        "mode": node_state.get("mode", DEFAULT_MODE),
        "restart_count": node_state.get("restart_count", 0),
    }


def record_restart(node_name, mode=None):
    """Record that a restart was performed."""
    state = load_state()
    old = state.get(node_name, {})
    state[node_name] = {
        "started_at": time.time(),
        "mode": mode or old.get("mode", DEFAULT_MODE),
        "restart_count": old.get("restart_count", 0) + 1,
        "last_restart": time.time(),
    }
    save_state(state)
    log.info(f"Auto-restart: {node_name} restarted (count: {state[node_name]['restart_count']})")
    return state[node_name]


def get_all_nodes_status():
    """Get restart status for all known nodes."""
    state = load_state()
    return {node: check_restart_needed(node) for node in state}