"""
Model Fallback Chain — automatic model downgrade on rate limit / error.

Inspired by Marveen's model-fallback:
  - When primary model hits usage limit, downgrade to next in chain
  - After revert window (no limit), climb back to primary
  - Configurable chain + revert interval

For A2A Mesh:
  - Chain: glm-5.2:cloud → step-3.7-flash:free → gemma4:31b-cloud → smollm2
  - Trigger: 429/503/timeout errors
  - Revert: after 330 min (5.5h) with no errors
  - Per-node tracking
"""

import time
import json
import os
import logging

log = logging.getLogger("model_fallback")

STATE_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/model_fallback_state.json")

DEFAULT_CHAIN = [
    "glm-5.3:cloud",
    "step-3.7-flash:free",
    "gemma4:31b-cloud",
    "smollm2:135m",
]

DEFAULT_REVERT_MINUTES = 330  # 5.5 hours


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


def get_current_model(node_name):
    """Get the current active model for a node."""
    state = load_state()
    node_state = state.get(node_name, {})
    return node_state.get("current_model", DEFAULT_CHAIN[0])


def get_chain_position(node_name):
    """Get current position in the fallback chain."""
    state = load_state()
    node_state = state.get(node_name, {})
    return node_state.get("chain_position", 0)


def record_error(node_name, model, error_type="unknown"):
    """Record a model error and potentially trigger fallback."""
    state = load_state()
    if node_name not in state:
        state[node_name] = {"chain_position": 0, "current_model": DEFAULT_CHAIN[0], "errors": []}
    
    node_state = state[node_name]
    node_state.setdefault("errors", []).append({
        "timestamp": time.time(),
        "model": model,
        "error_type": error_type,
    })
    
    # Keep last 50 errors
    node_state["errors"] = node_state["errors"][-50:]
    
    # Count recent errors (last 5 min)
    now = time.time()
    recent = [e for e in node_state["errors"] if now - e["timestamp"] < 300]
    
    # If 3+ recent errors, downgrade
    if len(recent) >= 3:
        pos = node_state.get("chain_position", 0)
        if pos < len(DEFAULT_CHAIN) - 1:
            pos += 1
            node_state["chain_position"] = pos
            node_state["current_model"] = DEFAULT_CHAIN[pos]
            node_state["downgraded_at"] = now
            log.warning(f"Model fallback: {node_name} downgraded to {DEFAULT_CHAIN[pos]} (pos {pos})")
        else:
            log.error(f"Model fallback: {node_name} already at bottom of chain ({DEFAULT_CHAIN[pos]})")
    
    save_state(state)
    return node_state.get("current_model", DEFAULT_CHAIN[0])


def check_revert(node_name):
    """Check if we should revert to primary model."""
    state = load_state()
    node_state = state.get(node_name, {})
    
    pos = node_state.get("chain_position", 0)
    if pos == 0:
        return DEFAULT_CHAIN[0]  # Already at primary
    
    downgraded_at = node_state.get("downgraded_at", 0)
    if not downgraded_at:
        return node_state.get("current_model", DEFAULT_CHAIN[0])
    
    # Check revert window
    now = time.time()
    elapsed_min = (now - downgraded_at) / 60
    
    # Check no recent errors
    errors = node_state.get("errors", [])
    recent = [e for e in errors if now - e["timestamp"] < 300]
    
    if elapsed_min >= DEFAULT_REVERT_MINUTES and len(recent) == 0:
        # Revert to primary
        node_state["chain_position"] = 0
        node_state["current_model"] = DEFAULT_CHAIN[0]
        node_state["reverted_at"] = now
        save_state(state)
        log.info(f"Model fallback: {node_name} reverted to primary {DEFAULT_CHAIN[0]}")
        return DEFAULT_CHAIN[0]
    
    return node_state.get("current_model", DEFAULT_CHAIN[0])


def get_node_status(node_name):
    """Get full fallback status for a node."""
    state = load_state()
    node_state = state.get(node_name, {})
    pos = node_state.get("chain_position", 0)
    return {
        "node": node_name,
        "current_model": node_state.get("current_model", DEFAULT_CHAIN[0]),
        "chain_position": pos,
        "chain": DEFAULT_CHAIN,
        "downgraded_at": node_state.get("downgraded_at"),
        "recent_errors": len([e for e in node_state.get("errors", []) if time.time() - e["timestamp"] < 300]),
        "total_errors": len(node_state.get("errors", [])),
    }