"""
A2A Mesh — MCP Client Registry.

Deterministic registry of agents that connect to a mesh node via the MCP bridge
(Streamable HTTP on :8100) instead of running their own mesh daemon.

These agents are "end devices": they have no P2P/PG transport of their own and
communicate through the parent node's bridge. The registry persists to a JSON
file that:
  - the bridge writes into (on every A2A tool call it records which remote
    agent is talking through it),
  - the mesh node reads when building the topology so each end device appears
    under its parent node with transport="mcp".

File: ~/.hermes/scripts/a2a_mesh/data/mcp_clients.json
"""

import json
import os
import threading
import time

REGISTRY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "mcp_clients.json"
)

_lock = threading.Lock()


def _load() -> dict:
    try:
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (IOError, json.JSONDecodeError):
        return {}


def _save(data: dict) -> None:
    os.makedirs(os.path.dirname(REGISTRY_PATH), exist_ok=True)
    tmp = REGISTRY_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, REGISTRY_PATH)


def register_agent(name: str, parent_node: str, transport: str = "mcp") -> None:
    """Record that an agent is reachable through this node's MCP bridge."""
    name = (name or "").strip().lower()
    parent_node = (parent_node or "").strip()
    if not name or not parent_node:
        return
    with _lock:
        data = _load()
        existing = data.get(name, {})
        now = time.time()
        data[name] = {
            "name": name,
            "parent_node": parent_node,
            "transport": transport,
            "first_seen": existing.get("first_seen", now),
            "last_seen": now,
            "online": True,
        }
        _save(data)


def register_sender(name: str, parent_node: str) -> None:
    """Convenience wrapper used by the message bus tool hooks."""
    register_agent(name, parent_node)


def touch(name: str, parent_node: str) -> None:
    """Update last_seen for an already-known agent (non-fatal if unknown)."""
    register_agent(name, parent_node)


def list_clients(parent_node: str = "") -> list:
    """Return all registered MCP end devices, optionally filtered by parent."""
    with _lock:
        data = _load()
    out = []
    for name, info in data.items():
        if parent_node and info.get("parent_node") != parent_node:
            continue
        out.append({
            "name": name,
            "parent_node": info.get("parent_node", ""),
            "transport": info.get("transport", "mcp"),
            "first_seen": info.get("first_seen"),
            "last_seen": info.get("last_seen"),
            "online": bool(info.get("online", True)),
        })
    return out


def is_empty() -> bool:
    with _lock:
        return not _load()
