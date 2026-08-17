"""
Store Watcher — monitor file changes in the mesh data directory.

Inspired by Marveen's store-watcher:
  - Watch STORE_DIR for file changes
  - Log agent-created files (not system files)
  - Detect unexpected modifications

For A2A Mesh:
  - Watches data/ directory for changes
  - Logs file events (create, modify, delete)
  - System files filtered out
  - Detects config drift
"""

import os
import time
import json
import logging
from pathlib import Path

log = logging.getLogger("store_watcher")

WATCH_DIR = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data")
EVENT_LOG = os.path.join(WATCH_DIR, "store_events.json")

# System files to ignore
SYSTEM_FILES = {
    "store_events.json", "DREAM.md", "cost_ledger.json",
    "model_fallback_state.json", "pending_retries.json",
    "auto_restart_state.json", "worker_liveness.json",
    "token_usage.json", "trust_graph.json",
    "kanban_boards.json", "projects.json",
}

# Sensitive file patterns (don't log content, just event)
SENSITIVE_PATTERNS = {".token", ".key", ".secret", ".pass", "credentials"}


def scan_directory():
    """Scan the data directory and return file inventory."""
    inventory = {}
    if not os.path.isdir(WATCH_DIR):
        return inventory
    
    for entry in os.listdir(WATCH_DIR):
        path = os.path.join(WATCH_DIR, entry)
        if os.path.isfile(path):
            stat = os.stat(path)
            is_sensitive = any(p in entry.lower() for p in SENSITIVE_PATTERNS)
            inventory[entry] = {
                "size": stat.st_size,
                "modified": stat.st_mtime,
                "is_system": entry in SYSTEM_FILES,
                "is_sensitive": is_sensitive,
            }
    return inventory


def detect_changes(prev_inventory, curr_inventory):
    """Compare two inventories and return changes."""
    changes = []
    
    # New files
    for name, info in curr_inventory.items():
        if name not in prev_inventory:
            changes.append({"file": name, "event": "created", "size": info["size"]})
        elif prev_inventory[name]["modified"] != info["modified"]:
            changes.append({"file": name, "event": "modified", "size": info["size"]})
    
    # Deleted files
    for name in prev_inventory:
        if name not in curr_inventory:
            changes.append({"file": name, "event": "deleted", "size": 0})
    
    return changes


def log_events(changes):
    """Log file change events."""
    events = []
    try:
        with open(EVENT_LOG, "r") as f:
            events = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        events = []
    
    for change in changes:
        change["timestamp"] = time.time()
        events.append(change)
        if not change["file"].startswith("."):
            log.info(f"Store watcher: {change['event']} {change['file']}")
    
    # Keep last 500 events
    events = events[-500:]
    
    with open(EVENT_LOG, "w") as f:
        json.dump(events, f, indent=2)


def get_events(limit=50):
    """Get recent file change events."""
    try:
        with open(EVENT_LOG, "r") as f:
            events = json.load(f)
        return events[-limit:]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def get_inventory_summary():
    """Get summary of data directory."""
    inv = scan_directory()
    total_size = sum(f["size"] for f in inv.values())
    agent_files = [f for f, info in inv.items() if not info["is_system"]]
    return {
        "total_files": len(inv),
        "total_size_bytes": total_size,
        "agent_files": len(agent_files),
        "system_files": len(inv) - len(agent_files),
        "files": list(inv.keys()),
    }