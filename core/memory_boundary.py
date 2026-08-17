"""
Memory Boundary — per-agent memory isolation.

Inspired by Marveen's memory-boundary:
  - Each agent resolves to its own project key → own auto-memory
  - Prevents cross-principal memory leak on multi-user installs
  - Plant stub git repo in agent dir to stop walk-up

For A2A Mesh:
  - Each node has its own memory namespace in PG
  - Boundary enforcement: Nova can't read Morzsa's private memories
  - Shared memories are explicitly tagged
"""

import os
import json
import logging
from pathlib import Path

log = logging.getLogger("memory_boundary")

MESH_DIR = os.path.expanduser("~/.hermes/scripts/a2a_mesh")
BOUNDARIES_FILE = os.path.join(MESH_DIR, "data", "memory_boundaries.json")

# Default boundary config
_DEFAULT = {
    "nodes": {
        "Nova": {"namespace": "nova", "private": True, "shared": ["mesh_alerts", "mesh_config", "mesh_topology"]},
        "Morzsa": {"namespace": "morzsa", "private": True, "shared": ["mesh_alerts", "mesh_config", "mesh_topology"]},
        "Runa": {"namespace": "runa", "private": True, "shared": ["mesh_alerts", "mesh_config", "mesh_topology"]},
    },
    "default_policy": "private",
    "leak_prevention": True,
}


def get_boundaries():
    """Get memory boundary config."""
    try:
        with open(BOUNDARIES_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return _DEFAULT


def check_access(reader_node, target_namespace):
    """Check if a node can access a memory namespace."""
    config = get_boundaries()
    
    # Own namespace always accessible
    reader_ns = config["nodes"].get(reader_node, {}).get("namespace", reader_node.lower())
    if reader_ns == target_namespace:
        return True, "own"
    
    # Shared namespaces
    shared = config["nodes"].get(reader_node, {}).get("shared", [])
    if target_namespace in shared:
        return True, "shared"
    
    # Default policy
    if config.get("default_policy") == "private":
        return False, "blocked:private"
    
    return True, "default"


def get_node_namespaces(node_name):
    """Get all accessible namespaces for a node."""
    config = get_boundaries()
    node_cfg = config["nodes"].get(node_name, {})
    ns_list = [node_cfg.get("namespace", node_name.lower())]
    ns_list.extend(node_cfg.get("shared", []))
    return ns_list


def add_shared_namespace(node_name, namespace):
    """Add a shared namespace to a node."""
    config = get_boundaries()
    if node_name not in config["nodes"]:
        config["nodes"][node_name] = {"namespace": node_name.lower(), "private": True, "shared": []}
    if namespace not in config["nodes"][node_name]["shared"]:
        config["nodes"][node_name]["shared"].append(namespace)
    _save(config)
    return True


def get_boundary_status():
    """Get boundary status for dashboard."""
    config = get_boundaries()
    return {
        "nodes": [
            {
                "name": name,
                "namespace": cfg.get("namespace", name.lower()),
                "private": cfg.get("private", True),
                "shared_count": len(cfg.get("shared", [])),
                "shared": cfg.get("shared", []),
            }
            for name, cfg in config["nodes"].items()
        ],
        "default_policy": config.get("default_policy", "private"),
        "leak_prevention": config.get("leak_prevention", True),
    }


def _save(config):
    os.makedirs(os.path.dirname(BOUNDARIES_FILE), exist_ok=True)
    with open(BOUNDARIES_FILE, "w") as f:
        json.dump(config, f, indent=2)