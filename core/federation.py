"""
Federation — inter-mesh federation for connecting separate mesh clusters.

Inspired by Marveen's federation:
  - Bridge: connect to remote mesh via SSH tunnel
  - Capabilities: exchange capability summaries between meshes
  - Poller: periodic poll of remote mesh status
  - Onboarding: enroll new mesh nodes

For A2A Mesh:
  - Connect multiple A2A Mesh clusters (e.g., home + office)
  - Exchange node capabilities
  - Cross-mesh delegation
"""

import time
import logging
import json
import os

log = logging.getLogger("federation")

FEDERATION_CONFIG = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/federation.json")


def get_federation_config():
    """Get federation configuration."""
    try:
        with open(FEDERATION_CONFIG, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"peers": [], "enabled": False}


def add_federation_peer(name, address, port=8650, ssh_tunnel=False):
    """Add a federated peer mesh."""
    config = get_federation_config()
    peer = {
        "name": name,
        "address": address,
        "port": port,
        "ssh_tunnel": ssh_tunnel,
        "added_at": time.time(),
        "status": "unknown",
    }
    # Remove existing with same name
    config["peers"] = [p for p in config["peers"] if p["name"] != name]
    config["peers"].append(peer)
    _save_config(config)
    return peer


def remove_federation_peer(name):
    """Remove a federated peer."""
    config = get_federation_config()
    before = len(config["peers"])
    config["peers"] = [p for p in config["peers"] if p["name"] != name]
    _save_config(config)
    return len(config["peers"]) < before


def get_federation_status():
    """Get federation status for dashboard."""
    config = get_federation_config()
    return {
        "enabled": config.get("enabled", False),
        "peer_count": len(config.get("peers", [])),
        "peers": [
            {
                "name": p["name"],
                "address": p["address"],
                "port": p.get("port", 8650),
                "status": p.get("status", "unknown"),
                "ssh_tunnel": p.get("ssh_tunnel", False),
            }
            for p in config.get("peers", [])
        ],
    }


def _save_config(config):
    os.makedirs(os.path.dirname(FEDERATION_CONFIG), exist_ok=True)
    with open(FEDERATION_CONFIG, "w") as f:
        json.dump(config, f, indent=2)