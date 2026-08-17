"""
Agent Team — hierarchy and team structure for A2A Mesh nodes.

Inspired by Marveen's agent-team:
  - Each agent declares: role (leader|member), reportsTo, delegatesTo
  - autoDelegation: can split tasks automatically
  - Security profile derived from role

For A2A Mesh:
  - Nova = leader (orchestrator), Morzsa+Runa = members (workers)
  - Reports-to and delegates-to relationships
  - Visual hierarchy in dashboard
"""

import json
import os
import time
import logging

log = logging.getLogger("agent_team")

TEAM_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/team_config.json")

_DEFAULT = {
    "nodes": {
        "Nova": {
            "role": "leader",
            "reports_to": None,
            "delegates_to": ["Morzsa", "Runa"],
            "auto_delegation": True,
            "can_split_tasks": True,
        },
        "Morzsa": {
            "role": "member",
            "reports_to": "Nova",
            "delegates_to": [],
            "auto_delegation": False,
            "can_split_tasks": False,
        },
        "Runa": {
            "role": "member",
            "reports_to": "Nova",
            "delegates_to": [],
            "auto_delegation": False,
            "can_split_tasks": False,
        },
    },
}


def get_team_config():
    """Get team configuration."""
    try:
        with open(TEAM_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return _DEFAULT


def get_node_role(node_name):
    """Get a node's role configuration."""
    config = get_team_config()
    return config["nodes"].get(node_name, {"role": "member", "reports_to": "Nova", "delegates_to": [], "auto_delegation": False})


def can_delegate(from_node, to_node):
    """Check if from_node can delegate to to_node."""
    config = get_team_config()
    node = config["nodes"].get(from_node, {})
    return to_node in node.get("delegates_to", [])


def get_delegation_targets(from_node):
    """Get list of nodes that from_node can delegate to."""
    config = get_team_config()
    return config["nodes"].get(from_node, {}).get("delegates_to", [])


def update_node_role(node_name, role, reports_to=None, delegates_to=None, auto_delegation=None):
    """Update a node's team configuration."""
    config = get_team_config()
    if node_name not in config["nodes"]:
        config["nodes"][node_name] = {"role": "member", "reports_to": "Nova", "delegates_to": [], "auto_delegation": False}
    
    if role is not None:
        config["nodes"][node_name]["role"] = role
    if reports_to is not None:
        config["nodes"][node_name]["reports_to"] = reports_to
    if delegates_to is not None:
        config["nodes"][node_name]["delegates_to"] = delegates_to
    if auto_delegation is not None:
        config["nodes"][node_name]["auto_delegation"] = auto_delegation
    
    _save(config)
    return config["nodes"][node_name]


def get_team_status():
    """Get team hierarchy status for dashboard."""
    config = get_team_config()
    return {
        "nodes": [
            {
                "name": name,
                "role": cfg["role"],
                "reports_to": cfg.get("reports_to"),
                "delegates_to": cfg.get("delegates_to", []),
                "auto_delegation": cfg.get("auto_delegation", False),
                "can_split_tasks": cfg.get("can_split_tasks", False),
            }
            for name, cfg in config["nodes"].items()
        ],
        "leader": next((name for name, cfg in config["nodes"].items() if cfg["role"] == "leader"), None),
    }


def _save(config):
    os.makedirs(os.path.dirname(TEAM_FILE), exist_ok=True)
    with open(TEAM_FILE, "w") as f:
        json.dump(config, f, indent=2)