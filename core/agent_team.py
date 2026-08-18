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
    """Update a node's team configuration.
    
    Marveen-inspired: sanitizes input to strip self-references, unknown nodes,
    and detects reportsTo cycles before persisting.
    """
    config = get_team_config()
    if node_name not in config["nodes"]:
        config["nodes"][node_name] = {"role": "member", "reports_to": "Nova", "delegates_to": [], "auto_delegation": False}
    
    # Sanitize: strip self-refs, unknown, cycles
    if reports_to is not None or delegates_to is not None:
        clean, warnings = sanitize_team_config(
            node_name, role or config["nodes"][node_name]["role"],
            reports_to, delegates_to, auto_delegation,
        )
        role, reports_to, delegates_to, auto_delegation = clean
        if warnings["dropped_self"] or warnings["dropped_unknown"]:
            log.warning(f"Team config sanitize warnings for {node_name}: {warnings}")
    
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
                "security_profile": resolve_security_profile(name, cfg.get("security_profile")),
            }
            for name, cfg in config["nodes"].items()
        ],
        "leader": next((name for name, cfg in config["nodes"].items() if cfg["role"] == "leader"), None),
    }


def _save(config):
    os.makedirs(os.path.dirname(TEAM_FILE), exist_ok=True)
    with open(TEAM_FILE, "w") as f:
        json.dump(config, f, indent=2)


# ── Marveen-inspired: Team Cycle Detection ──
# Prevents creating a reportsTo cycle (A→B→A) that would orphan a subtree.

def reports_to_creates_cycle(node_name, proposed_reports_to, config=None):
    """Check if setting node_name.reports_to = proposed_reports_to would create a cycle.
    
    Returns True if a cycle would be created, False otherwise.
    Pure function — reads config from file or accepts it as parameter.
    """
    if not proposed_reports_to or proposed_reports_to == node_name:
        return proposed_reports_to == node_name  # self-reference is a cycle
    if config is None:
        config = get_team_config()
    cursor = proposed_reports_to
    visited = set()
    while cursor and cursor not in visited:
        if cursor == node_name:
            return True  # cycle detected
        visited.add(cursor)
        node = config["nodes"].get(cursor, {})
        cursor = node.get("reports_to")
    return False


def sanitize_team_config(node_name, role, reports_to, delegates_to, auto_delegation):
    """Sanitize team config update — strip self-references and unknown nodes.
    
    Returns (clean_config, warnings) where warnings lists dropped fields.
    Marveen-inspired: never silently accept bad references.
    """
    config = get_team_config()
    known = set(config["nodes"].keys())
    warnings = {"dropped_self": [], "dropped_unknown": []}
    
    # reports_to: strip self-reference and unknown
    if reports_to == node_name:
        warnings["dropped_self"].append("reports_to")
        reports_to = None
    elif reports_to and reports_to not in known:
        warnings["dropped_unknown"].append(reports_to)
        reports_to = None
    
    # delegates_to: strip self-references and unknown
    if delegates_to:
        clean = []
        for d in delegates_to:
            if d == node_name:
                warnings["dropped_self"].append("delegates_to")
                continue
            if d not in known:
                warnings["dropped_unknown"].append(d)
                continue
            if d not in clean:
                clean.append(d)
        delegates_to = clean
    
    # Check for cycle before accepting
    if reports_to and reports_to_creates_cycle(node_name, reports_to, config):
        warnings["dropped_self"].append("reports_to (cycle)")
        reports_to = None
    
    return (role, reports_to, delegates_to, auto_delegation), warnings


# ── Marveen-inspired: Role-based Security Profile ──
# Leader gets full access (applier), members get limited access (default).

def resolve_security_profile(node_name, stored_profile=None):
    """Resolve a node's effective security profile based on role.
    
    - Explicit non-default stored profile wins (e.g. pinned to 'sub-dev')
    - Otherwise role-derived: leader → 'applier' (full), member → 'default' (limited)
    """
    if stored_profile and stored_profile.strip() and stored_profile.strip() != "default":
        return stored_profile.strip()
    role = get_node_role(node_name).get("role", "member")
    return "applier" if role == "leader" else "default"


# ── Marveen-inspired: Cleanup Team References ──
# When a node is removed, clean up dangling references in other nodes' configs.

def cleanup_team_references(removed_name):
    """Remove a node from all other nodes' delegatesTo and fix reportsTo.
    
    Members who reported to the removed node fall back to the leader.
    """
    config = get_team_config()
    leader = next((n for n, c in config["nodes"].items() if c["role"] == "leader"), None)
    dirty = False
    for name, cfg in config["nodes"].items():
        if cfg.get("reports_to") == removed_name:
            cfg["reports_to"] = leader
            dirty = True
        if removed_name in cfg.get("delegates_to", []):
            cfg["delegates_to"] = [d for d in cfg["delegates_to"] if d != removed_name]
            dirty = True
    if dirty:
        _save(config)
        log.info(f"Cleaned up team references for removed node: {removed_name}")
    return dirty