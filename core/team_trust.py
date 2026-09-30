"""
Team Trust Graph — inter-agent trust relationships.

Inspired by Marveen's team-trust module:
  - Trust graph: which agents trust which
  - Reports-to / delegates-to relationships
  - Explicit trustFrom overrides
  - Symmetric: if either side acknowledges, both trust each other
  - Used by prompt_safety: trusted vs untrusted wrapping

For A2A Mesh:
  - 4 nodes: Nova, Morzsa, Runa, Tor
  - Full mesh = all trust all (default)
  - Supports asymmetric trust (e.g. temporary revocation)
  - Trust levels: full, limited, none
  - Persisted in PG for mesh-wide consistency
"""

import json
import os
import time
import logging

log = logging.getLogger("team_trust")

TRUST_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/trust_graph.json")

# Default trust graph for A2A Mesh (full mesh — 4 nodes)
DEFAULT_TRUST = {
    "Nova": {"Morzsa": "full", "Runa": "full", "Tor": "full"},
    "Morzsa": {"Nova": "full", "Runa": "full", "Tor": "full"},
    "Runa": {"Nova": "full", "Morzsa": "full", "Tor": "full"},
    "Tor": {"Nova": "full", "Morzsa": "full", "Runa": "full"},
}

TRUST_LEVELS = {"full": 3, "limited": 2, "none": 0}


def load_trust_graph():
    """Load trust graph from JSON file."""
    try:
        with open(TRUST_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return DEFAULT_TRUST.copy()


def save_trust_graph(graph):
    """Save trust graph to JSON file."""
    os.makedirs(os.path.dirname(TRUST_FILE), exist_ok=True)
    with open(TRUST_FILE, "w") as f:
        json.dump(graph, f, indent=2)


def is_trusted_peer(agent_a, agent_b):
    """Check if agent_a trusts agent_b.
    Symmetric: if either side acknowledges, both trust each other."""
    graph = load_trust_graph()
    _ga = graph.get(agent_a) or graph.get(_norm(agent_a).capitalize()) or graph.get(_norm(agent_a)) or {}
    _gb = graph.get(agent_b) or graph.get(_norm(agent_b).capitalize()) or graph.get(_norm(agent_b)) or {}
    trust_a_to_b = _ga.get(agent_b) or _ga.get(_norm(agent_b).capitalize()) or _ga.get(_norm(agent_b)) or "none"
    trust_b_to_a = _gb.get(agent_a) or _gb.get(_norm(agent_a).capitalize()) or _gb.get(_norm(agent_a)) or "none"
    # Symmetric: take the higher trust level
    level = max(TRUST_LEVELS.get(trust_a_to_b, 0), TRUST_LEVELS.get(trust_b_to_a, 0))
    return level >= TRUST_LEVELS["limited"]


def _norm(name):
    """Normalize agent name for trust lookups — node names are lowercase in the
    mesh (nova, morzsa, runa, tor) but the trust graph historically stores
    capitalized keys (Nova, Morzsa...). Case-insensitive lookup prevents
    'Rejected message from untrusted peer' false negatives."""
    return (name or "").strip().lower()

def get_trust_level(agent_a, agent_b):
    """Get trust level between two agents. Returns 'full', 'limited', or 'none'."""
    graph = load_trust_graph()
    # Case-insensitive lookup: graph keys may be capitalized while node names are lowercase
    _ga = graph.get(agent_a) or graph.get(_norm(agent_a).capitalize()) or graph.get(_norm(agent_a)) or {}
    _gb = graph.get(agent_b) or graph.get(_norm(agent_b).capitalize()) or graph.get(_norm(agent_b)) or {}
    trust_a_to_b = _ga.get(agent_b) or _ga.get(_norm(agent_b).capitalize()) or _ga.get(_norm(agent_b)) or "none"
    trust_b_to_a = _gb.get(agent_a) or _gb.get(_norm(agent_a).capitalize()) or _gb.get(_norm(agent_a)) or "none"
    level = max(TRUST_LEVELS.get(trust_a_to_b, 0), TRUST_LEVELS.get(trust_b_to_a, 0))
    for name, val in TRUST_LEVELS.items():
        if val == level:
            return name
    return "none"


def set_trust(agent_a, agent_b, level):
    """Set trust level from agent_a to agent_b."""
    graph = load_trust_graph()
    if agent_a not in graph:
        graph[agent_a] = {}
    graph[agent_a][agent_b] = level
    save_trust_graph(graph)
    log.info(f"Trust set: {agent_a} → {agent_b} = {level}")
    return {"from": agent_a, "to": agent_b, "level": level}


def get_trust_graph():
    """Get the full trust graph."""
    return load_trust_graph()


def get_agent_trust_report(agent_name):
    """Get trust report for a specific agent."""
    graph = load_trust_graph()
    outgoing = graph.get(agent_name, {})
    incoming = {}
    for a, relations in graph.items():
        if agent_name in relations:
            incoming[a] = relations[agent_name]
    return {
        "agent": agent_name,
        "trusted_peers": [a for a, l in outgoing.items() if TRUST_LEVELS.get(l, 0) >= 2],
        "outgoing": outgoing,
        "incoming": incoming,
    }