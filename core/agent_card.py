"""A2A Mesh Agent Card — Capability discovery endpoint.

Implements the A2A v1.0 agent-card specification for advertising agent
capabilities, skills, and metadata. Other agents can discover what an agent
can do by fetching /.well-known/agent-card.json.

Inspired by gensyn-ai/axl's auto-discovery pattern where agents query the
MCP router at startup to build their agent card dynamically.
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

log = logging.getLogger("a2a_mesh.agent_card")


@dataclass
class AgentSkill:
    """A specific skill or capability the agent offers."""
    id: str
    name: str
    description: str = ""
    tags: List[str] = field(default_factory=list)
    input_schema: Optional[Dict] = None  # JSON Schema for input
    output_schema: Optional[Dict] = None  # JSON Schema for output
    
    def to_dict(self) -> dict:
        d = asdict(self)
        # Remove None values
        return {k: v for k, v in d.items() if v is not None}


@dataclass
class AgentCard:
    """Agent capability card following A2A v1.0 specification.
    
    Advertises what an agent can do, how to reach it, and what
    authentication it requires. This enables dynamic capability
    discovery across the mesh.
    """
    # Identity
    name: str = ""
    description: str = ""
    version: str = "0.1.0"
    url: str = ""  # Base URL for this agent (e.g., http://192.168.1.8:8650)
    
    # Capabilities
    skills: List[AgentSkill] = field(default_factory=list)
    capabilities: Dict[str, Any] = field(default_factory=dict)
    
    # Transport
    protocols: List[str] = field(default_factory=lambda: ["a2a-mesh/v0.8"])
    preferred_transport: str = "pg_notify"  # pg_notify, p2p, http
    transport_priority: List[str] = field(default_factory=lambda: ["pg_notify", "p2p", "http"])
    
    # Authentication
    authentication: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    node_id: str = ""  # Mesh node name
    mesh_address: str = ""  # P2P address
    uptime_seconds: float = 0
    last_seen: str = ""
    
    # Health
    health_score: float = 1.0  # 0.0 - 1.0
    load: float = 0.0  # Current load (0.0 - 1.0)
    message_queue_size: int = 0
    
    def to_dict(self) -> dict:
        """Serialize to dictionary, removing empty/null fields."""
        d = {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "url": self.url,
            "skills": [s.to_dict() for s in self.skills],
            "capabilities": self.capabilities,
            "protocols": self.protocols,
            "preferred_transport": self.preferred_transport,
            "transport_priority": self.transport_priority,
            "authentication": self.authentication if self.authentication else None,
            "node_id": self.node_id,
            "mesh_address": self.mesh_address,
            "health": {
                "score": self.health_score,
                "load": self.load,
                "message_queue_size": self.message_queue_size,
                "uptime_seconds": self.uptime_seconds,
                "last_seen": self.last_seen or datetime.now(timezone.utc).isoformat(),
            }
        }
        # Remove None values
        return {k: v for k, v in d.items() if v is not None}
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2, default=str)


def build_agent_card(node_name: str, registry: Any = None, 
                     health_score: float = 1.0, load: float = 0.0,
                     queue_size: int = 0, uptime: float = 0.0,
                     base_url: str = "", mesh_address: str = "",
                     config_skills: Optional[list] = None) -> AgentCard:
    """Build an agent card from the current node state.
    
    Args:
        node_name: The mesh node name
        registry: AgentRegistry instance (optional, for auto-discovery)
        health_score: Current health score (0-1)
        load: Current load (0-1)
        queue_size: Message queue size
        uptime: Uptime in seconds
        base_url: Base URL for this agent
        mesh_address: P2P mesh address
    
    Returns:
        AgentCard with auto-discovered capabilities
    """
    card = AgentCard(
        name=node_name,
        description=f"A2A Mesh Agent '{node_name}'",
        version="0.8.0",
        url=base_url,
        node_id=node_name,
        mesh_address=mesh_address,
        health_score=health_score,
        load=load,
        message_queue_size=queue_size,
        uptime_seconds=uptime,
        last_seen=datetime.now(timezone.utc).isoformat(),
    )
    
    # Auto-discover capabilities from registry
    if registry:
        try:
            # Get registered agents and their capabilities
            agents = registry.list_agents()
            agent_info = registry.get_agent(node_name)
            
            if agent_info:
                # Add capabilities from registry
                caps = agent_info.capabilities if hasattr(agent_info, 'capabilities') else []
                for cap in caps:
                    card.skills.append(AgentSkill(
                        id=f"cap_{cap}",
                        name=cap,
                        description=f"Capability: {cap}",
                        tags=[cap],
                    ))
                card.capabilities = {cap: True for cap in caps}
        except Exception as e:
            log.debug(f"Could not auto-discover capabilities from registry: {e}")
    
    # Add standard mesh capabilities
    card.capabilities.update({
        "a2a_messaging": True,
        "mesh_routing": True,
        "dedup_cache": True,
        "priority_queue": True,
        "multi_transport": True,
    })
    
    # Add config-defined skills first (from YAML config or MeshConfig.skills)
    existing_ids = set()
    if config_skills:
        for skill_data in config_skills:
            if isinstance(skill_data, dict):
                skill_id = skill_data.get("id", "")
                if skill_id and skill_id not in existing_ids:
                    card.skills.append(AgentSkill(
                        id=skill_id,
                        name=skill_data.get("name", skill_id),
                        description=skill_data.get("description", ""),
                        tags=skill_data.get("tags", []),
                    ))
                    existing_ids.add(skill_id)
    
    # Add standard mesh skills (skip if already provided via config_skills)
    standard_skills = [
        AgentSkill(
            id="mesh_send",
            name="Send Message",
            description="Send a message to another agent or broadcast to all",
            tags=["messaging", "send"],
            input_schema={"type": "object", "properties": {
                "recipient": {"type": "string", "description": "Target agent or 'broadcast'"},
                "content": {"type": "string"},
                "priority": {"type": "integer", "minimum": 1, "maximum": 10},
            }},
        ),
        AgentSkill(
            id="mesh_discover",
            name="Discover Agents",
            description="List all agents in the mesh and their capabilities",
            tags=["discovery", "agents"],
        ),
        AgentSkill(
            id="mesh_health",
            name="Health Check",
            description="Get health status and metrics of this agent",
            tags=["health", "monitoring"],
        ),
        AgentSkill(
            id="gdm",
            name="Group Decision Making",
            description="Coordinate multi-agent decisions with voting, consensus, and ranking protocols",
            tags=["gdm", "decision", "voting", "consensus", "coordination"],
        ),
        AgentSkill(
            id="task_execution",
            name="Task Execution",
            description="Execute delegated tasks and report results back to the coordinator",
            tags=["task", "execution", "delegation"],
        ),
    ]
    
    # Add only skills not already present (dedup by id)
    for skill in standard_skills:
        if skill.id not in existing_ids:
            card.skills.append(skill)
            existing_ids.add(skill.id)
    
    return card


# ─── Agent Card API Endpoint ─────────────────────────────────────

async def handle_agent_card_request(request_data: dict, node_name: str, 
                                      registry: Any = None, 
                                      health_score: float = 1.0,
                                      load: float = 0.0,
                                      queue_size: int = 0,
                                      uptime: float = 0.0,
                                      base_url: str = "",
                                      mesh_address: str = "") -> dict:
    """Handle an agent-card.json request.
    
    Can be called from the dashboard API handler or directly
    as a well-known endpoint.
    """
    card = build_agent_card(
        node_name=node_name,
        registry=registry,
        health_score=health_score,
        load=load,
        queue_size=queue_size,
        uptime=uptime,
        base_url=base_url,
        mesh_address=mesh_address,
    )
    return card.to_dict()