"""A2A Mesh Agent Registry — Capability-based agent discovery and health monitoring.

Inspired by sushaan-k/a2a-mesh, adapted for our P2P mesh architecture.

Features:
- Agent Card registration (name, capabilities, version, endpoint)
- Capability-based discovery (with version support: "summarization@v2")
- Composite health scoring (latency + success_rate + load)
- Background health monitoring loop
- Integration with existing PeerDiscovery and LocalStore

Health Score Formula:
  score = 0.4 * latency_score + 0.3 * success_rate + 0.3 * availability_score
  - latency_score: 1.0 if < threshold, sigmoid decay above threshold
  - success_rate: successes / total_requests
  - availability_score: uptime_pct based on health checks
"""

import asyncio
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple
from collections import defaultdict

log = logging.getLogger("a2a_mesh.registry")


# ─── Data Models ─────────────────────────────────────────────────────────

@dataclass
class AgentCapability:
    """A capability offered by an agent, with optional version."""
    name: str
    version: Optional[str] = None
    description: str = ""

    @classmethod
    def from_string(cls, cap_str: str) -> 'AgentCapability':
        """Parse 'name@version' or 'name' format."""
        if "@" in cap_str:
            name, version = cap_str.split("@", 1)
            return cls(name=name.strip(), version=version.strip())
        return cls(name=cap_str.strip())

    def __str__(self):
        if self.version:
            return f"{self.name}@{self.version}"
        return self.name

    def matches(self, required: 'AgentCapability') -> bool:
        """Check if this capability satisfies a requirement."""
        if self.name != required.name:
            return False
        # If required has a version, we must match exactly
        if required.version is not None:
            return self.version == required.version
        # If required has no version, any version (or unversioned) matches
        return True


@dataclass
class AgentCard:
    """Agent capability card — describes what an agent can do."""
    name: str
    capabilities: List[str] = field(default_factory=list)  # ["web_search", "summarization@v2"]
    skills: List[Dict] = field(default_factory=list)  # [{"id": "gdm", "name": "GDM", "tags": [...]}]
    version: str = "1.0.0"
    description: str = ""
    endpoint: str = ""       # e.g., "http://192.168.1.30:8651"
    health_endpoint: str = "/health"
    max_concurrent: int = 10
    cost_per_task: float = 0.0
    metadata: Dict = field(default_factory=dict)

    def has_capability(self, required: str) -> bool:
        """Check if this agent has a required capability."""
        req = AgentCapability.from_string(required)
        for cap_str in self.capabilities:
            cap = AgentCapability.from_string(cap_str)
            if cap.matches(req):
                return True
        return False


@dataclass
class HealthRecord:
    """Health tracking data for an agent."""
    health_score: float = 1.0         # 0.0 (dead) to 1.0 (perfect)
    total_requests: int = 0
    total_failures: int = 0
    current_load: int = 0
    avg_latency_ms: float = 0.0
    last_health_check: float = 0.0
    last_success: float = 0.0
    last_failure: float = 0.0
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    uptime_pct: float = 100.0         # Percentage of health checks passed
    _health_checks_total: int = 0
    _health_checks_passed: int = 0

    @property
    def success_rate(self) -> float:
        """Success rate (0.0 to 1.0)."""
        if self.total_requests == 0:
            return 1.0  # Assume healthy if no data
        return 1.0 - (self.total_failures / self.total_requests)

    @property
    def status(self) -> str:
        """Human-readable status based on health score."""
        if self.health_score >= 0.8:
            return "healthy"
        elif self.health_score >= 0.5:
            return "degraded"
        elif self.health_score > 0.0:
            return "unhealthy"
        return "dead"


# ─── Health Scorer ────────────────────────────────────────────────────────

# Default weights for composite score
DEFAULT_WEIGHTS = {
    "latency": 0.4,
    "success_rate": 0.3,
    "availability": 0.3,
}

# Decay/recovery factors (from sushaan approach)
DECAY_FACTOR = 0.15       # Score penalty per failure
RECOVERY_FACTOR = 0.05    # Score recovery per success
LATENCY_THRESHOLD_MS = 5000.0  # Latency above this is penalized


class HealthScorer:
    """Computes composite health scores for mesh agents.

    The score combines three dimensions:
    1. Latency score (sigmoid decay above threshold)
    2. Success rate (successes / total)
    3. Availability score (health check pass rate)

    Weighted formula:
      composite = w1*latency_score + w2*success_rate + w3*availability_score

    Failures cause fast degradation; successes cause slow recovery.
    """

    def __init__(
        self,
        decay_factor: float = DECAY_FACTOR,
        recovery_factor: float = RECOVERY_FACTOR,
        latency_threshold_ms: float = LATENCY_THRESHOLD_MS,
        weights: Optional[Dict[str, float]] = None,
    ):
        self.decay_factor = decay_factor
        self.recovery_factor = recovery_factor
        self.latency_threshold_ms = latency_threshold_ms
        self.weights = weights or DEFAULT_WEIGHTS

    def record_success(
        self, health: HealthRecord, latency_ms: float = 0.0
    ) -> float:
        """Record a successful request and update the health score."""
        health.total_requests += 1
        health.consecutive_successes += 1
        health.consecutive_failures = 0
        health.last_success = time.time()

        # Update latency (exponential moving average)
        if health.avg_latency_ms == 0:
            health.avg_latency_ms = latency_ms
        else:
            alpha = 0.3  # Smoothing factor
            health.avg_latency_ms = (
                alpha * latency_ms + (1 - alpha) * health.avg_latency_ms
            )

        # Compute composite score
        return self.compute_score(health)

    def record_failure(self, health: HealthRecord) -> float:
        """Record a failed request and degrade the health score."""
        health.total_requests += 1
        health.total_failures += 1
        health.consecutive_failures += 1
        health.consecutive_successes = 0
        health.last_failure = time.time()

        return self.compute_score(health)

    def record_health_check(self, health: HealthRecord, passed: bool) -> float:
        """Record a health check result."""
        health._health_checks_total += 1
        if passed:
            health._health_checks_passed += 1
        health.uptime_pct = (
            (health._health_checks_passed / health._health_checks_total) * 100
            if health._health_checks_total > 0 else 100.0
        )
        health.last_health_check = time.time()
        return self.compute_score(health)

    def compute_score(self, health: HealthRecord) -> float:
        """Compute composite health score (0.0 to 1.0)."""
        # Latency score: sigmoid decay above threshold
        latency_score = self._latency_score(health.avg_latency_ms)

        # Success rate
        success_rate = health.success_rate

        # Availability score (from health checks)
        # If no health checks have been performed, assume healthy (1.0)
        if health._health_checks_total == 0:
            availability_score = 1.0
        else:
            availability_score = health.uptime_pct / 100.0

        # Treat 0% uptime with 0 requests as "newly registered — healthy"
        # (avoids degrading fresh P2P peers that haven't been health-checked yet)
        if availability_score == 0.0 and health.total_requests == 0 and health.total_failures == 0:
            availability_score = 1.0

        # Weighted composite
        w = self.weights
        composite = (
            w["latency"] * latency_score
            + w["success_rate"] * success_rate
            + w["availability"] * availability_score
        )

        # Apply decay/recovery based on recent outcomes
        if health.consecutive_failures >= 3:
            composite *= 0.5  # Heavy penalty for 3+ consecutive failures
        elif health.consecutive_failures > 0:
            composite -= self.decay_factor * health.consecutive_failures
        elif health.consecutive_successes >= 3:
            composite = min(1.0, composite + self.recovery_factor)

        health.health_score = max(0.0, min(1.0, composite))
        return health.health_score

    def _latency_score(self, latency_ms: float) -> float:
        """Convert latency to a 0-1 score using sigmoid decay."""
        if latency_ms <= 0:
            return 1.0
        if latency_ms <= self.latency_threshold_ms:
            return 1.0
        # Sigmoid decay above threshold
        overshoot = latency_ms / self.latency_threshold_ms
        return 1.0 / (1.0 + math.exp(2.0 * (overshoot - 2.0)))


# ─── Agent Registry ────────────────────────────────────────────────────────

class AgentRegistry:
    """In-memory agent registry with capability-based discovery and health scoring.

    Features:
    - Agent registration with auto-approval or pending approval
    - Capability-based discovery (with version support)
    - Composite health scoring
    - P2P auto-discovery integration (new agents require approval)

    Usage:
        registry = AgentRegistry(health_scorer=HealthScorer())

        # Register agents
        card = AgentCard(name=\"morzsa\", capabilities=[\"code_review\", \"web_search@v2\"])
        registry.register(card)

        # Auto-discovered agents need approval
        registry.request_registration(discovered_card)  # → pending
        registry.approve_agent(\"new_agent\")             # → registered

        # Find agents by capability
        agents = registry.find_by_capability([\"web_search\"])
    """

    def __init__(self, health_scorer: Optional[HealthScorer] = None, auto_approve: bool = False):
        self.agents: Dict[str, AgentCard] = {}
        self.health_records: Dict[str, HealthRecord] = {}
        self.health_scorer = health_scorer or HealthScorer()
        self._health_task: Optional[asyncio.Task] = None
        self._running = False
        self._health_interval = 30.0
        self._peer_discovery = None

        # Pending agent approval system
        self.auto_approve = auto_approve  # If True, auto-approve new agents
        self.pending_agents: Dict[str, AgentCard] = {}  # name → card (awaiting approval)
        self._approval_callbacks: List = []  # Callbacks on approval

    def register(self, card: AgentCard, force: bool = False) -> HealthRecord:
        """Register an agent with the mesh.

        Args:
            card: Agent capability card.
            force: If True, overwrite existing registration.

        Returns:
            The agent's health record.
        """
        if card.name in self.agents and not force:
            log.warning(f"Agent {card.name} already registered, use force=True to overwrite")
            return self.health_records[card.name]

        self.agents[card.name] = card
        if card.name not in self.health_records:
            self.health_records[card.name] = HealthRecord()
        else:
            # Reset health on re-registration
            self.health_records[card.name].last_health_check = time.time()

        log.info(
            f"Agent registered: {card.name} "
            f"capabilities={card.capabilities} version={card.version}"
        )
        return self.health_records[card.name]

    def deregister(self, agent_name: str) -> None:
        """Remove an agent from the registry."""
        if agent_name in self.agents:
            del self.agents[agent_name]
        if agent_name in self.health_records:
            del self.health_records[agent_name]
        log.info(f"Agent deregistered: {agent_name}")

    def get(self, agent_name: str) -> Optional[AgentCard]:
        """Get an agent's card by name."""
        return self.agents.get(agent_name)

    def get_health(self, agent_name: str) -> Optional[HealthRecord]:
        """Get an agent's health record."""
        return self.health_records.get(agent_name)

    def find_by_capability(
        self,
        capabilities: Sequence[str],
        healthy_only: bool = True,
        min_health_score: float = 0.3,
    ) -> List[Tuple[AgentCard, HealthRecord]]:
        """Find agents that support ALL given capabilities.

        Capabilities can include version: "summarization@v2"
        When version specified, agent must have that exact version.
        When no version, any version matches.

        Args:
            capabilities: Required capability tags.
            healthy_only: If True, exclude unhealthy agents.
            min_health_score: Minimum health score threshold.

        Returns:
            List of (AgentCard, HealthRecord) tuples, sorted by health score.
        """
        matches = []
        for name, card in self.agents.items():
            health = self.health_records.get(name, HealthRecord())

            # Check health filter
            if healthy_only and health.health_score < min_health_score:
                continue

            # Check all required capabilities
            all_match = True
            for req_cap in capabilities:
                if not card.has_capability(req_cap):
                    all_match = False
                    break

            if all_match:
                matches.append((card, health))

        # Sort by health score (best first), then by load (least loaded first)
        matches.sort(key=lambda x: (-x[1].health_score, x[1].current_load))
        return matches

    def list_agents(self) -> List[Tuple[AgentCard, HealthRecord]]:
        """List all registered agents with their health."""
        result = []
        for name, card in self.agents.items():
            health = self.health_records.get(name, HealthRecord())
            result.append((card, health))
        return result

    def record_success(self, agent_name: str, latency_ms: float = 0.0) -> float:
        """Record a successful interaction with an agent."""
        if agent_name not in self.health_records:
            self.health_records[agent_name] = HealthRecord()
        return self.health_scorer.record_success(
            self.health_records[agent_name], latency_ms
        )

    def record_failure(self, agent_name: str) -> float:
        """Record a failed interaction with an agent."""
        if agent_name not in self.health_records:
            self.health_records[agent_name] = HealthRecord()
        return self.health_scorer.record_failure(
            self.health_records[agent_name]
        )

    def set_peer_discovery(self, peer_discovery):
        """Link to PeerDiscovery for health checking."""
        self._peer_discovery = peer_discovery

    async def check_agent_health(self, agent_name: str) -> str:
        """Check health of a single agent via PeerDiscovery."""
        if agent_name not in self.agents:
            return "unknown"

        card = self.agents[agent_name]
        health = self.health_records.get(agent_name, HealthRecord())

        # Try PeerDiscovery health check
        if self._peer_discovery:
            peer = self._peer_discovery.get_peer(agent_name)
            if peer:
                # P2P-connected peer: check if the connection is alive
                p2p_available = getattr(peer, 'p2p_available', False)
                if p2p_available:
                    # P2P connection is alive → peer is healthy
                    self.health_scorer.record_health_check(health, True)
                    return health.status
                # P2P not available — try HTTP health endpoint
                is_healthy = await self._peer_discovery.check_peer_health(peer)
                self.health_scorer.record_health_check(health, is_healthy)
                return health.status
            else:
                # No peer info available — check via endpoint
                if card.endpoint:
                    try:
                        import aiohttp
                        url = f"{card.endpoint.rstrip('/')}{card.health_endpoint}"
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                url, timeout=aiohttp.ClientTimeout(total=5)
                            ) as resp:
                                is_healthy = resp.status == 200
                                self.health_scorer.record_health_check(health, is_healthy)
                                return health.status
                    except Exception:
                        self.health_scorer.record_health_check(health, False)
                        return health.status

        self.health_scorer.record_health_check(health, False)
        return health.status

    async def start_health_monitoring(self, interval: float = 30.0):
        """Start background health monitoring loop."""
        self._health_interval = interval
        self._running = True
        self._health_task = asyncio.create_task(self._health_loop())
        log.info(f"Health monitoring started (interval: {interval}s)")

    async def stop_health_monitoring(self):
        """Stop background health monitoring."""
        self._running = False
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        log.info("Health monitoring stopped")

    async def _health_loop(self):
        """Periodic health check loop."""
        while self._running:
            try:
                for name in list(self.agents.keys()):
                    try:
                        await self.check_agent_health(name)
                    except Exception as e:
                        log.warning(f"Health check failed for {name}: {e}")
            except Exception as e:
                log.error(f"Health loop error: {e}")

            await asyncio.sleep(self._health_interval)

    def get_stats(self) -> Dict:
        """Return registry statistics."""
        agents_data = {}
        for name, card in self.agents.items():
            health = self.health_records.get(name, HealthRecord())
            agents_data[name] = {
                "name": name,
                "capabilities": card.capabilities,
                "skills": card.skills if hasattr(card, 'skills') else [],
                "version": card.version,
                "endpoint": card.endpoint,
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "success_rate": round(health.success_rate, 3),
                "avg_latency_ms": round(health.avg_latency_ms, 1),
                "current_load": health.current_load,
                "uptime_pct": round(health.uptime_pct, 1),
                "total_requests": health.total_requests,
                "total_failures": health.total_failures,
            }

        return {
            "total_agents": len(self.agents),
            "healthy_agents": sum(
                1 for h in self.health_records.values()
                if h.health_score >= 0.8
            ),
            "degraded_agents": sum(
                1 for h in self.health_records.values()
                if 0.5 <= h.health_score < 0.8
            ),
            "unhealthy_agents": sum(
                1 for h in self.health_records.values()
                if h.health_score < 0.5
            ),
            "agents": agents_data,
        }

    # ─── Pending Agent Approval ──────────────────────────────────────

    def request_registration(self, card: AgentCard) -> str:
        """Request registration for a new agent (P2P auto-discovery).

        If auto_approve is True, the agent is registered immediately.
        Otherwise, it goes into pending state awaiting admin approval.

        Returns:
            "approved" if auto-approved, "pending" if awaiting approval.
        """
        if card.name in self.agents:
            log.info(f"Agent {card.name} already registered, updating (card.version={card.version}, existing.version={self.agents[card.name].version})")
            # Merge capabilities: keep existing if new ones are fewer (PG has richer data)
            existing = self.agents[card.name]
            # Preserve existing skills if new card has none (P2P handshake doesn't include skills)
            existing_skills = getattr(existing, 'skills', None) or []
            new_skills = getattr(card, 'skills', None) or []
            # Extract skill IDs, handling both Skill objects, strings, and dicts (unhashable)
            def _skill_id(s):
                if hasattr(s, 'id'):
                    return s.id
                if isinstance(s, dict):
                    return s.get('id', str(s))
                if isinstance(s, (str, int, float, tuple)):
                    return s
                return str(s)
            _existing = [_skill_id(s) for s in (existing_skills if existing_skills else []) if isinstance(_skill_id(s), (str, int, float, tuple))]
            _new = [_skill_id(s) for s in (new_skills if new_skills else []) if isinstance(_skill_id(s), (str, int, float, tuple))]
            final_skills = list(set(_existing + _new)) if (_existing or _new) else []
            # Rebuild skill objects from IDs if we only have IDs
            if final_skills and all(isinstance(s, str) for s in final_skills):
                # Map IDs back to original skill objects from existing
                skill_map = {}
                for s in (existing_skills or []):
                    if hasattr(s, 'id'):
                        sid = s.id
                    elif isinstance(s, dict):
                        sid = s.get('id', str(s))
                    elif isinstance(s, (str, int, float, tuple)):
                        sid = s
                    else:
                        sid = str(s)
                    # Only use hashable keys
                    if isinstance(sid, (str, int, float, tuple)):
                        skill_map[sid] = s if hasattr(s, 'id') else s
                for s in (new_skills or []):
                    if hasattr(s, 'id'):
                        sid = s.id
                    elif isinstance(s, dict):
                        sid = s.get('id', str(s))
                    elif isinstance(s, (str, int, float, tuple)):
                        sid = s
                    else:
                        sid = str(s)
                    # Only use hashable keys
                    if isinstance(sid, (str, int, float, tuple)):
                        skill_map[sid] = s if hasattr(s, 'id') else s
                final_skills_list = list(skill_map.values())
            else:
                final_skills_list = existing_skills or new_skills or []

            # Merge capabilities — filter out unhashable types (dicts) to prevent TypeError
            existing_caps_set = set(c for c in existing.capabilities if isinstance(c, (str, int, float, tuple)))
            new_caps_set = set(c for c in card.capabilities if isinstance(c, (str, int, float, tuple)))

            if len(card.capabilities) <= 1 and len(existing.capabilities) > 1:
                # New registration has fewer caps (e.g. P2P handshake only sends a2a_messaging)
                # Keep existing richer capabilities
                merged_caps = list(existing_caps_set | new_caps_set)
                # Use the better version (prefer non-default)
                better_version = card.version if card.version and card.version != "1.0.0" else (existing.version if existing.version and existing.version != "1.0.0" else card.version)
                card = AgentCard(
                    name=card.name,
                    capabilities=merged_caps,
                    endpoint=card.endpoint or existing.endpoint,
                    description=card.description or existing.description,
                    version=better_version,
                    skills=final_skills_list if final_skills_list else None,
                )
            else:
                # Even if caps are fine, preserve existing skills if new card has none
                if not new_skills and existing_skills:
                    # Use the better version (prefer non-default)
                    better_version = card.version if card.version and card.version != "1.0.0" else (existing.version if existing.version and existing.version != "1.0.0" else card.version)
                    card = AgentCard(
                        name=card.name,
                        capabilities=list(existing_caps_set | new_caps_set) if card.capabilities else existing.capabilities,
                        endpoint=card.endpoint or existing.endpoint,
                        description=card.description or existing.description,
                        version=better_version,
                        skills=final_skills_list if final_skills_list else None,
                    )
            self.agents[card.name] = card
            return "approved"

        if self.auto_approve:
            self.register(card, force=True)
            log.info(f"Auto-approved agent: {card.name}")
            self._fire_approval_callbacks(card, "approved")
            return "approved"

        # Add to pending
        self.pending_agents[card.name] = card
        log.info(
            f"Agent {card.name} pending approval "
            f"(capabilities={card.capabilities}, endpoint={card.endpoint})"
        )
        return "pending"

    def approve_agent(self, agent_name: str) -> Optional[AgentCard]:
        """Approve a pending agent registration.

        Moves agent from pending to registered state.
        Returns the AgentCard if approved, None if not found in pending.
        """
        if agent_name not in self.pending_agents:
            log.warning(f"Agent {agent_name} not in pending list")
            return None

        card = self.pending_agents.pop(agent_name)
        self.register(card, force=True)
        log.info(f"Agent {agent_name} approved and registered")
        self._fire_approval_callbacks(card, "approved")
        return card

    def reject_agent(self, agent_name: str) -> bool:
        """Reject a pending agent registration.

        Returns True if the agent was in pending and removed.
        """
        if agent_name in self.pending_agents:
            del self.pending_agents[agent_name]
            log.info(f"Agent {agent_name} registration rejected")
            self._fire_approval_callbacks(
                AgentCard(name=agent_name), "rejected"
            )
            return True
        return False

    def list_pending(self) -> List[Tuple[AgentCard, str]]:
        """List all pending agent registrations.

        Returns:
            List of (AgentCard, status) tuples for pending agents.
        """
        return [(card, "pending") for card in self.pending_agents.values()]

    def on_approval(self, callback):
        """Register a callback for agent approval events.

        Callback signature: callback(card: AgentCard, action: str)
        action is "approved" or "rejected".
        """
        self._approval_callbacks.append(callback)

    def _fire_approval_callbacks(self, card: AgentCard, action: str):
        """Fire all registered approval callbacks."""
        for cb in self._approval_callbacks:
            try:
                cb(card, action)
            except Exception as e:
                log.error(f"Approval callback error: {e}")