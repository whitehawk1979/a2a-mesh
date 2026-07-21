"""A2A Mesh Smart Router — Capability-based, health-aware routing strategies.

Extends the base MeshRouter with intelligent agent selection:
- Round-robin: Equal distribution across agents
- Least-loaded: Route to the agent with the lowest current load
- Capability-based: Find agents with specific capabilities
- Health-weighted: Prefer healthier agents
- Random: Distribute randomly (baseline)

The SmartRouter integrates with AgentRegistry to make routing decisions
based on agent health scores, capabilities, and current load.
"""

import asyncio
import logging
import random
from typing import Dict, List, Optional, Sequence, Tuple
from ..core.registry import AgentRegistry, AgentCard, HealthRecord
from ..core.message import A2AMessage, SendResult

log = logging.getLogger("a2a_mesh.smart_router")


class RoutingStrategy:
    """Base class for routing strategies."""
    name: str = "base"

    def select(
        self,
        agents: List[Tuple[AgentCard, HealthRecord]],
        message: Optional[A2AMessage] = None,
        **kwargs,
    ) -> Optional[AgentCard]:
        """Select the best agent from the candidate list."""
        if not agents:
            return None
        return agents[0][0]


class RoundRobinStrategy(RoutingStrategy):
    """Route messages to agents in round-robin order."""

    name = "round_robin"
    _index = 0

    def select(
        self,
        agents: List[Tuple[AgentCard, HealthRecord]],
        message: Optional[A2AMessage] = None,
        **kwargs,
    ) -> Optional[AgentCard]:
        if not agents:
            return None
        # Sort by name for deterministic order
        sorted_agents = sorted(agents, key=lambda x: x[0].name)
        idx = self._index % len(sorted_agents)
        self._index += 1
        return sorted_agents[idx][0]


class LeastLoadedStrategy(RoutingStrategy):
    """Route to the agent with the lowest current load."""

    name = "least_loaded"

    def select(
        self,
        agents: List[Tuple[AgentCard, HealthRecord]],
        message: Optional[A2AMessage] = None,
        **kwargs,
    ) -> Optional[AgentCard]:
        if not agents:
            return None
        # Sort by current_load ascending, then by health_score descending
        sorted_agents = sorted(
            agents,
            key=lambda x: (x[1].current_load, -x[1].health_score),
        )
        return sorted_agents[0][0]


class CapabilityBasedStrategy(RoutingStrategy):
    """Route to agents that have specific capabilities."""

    name = "capability_based"

    def select(
        self,
        agents: List[Tuple[AgentCard, HealthRecord]],
        message: Optional[A2AMessage] = None,
        required_capabilities: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[AgentCard]:
        if not agents:
            return None
        if not required_capabilities:
            # No capability filter — fall back to health-weighted
            return HealthWeightedStrategy().select(agents, message)

        # Filter agents by capabilities
        matching = []
        for card, health in agents:
            if all(card.has_capability(cap) for cap in required_capabilities):
                matching.append((card, health))

        if not matching:
            log.warning(f"No agents found with capabilities: {required_capabilities}")
            return None

        # Among matching agents, pick the healthiest
        matching.sort(key=lambda x: -x[1].health_score)
        return matching[0][0]


class HealthWeightedStrategy(RoutingStrategy):
    """Route to agents weighted by health score (healthier = more likely)."""

    name = "health_weighted"

    def select(
        self,
        agents: List[Tuple[AgentCard, HealthRecord]],
        message: Optional[A2AMessage] = None,
        **kwargs,
    ) -> Optional[AgentCard]:
        if not agents:
            return None
        if len(agents) == 1:
            return agents[0][0]

        # Weighted random selection based on health score
        weights = [max(0.01, health.health_score) for _, health in agents]
        total = sum(weights)
        if total == 0:
            return random.choice(agents)[0]

        # Normalize weights
        probs = [w / total for w in weights]
        selected = random.choices(agents, weights=probs, k=1)[0]
        return selected[0]


class ExplainStrategy(RoutingStrategy):
    """Wrapper strategy that adds explainability to any routing decision.

    Returns the selected agent AND the reasoning behind the selection.
    """

    name = "explain"

    def __init__(self, inner_strategy: RoutingStrategy = None):
        self.inner = inner_strategy or HealthWeightedStrategy()

    def select(
        self,
        agents: List[Tuple[AgentCard, HealthRecord]],
        message: Optional[A2AMessage] = None,
        **kwargs,
    ) -> Tuple[Optional[AgentCard], str]:
        """Select agent and return (agent, explanation)."""
        if not agents:
            return None, "No agents available"

        selected = self.inner.select(agents, message, **kwargs)
        if not selected:
            return None, "Inner strategy returned no agent"

        # Build explanation
        health = next(
            (h for c, h in agents if c.name == selected.name), None
        )
        explanation = (
            f"Strategy: {self.inner.name} → "
            f"Selected: {selected.name} "
            f"(health={health.health_score:.2f}, "
            f"load={health.current_load}/{selected.max_concurrent}, "
            f"caps={selected.capabilities})"
        )
        return selected, explanation


# Strategy registry
STRATEGIES: Dict[str, type] = {
    "round_robin": RoundRobinStrategy,
    "least_loaded": LeastLoadedStrategy,
    "capability_based": CapabilityBasedStrategy,
    "health_weighted": HealthWeightedStrategy,
    "explain": ExplainStrategy,
}


class SmartRouter:
    """Intelligent agent routing using the AgentRegistry.

    Selects the best agent for a task based on:
    - Required capabilities
    - Agent health scores
    - Current load
    - Routing strategy

    Usage:
        registry = AgentRegistry()
        smart_router = SmartRouter(registry)

        # Register agents
        registry.register(AgentCard(name="morzsa", capabilities=["web_search"]))

        # Find best agent for a task
        agent = smart_router.route(required_capabilities=["web_search"])
        # Returns: AgentCard for 'morzsa' (or None)

        # Route with explanation
        agent, explanation = smart_router.route(
            required_capabilities=["web_search"],
            strategy="explain",
        )
        # Returns: (AgentCard, "Strategy: health_weighted → Selected: morzsa ...")
    """

    def __init__(self, registry: AgentRegistry, default_strategy: str = "health_weighted"):
        self.registry = registry
        self.default_strategy = default_strategy
        self._strategies: Dict[str, RoutingStrategy] = {
            name: cls() for name, cls in STRATEGIES.items()
        }

    def get_strategy(self, name: str) -> RoutingStrategy:
        """Get a strategy instance by name."""
        return self._strategies.get(name, self._strategies[self.default_strategy])

    def set_strategy(self, name: str, strategy: RoutingStrategy):
        """Register a custom strategy."""
        self._strategies[name] = strategy

    def route(
        self,
        required_capabilities: Optional[List[str]] = None,
        strategy: Optional[str] = None,
        exclude_agents: Optional[List[str]] = None,
        min_health_score: float = 0.3,
        message: Optional[A2AMessage] = None,
    ) -> Optional[AgentCard]:
        """Route a task to the best available agent.

        Args:
            required_capabilities: List of required capabilities (e.g., ["web_search", "summarization@v2"]).
            strategy: Routing strategy name (default: health_weighted).
            exclude_agents: List of agent names to exclude.
            min_health_score: Minimum health score threshold (default: 0.3).
            message: Optional message for context-aware routing.

        Returns:
            AgentCard of the selected agent, or None if no suitable agent found.
        """
        strategy_name = strategy or self.default_strategy
        strat = self.get_strategy(strategy_name)

        # Find agents by capability (if specified)
        if required_capabilities:
            agents = self.registry.find_by_capability(
                required_capabilities,
                healthy_only=True,
                min_health_score=min_health_score,
            )
        else:
            # No capability filter — use all healthy agents
            agents = self.registry.list_agents()
            agents = [
                (card, health)
                for card, health in agents
                if health.health_score >= min_health_score
            ]

        # Exclude specified agents
        if exclude_agents:
            agents = [
                (card, health)
                for card, health in agents
                if card.name not in exclude_agents
            ]

        if not agents:
            log.warning(
                f"No agents found for capabilities={required_capabilities} "
                f"strategy={strategy_name} min_health={min_health_score}"
            )
            return None

        # Apply strategy
        if strategy_name == "capability_based":
            result = strat.select(agents, message, required_capabilities=required_capabilities)
        elif strategy_name == "explain":
            result, explanation = strat.select(agents, message, required_capabilities=required_capabilities)
            log.info(f"Routing explanation: {explanation}")
        else:
            result = strat.select(agents, message)

        if result:
            log.info(
                f"Routed via {strategy_name} → {result.name} "
                f"(caps={result.capabilities})"
            )
        return result

    def route_with_explanation(
        self,
        required_capabilities: Optional[List[str]] = None,
        strategy: Optional[str] = None,
        exclude_agents: Optional[List[str]] = None,
        min_health_score: float = 0.3,
    ) -> Tuple[Optional[AgentCard], str]:
        """Route a task and return both the agent and the explanation."""
        strategy_name = strategy or self.default_strategy

        # Find agents
        if required_capabilities:
            agents = self.registry.find_by_capability(
                required_capabilities, healthy_only=True, min_health_score=min_health_score
            )
        else:
            agents = self.registry.list_agents()
            agents = [(c, h) for c, h in agents if h.health_score >= min_health_score]

        if exclude_agents:
            agents = [(c, h) for c, h in agents if c.name not in exclude_agents]

        if not agents:
            return None, f"No agents available (caps={required_capabilities}, strategy={strategy_name})"

        # Use explain strategy for transparency
        explain = ExplainStrategy(self.get_strategy(strategy_name))
        result, explanation = explain.select(agents, required_capabilities=required_capabilities)
        return result, explanation

    def get_all_routes(
        self,
        required_capabilities: Optional[List[str]] = None,
        min_health_score: float = 0.3,
    ) -> List[Dict]:
        """Get all possible routing options with their health scores.

        Returns a sorted list of dicts with agent info and scores,
        useful for dashboard visualization.
        """
        if required_capabilities:
            agents = self.registry.find_by_capability(
                required_capabilities, healthy_only=True, min_health_score=min_health_score
            )
        else:
            agents = self.registry.list_agents()
            agents = [(c, h) for c, h in agents if h.health_score >= min_health_score]

        result = []
        for card, health in agents:
            result.append({
                "name": card.name,
                "capabilities": card.capabilities,
                "endpoint": card.endpoint,
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "current_load": health.current_load,
                "max_concurrent": card.max_concurrent,
                "avg_latency_ms": round(health.avg_latency_ms, 1),
                "success_rate": round(health.success_rate, 3),
            })

        # Sort by health score descending, then load ascending
        result.sort(key=lambda x: (-x["health_score"], x["current_load"]))
        return result