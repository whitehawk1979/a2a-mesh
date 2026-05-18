"""Smart task router for a2a-mesh.

Routes incoming tasks to the best available agent based on capability
matching, load balancing, cost optimization, and custom routing policies.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TypeAlias

from a2a_mesh._logging import get_logger
from a2a_mesh.exceptions import NoCapableAgentError, QueueFullError
from a2a_mesh.models import (
    RegisteredAgent,
    RoutingPolicy,
    RoutingStrategy,
    Task,
)
from a2a_mesh.registry import AgentRegistry

logger = get_logger(__name__)

RoutingHookResult: TypeAlias = RegisteredAgent | Sequence[RegisteredAgent] | None
RoutingHook: TypeAlias = Callable[
    [Sequence[RegisteredAgent], Task, RoutingPolicy],
    RoutingHookResult,
]


@dataclass(frozen=True)
class RouteCandidate:
    """Explainable routing candidate returned by :meth:`Router.explain_route`.

    Attributes:
        agent_name: Human-readable agent name.
        rank: 1-based ranking after applying the active strategy.
        available: Whether the agent is within queue/capacity limits.
        strategy_value: Raw metric used by the current strategy, if any.
        reasons: Human-readable explanation snippets for the ranking.
    """

    agent_name: str
    rank: int
    available: bool
    strategy_value: float | None = None
    reasons: list[str] = field(default_factory=list)


def _capability_base(capability: str) -> str:
    """Return the unversioned capability name used for explanations."""
    return capability.split("@", 1)[0]


class Router:
    """Routes tasks to agents based on configurable policies.

    The router evaluates registered agents against the task's requirements
    and the active routing policy to select the optimal target.

    Attributes:
        registry: The agent registry to query.
        policy: The active routing policy.
    """

    def __init__(
        self,
        registry: AgentRegistry,
        policy: RoutingPolicy | None = None,
        strategy_hook: RoutingHook | None = None,
    ) -> None:
        """Initialize the router.

        Args:
            registry: The agent registry for discovery.
            policy: Routing policy configuration. Defaults to round-robin.
            strategy_hook: Optional user-supplied routing hook that can pick
                or reorder candidates before the built-in strategies run.
        """
        self.registry = registry
        self.policy = policy or RoutingPolicy()
        self.strategy_hook = strategy_hook
        self._round_robin_index: int = 0

    def route(self, task: Task) -> RegisteredAgent:
        """Select the best agent for a task.

        Evaluates all registered agents against the task's requirements
        and the routing policy, then returns the optimal match.

        Args:
            task: The task to route.

        Returns:
            The selected agent.

        Raises:
            NoCapableAgentError: If no agent can handle the task.
            QueueFullError: If all capable agents are at capacity.
        """
        candidates = self._find_candidates(task)

        if not candidates:
            caps = task.required_capabilities or [task.agent]
            raise NoCapableAgentError(caps)

        # Filter out agents that are at capacity
        available = [a for a in candidates if self._within_capacity(a)]

        if not available:
            first = candidates[0]
            raise QueueFullError(first.card.name, first.current_load)

        selected = self._apply_strategy(available, task)

        logger.info(
            "task.routed",
            task_id=task.task_id,
            agent=selected.card.name,
            strategy=self.policy.strategy.value,
            load=selected.current_load,
        )
        return selected

    def route_multi(
        self,
        task: Task,
        count: int,
    ) -> list[RegisteredAgent]:
        """Select multiple agents for fan-out execution.

        Args:
            task: The task to route.
            count: Number of agents to select.

        Returns:
            List of selected agents (up to count).

        Raises:
            NoCapableAgentError: If no agent can handle the task.
        """
        candidates = self._find_candidates(task)
        if not candidates:
            caps = task.required_capabilities or [task.agent]
            raise NoCapableAgentError(caps)

        available = [a for a in candidates if self._within_capacity(a)]

        if not available:
            available = candidates

        # Return up to `count` agents, prioritised by strategy
        selected = self._sort_by_strategy(available, task)[:count]
        logger.info(
            "task.routed_multi",
            task_id=task.task_id,
            count=len(selected),
            agents=[a.card.name for a in selected],
        )
        return selected

    def explain_route(
        self,
        task: Task,
        count: int | None = None,
    ) -> list[RouteCandidate]:
        """Explain how the router would rank candidates for *task*.

        This is useful for debugging routing policy choices, building
        dashboards, and validating custom hooks before dispatching work.

        Args:
            task: Task to evaluate.
            count: Optional maximum number of ranked candidates to return.

        Returns:
            Ranked explainability records. Agents at capacity are included
            but marked unavailable so callers can diagnose queue pressure.

        Raises:
            NoCapableAgentError: If no registered agent can satisfy the task.
        """
        candidates = self._find_candidates(task)
        if not candidates:
            caps = task.required_capabilities or [task.agent]
            raise NoCapableAgentError(caps)

        ranked = self._sort_by_strategy(candidates, task)
        if count is not None:
            ranked = ranked[:count]

        return [
            RouteCandidate(
                agent_name=agent.card.name,
                rank=index,
                available=self._within_capacity(agent),
                strategy_value=self._strategy_value(agent),
                reasons=self._build_route_reasons(agent, task),
            )
            for index, agent in enumerate(ranked, start=1)
        ]

    def _find_candidates(self, task: Task) -> list[RegisteredAgent]:
        """Find agents capable of handling the task.

        If the task specifies an agent name, look it up directly.
        Otherwise, search by required capabilities.
        """
        if task.agent:
            try:
                agent = self.registry.get(task.agent)
                return [agent]
            except Exception:
                return []

        if task.required_capabilities:
            return self.registry.find_by_capability(
                task.required_capabilities,
                healthy_only=True,
            )

        # No constraints: return all healthy agents
        return self.registry.find_by_capability([], healthy_only=True)

    def _apply_strategy(
        self,
        agents: Sequence[RegisteredAgent],
        task: Task,
    ) -> RegisteredAgent:
        """Apply the routing strategy to select one agent."""
        custom = self._apply_custom_hook(agents, task)
        if isinstance(custom, list):
            return custom[0]
        if custom is not None:
            return custom

        strategy = self.policy.strategy

        if strategy == RoutingStrategy.ROUND_ROBIN:
            idx = self._round_robin_index % len(agents)
            self._round_robin_index += 1
            return agents[idx]

        if strategy == RoutingStrategy.LEAST_LOAD:
            return min(agents, key=lambda a: a.current_load)

        if strategy == RoutingStrategy.LEAST_COST:
            return min(agents, key=lambda a: a.card.cost_per_task)

        if strategy == RoutingStrategy.LEAST_LATENCY:
            return min(agents, key=lambda a: a.avg_latency_ms)

        if strategy == RoutingStrategy.RANDOM:
            return random.choice(list(agents))

        if strategy == RoutingStrategy.HEALTH_SCORE:
            return max(agents, key=lambda a: a.health_score)

        # Default fallback
        return agents[0]

    def _sort_by_strategy(
        self,
        agents: Sequence[RegisteredAgent],
        task: Task,
    ) -> list[RegisteredAgent]:
        """Sort agents according to the active strategy."""
        custom = self._apply_custom_hook(agents, task)
        if isinstance(custom, list):
            return custom
        if custom is not None:
            return [custom]

        strategy = self.policy.strategy

        if strategy == RoutingStrategy.LEAST_LOAD:
            return sorted(agents, key=lambda a: a.current_load)

        if strategy == RoutingStrategy.LEAST_COST:
            return sorted(agents, key=lambda a: a.card.cost_per_task)

        if strategy == RoutingStrategy.LEAST_LATENCY:
            return sorted(agents, key=lambda a: a.avg_latency_ms)

        if strategy == RoutingStrategy.HEALTH_SCORE:
            return sorted(agents, key=lambda a: a.health_score, reverse=True)

        return list(agents)

    def _apply_custom_hook(
        self,
        agents: Sequence[RegisteredAgent],
        task: Task,
    ) -> RegisteredAgent | list[RegisteredAgent] | None:
        """Apply a user-supplied routing hook if one is configured."""
        if self.strategy_hook is None:
            return None

        result = self.strategy_hook(agents, task, self.policy)
        if result is None:
            return None

        if isinstance(result, RegisteredAgent):
            return result if result in agents else None

        selected = [agent for agent in result if agent in agents]
        if not selected:
            return None
        return selected

    def _within_capacity(self, agent: RegisteredAgent) -> bool:
        """Check whether an agent can accept another task."""
        capacity = min(self.policy.max_queue_depth, agent.card.max_concurrent)
        return agent.current_load < max(0, capacity)

    def _strategy_value(self, agent: RegisteredAgent) -> float | None:
        """Return the raw metric used by the active routing strategy."""
        strategy = self.policy.strategy
        if strategy == RoutingStrategy.LEAST_LOAD:
            return float(agent.current_load)
        if strategy == RoutingStrategy.LEAST_COST:
            return float(agent.card.cost_per_task)
        if strategy == RoutingStrategy.LEAST_LATENCY:
            return float(agent.avg_latency_ms)
        if strategy == RoutingStrategy.HEALTH_SCORE:
            return float(agent.health_score)
        return None

    def _build_route_reasons(self, agent: RegisteredAgent, task: Task) -> list[str]:
        """Build human-readable explanation snippets for a ranked agent."""
        reasons = [
            f"status={agent.status.value}",
            f"load={agent.current_load}/{min(self.policy.max_queue_depth, agent.card.max_concurrent)}",
        ]
        if task.required_capabilities:
            required_bases = {
                _capability_base(cap) for cap in task.required_capabilities
            }
            matched = sorted(
                cap
                for cap in agent.card.capabilities
                if _capability_base(cap) in required_bases
            )
            if matched:
                reasons.append(f"matches capabilities: {', '.join(matched)}")

        strategy = self.policy.strategy
        if strategy == RoutingStrategy.LEAST_LOAD:
            reasons.append(f"least_load={agent.current_load}")
        elif strategy == RoutingStrategy.LEAST_COST:
            reasons.append(f"cost_per_task=${agent.card.cost_per_task:.4f}")
        elif strategy == RoutingStrategy.LEAST_LATENCY:
            reasons.append(f"avg_latency_ms={agent.avg_latency_ms:.1f}")
        elif strategy == RoutingStrategy.HEALTH_SCORE:
            reasons.append(f"health_score={agent.health_score:.3f}")
        elif strategy == RoutingStrategy.ROUND_ROBIN:
            reasons.append("round_robin ordering")
        elif strategy == RoutingStrategy.RANDOM:
            reasons.append("random candidate set")

        if self.strategy_hook is not None:
            reasons.append("custom strategy hook active")

        return reasons
