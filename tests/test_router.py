"""Tests for the smart task router."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Sequence

import pytest

from a2a_mesh.exceptions import NoCapableAgentError, QueueFullError
from a2a_mesh.models import (
    AgentCard,
    AgentStatus,
    RegisteredAgent,
    RoutingPolicy,
    RoutingStrategy,
    Task,
)
from a2a_mesh.registry import AgentRegistry
from a2a_mesh.router import Router


class TestRouterRouting:
    """Tests for single-agent routing."""

    def test_route_by_agent_name(self, populated_registry: AgentRegistry) -> None:
        router = Router(populated_registry)
        task = Task(name="t", agent="research-agent")
        selected = router.route(task)
        assert selected.card.name == "research-agent"

    def test_route_by_capability(self, populated_registry: AgentRegistry) -> None:
        router = Router(populated_registry)
        task = Task(name="t", required_capabilities=["web_search"])
        selected = router.route(task)
        assert selected.card.name == "research-agent"

    def test_route_no_capable_agent_raises(
        self, populated_registry: AgentRegistry
    ) -> None:
        router = Router(populated_registry)
        task = Task(name="t", required_capabilities=["quantum_computing"])
        with pytest.raises(NoCapableAgentError):
            router.route(task)

    def test_route_unknown_agent_raises(
        self, populated_registry: AgentRegistry
    ) -> None:
        router = Router(populated_registry)
        task = Task(name="t", agent="ghost-agent")
        with pytest.raises(NoCapableAgentError):
            router.route(task)

    def test_route_queue_full_raises(self, populated_registry: AgentRegistry) -> None:
        policy = RoutingPolicy(max_queue_depth=1)
        router = Router(populated_registry, policy=policy)
        populated_registry.agents["research-agent"].current_load = 5
        task = Task(name="t", required_capabilities=["web_search"])
        with pytest.raises(QueueFullError):
            router.route(task)

    def test_route_respects_agent_max_concurrent(
        self, populated_registry: AgentRegistry
    ) -> None:
        router = Router(populated_registry)
        populated_registry.agents["research-agent"].current_load = 5
        populated_registry.agents["research-agent"].card.max_concurrent = 5
        populated_registry.agents["analysis-agent"].current_load = 1
        populated_registry.agents["analysis-agent"].card.max_concurrent = 1
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route(task)
        assert selected.card.name == "writing-agent"

    def test_route_no_constraints_returns_any_agent(
        self, populated_registry: AgentRegistry
    ) -> None:
        """Task with no agent and no capabilities routes to any healthy agent."""
        router = Router(populated_registry)
        task = Task(name="t")
        selected = router.route(task)
        assert selected.card.name in {
            "research-agent",
            "analysis-agent",
            "writing-agent",
        }


class TestRoutingStrategies:
    """Tests for routing strategy selection."""

    def test_round_robin(self, populated_registry: AgentRegistry) -> None:
        policy = RoutingPolicy(strategy=RoutingStrategy.ROUND_ROBIN)
        router = Router(populated_registry, policy=policy)
        task = Task(name="t", required_capabilities=["summarization"])

        agents_seen: list[str] = []
        for _ in range(6):
            selected = router.route(task)
            agents_seen.append(selected.card.name)

        # Should cycle through agents
        assert len(set(agents_seen)) > 1

    def test_least_cost(self, populated_registry: AgentRegistry) -> None:
        policy = RoutingPolicy(strategy=RoutingStrategy.LEAST_COST)
        router = Router(populated_registry, policy=policy)
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route(task)
        # writing-agent has cost_per_task=0.01, the lowest
        assert selected.card.name == "writing-agent"

    def test_least_load(self, populated_registry: AgentRegistry) -> None:
        policy = RoutingPolicy(strategy=RoutingStrategy.LEAST_LOAD)
        router = Router(populated_registry, policy=policy)
        populated_registry.agents["writing-agent"].current_load = 10
        populated_registry.agents["research-agent"].current_load = 0
        populated_registry.agents["analysis-agent"].current_load = 5
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route(task)
        assert selected.card.name == "research-agent"

    def test_least_latency(self, populated_registry: AgentRegistry) -> None:
        policy = RoutingPolicy(strategy=RoutingStrategy.LEAST_LATENCY)
        router = Router(populated_registry, policy=policy)
        populated_registry.agents["writing-agent"].avg_latency_ms = 500.0
        populated_registry.agents["research-agent"].avg_latency_ms = 100.0
        populated_registry.agents["analysis-agent"].avg_latency_ms = 200.0
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route(task)
        assert selected.card.name == "research-agent"

    def test_random_strategy(self, populated_registry: AgentRegistry) -> None:
        policy = RoutingPolicy(strategy=RoutingStrategy.RANDOM)
        router = Router(populated_registry, policy=policy)
        task = Task(name="t", required_capabilities=["summarization"])
        # Just verify it does not raise and returns a valid agent
        selected = router.route(task)
        assert selected.card.name in {
            "research-agent",
            "analysis-agent",
            "writing-agent",
        }

    def test_custom_strategy_hook_selects_agent(
        self, populated_registry: AgentRegistry
    ) -> None:
        def prefer_analysis(
            agents: Sequence[RegisteredAgent],
            task: Task,
            policy: RoutingPolicy,
        ) -> RegisteredAgent:
            del task, policy
            for agent in agents:
                if agent.card.name == "analysis-agent":
                    return agent
            return agents[0]

        router = Router(populated_registry, strategy_hook=prefer_analysis)
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route(task)
        assert selected.card.name == "analysis-agent"

    def test_custom_strategy_hook_orders_multi_route(
        self, populated_registry: AgentRegistry
    ) -> None:
        def prefer_writing_then_research(
            agents: Sequence[RegisteredAgent],
            task: Task,
            policy: RoutingPolicy,
        ) -> list[RegisteredAgent]:
            del task, policy
            preferred: list[RegisteredAgent] = []
            for name in ("writing-agent", "research-agent"):
                for agent in agents:
                    if agent.card.name == name:
                        preferred.append(agent)
            return preferred

        router = Router(populated_registry, strategy_hook=prefer_writing_then_research)
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route_multi(task, count=2)
        assert [agent.card.name for agent in selected] == [
            "writing-agent",
            "research-agent",
        ]


class TestRouterMulti:
    """Tests for multi-agent fan-out routing."""

    def test_route_multi(self, populated_registry: AgentRegistry) -> None:
        router = Router(populated_registry)
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route_multi(task, count=2)
        assert len(selected) == 2

    def test_route_multi_limited_by_available(
        self, populated_registry: AgentRegistry
    ) -> None:
        router = Router(populated_registry)
        task = Task(name="t", required_capabilities=["web_search"])
        # Only one agent has web_search
        selected = router.route_multi(task, count=5)
        assert len(selected) == 1

    def test_route_multi_no_capable_raises(
        self, populated_registry: AgentRegistry
    ) -> None:
        router = Router(populated_registry)
        task = Task(name="t", required_capabilities=["nonexistent_cap"])
        with pytest.raises(NoCapableAgentError):
            router.route_multi(task, count=2)

    def test_route_multi_falls_back_when_all_at_capacity(
        self, populated_registry: AgentRegistry
    ) -> None:
        """route_multi uses all candidates when all are at capacity."""
        policy = RoutingPolicy(max_queue_depth=1)
        router = Router(populated_registry, policy=policy)
        # All agents at capacity
        for agent in populated_registry.agents.values():
            agent.current_load = 100
        task = Task(name="t", required_capabilities=["summarization"])
        # Should still return agents (falls back to candidates)
        selected = router.route_multi(task, count=2)
        assert len(selected) >= 1

    def test_route_multi_sorts_by_cost(self, populated_registry: AgentRegistry) -> None:
        """route_multi with LEAST_COST returns agents sorted by cost."""
        policy = RoutingPolicy(strategy=RoutingStrategy.LEAST_COST)
        router = Router(populated_registry, policy=policy)
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route_multi(task, count=3)
        costs = [a.card.cost_per_task for a in selected]
        assert costs == sorted(costs)

    def test_route_multi_sorts_by_latency(
        self, populated_registry: AgentRegistry
    ) -> None:
        """route_multi with LEAST_LATENCY returns agents sorted by latency."""
        policy = RoutingPolicy(strategy=RoutingStrategy.LEAST_LATENCY)
        router = Router(populated_registry, policy=policy)
        populated_registry.agents["writing-agent"].avg_latency_ms = 300.0
        populated_registry.agents["research-agent"].avg_latency_ms = 100.0
        populated_registry.agents["analysis-agent"].avg_latency_ms = 200.0
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route_multi(task, count=3)
        latencies = [a.avg_latency_ms for a in selected]
        assert latencies == sorted(latencies)

    def test_route_multi_sorts_by_load(self, populated_registry: AgentRegistry) -> None:
        """route_multi with LEAST_LOAD returns agents sorted by load."""
        policy = RoutingPolicy(strategy=RoutingStrategy.LEAST_LOAD)
        router = Router(populated_registry, policy=policy)
        populated_registry.agents["writing-agent"].current_load = 5
        populated_registry.agents["research-agent"].current_load = 1
        populated_registry.agents["analysis-agent"].current_load = 3
        task = Task(name="t", required_capabilities=["summarization"])
        selected = router.route_multi(task, count=3)
        loads = [a.current_load for a in selected]
        assert loads == sorted(loads)


class TestRouteExplainability:
    """Tests for ranked routing explanations."""

    def test_explain_route_sorts_and_marks_availability(
        self, populated_registry: AgentRegistry
    ) -> None:
        policy = RoutingPolicy(strategy=RoutingStrategy.LEAST_LOAD, max_queue_depth=2)
        router = Router(populated_registry, policy=policy)
        populated_registry.agents["research-agent"].current_load = 0
        populated_registry.agents["analysis-agent"].current_load = 1
        populated_registry.agents["writing-agent"].current_load = 5
        task = Task(name="t", required_capabilities=["summarization"])

        explanation = router.explain_route(task)

        assert [row.agent_name for row in explanation] == [
            "research-agent",
            "analysis-agent",
            "writing-agent",
        ]
        assert explanation[0].rank == 1
        assert explanation[0].available is True
        assert explanation[-1].available is False
        assert explanation[0].strategy_value == 0.0
        assert any("least_load=0" in reason for reason in explanation[0].reasons)

    def test_explain_route_includes_custom_hook_reason(
        self, populated_registry: AgentRegistry
    ) -> None:
        def prefer_writing(
            agents: Sequence[RegisteredAgent],
            task: Task,
            policy: RoutingPolicy,
        ) -> list[RegisteredAgent]:
            del task, policy
            return sorted(agents, key=lambda agent: agent.card.name != "writing-agent")

        router = Router(populated_registry, strategy_hook=prefer_writing)
        task = Task(name="t", required_capabilities=["summarization"])

        explanation = router.explain_route(task, count=2)

        assert len(explanation) == 2
        assert explanation[0].agent_name == "writing-agent"
        assert any(
            "custom strategy hook active" in reason for reason in explanation[0].reasons
        )

    def test_explain_route_reports_versioned_capability_match(self) -> None:
        registry = AgentRegistry(health_interval=600.0)
        registry.register(
            AgentCard(
                name="versioned-writer",
                capabilities=["summarization@v2"],
            )
        )
        registry.agents["versioned-writer"].status = AgentStatus.HEALTHY
        router = Router(registry)
        task = Task(name="t", required_capabilities=["summarization"])

        explanation = router.explain_route(task)

        assert explanation[0].agent_name == "versioned-writer"
        assert "matches capabilities: summarization@v2" in explanation[0].reasons


class TestRouterConcurrencyStress:
    """Concurrency stress tests for the router."""

    @pytest.mark.asyncio
    async def test_many_simultaneous_route_requests(self) -> None:
        """Route 500 tasks concurrently to verify thread-safety of routing."""
        registry = AgentRegistry(health_interval=600.0)
        for i in range(10):
            registry.register(
                AgentCard(
                    name=f"agent-{i}",
                    capabilities=["compute"],
                    cost_per_task=0.01 * (i + 1),
                )
            )

        policy = RoutingPolicy(strategy=RoutingStrategy.ROUND_ROBIN)
        router = Router(registry, policy=policy)

        results: list[str] = []

        async def _route_task(idx: int) -> str:
            task = Task(name=f"task-{idx}", required_capabilities=["compute"])
            selected = router.route(task)
            return selected.card.name

        coros = [_route_task(i) for i in range(500)]
        results = await asyncio.gather(*coros)

        assert len(results) == 500
        # All results should be valid agent names
        valid_names = {f"agent-{i}" for i in range(10)}
        for name in results:
            assert name in valid_names
        # Round-robin should distribute across agents
        counts = Counter(results)
        assert len(counts) == 10
        for count in counts.values():
            assert count == 50

    @pytest.mark.asyncio
    async def test_concurrent_mixed_strategies(self) -> None:
        """Multiple routers with different strategies route concurrently."""
        registry = AgentRegistry(health_interval=600.0)
        for i in range(5):
            card = AgentCard(
                name=f"agent-{i}",
                capabilities=["work"],
                cost_per_task=0.01 * (i + 1),
            )
            agent = registry.register(card)
            agent.current_load = i
            agent.avg_latency_ms = float(10 * (5 - i))

        strategies = [
            RoutingStrategy.ROUND_ROBIN,
            RoutingStrategy.LEAST_COST,
            RoutingStrategy.LEAST_LOAD,
            RoutingStrategy.LEAST_LATENCY,
            RoutingStrategy.RANDOM,
        ]

        async def _route_with_strategy(strategy: RoutingStrategy) -> list[str]:
            router = Router(registry, policy=RoutingPolicy(strategy=strategy))
            names = []
            for j in range(100):
                task = Task(name=f"t-{j}", required_capabilities=["work"])
                selected = router.route(task)
                names.append(selected.card.name)
            return names

        all_results = await asyncio.gather(
            *[_route_with_strategy(s) for s in strategies]
        )

        for strategy_results in all_results:
            assert len(strategy_results) == 100
            for name in strategy_results:
                assert name.startswith("agent-")

    @pytest.mark.asyncio
    async def test_high_contention_queue_depth(self) -> None:
        """Many tasks contending for agents with low queue depth."""
        registry = AgentRegistry(health_interval=600.0)
        for i in range(3):
            card = AgentCard(name=f"agent-{i}", capabilities=["scarce"])
            registry.register(card)

        policy = RoutingPolicy(
            strategy=RoutingStrategy.ROUND_ROBIN,
            max_queue_depth=50,
        )
        router = Router(registry, policy=policy)

        success_count = 0
        failure_count = 0

        async def _try_route(idx: int) -> bool:
            nonlocal success_count, failure_count
            task = Task(name=f"t-{idx}", required_capabilities=["scarce"])
            try:
                agent = router.route(task)
                agent.current_load += 1
                return True
            except QueueFullError:
                return False

        results = await asyncio.gather(*[_try_route(i) for i in range(200)])
        success_count = sum(1 for r in results if r)
        failure_count = sum(1 for r in results if not r)

        # Some should succeed, some may fail due to queue depth
        assert success_count > 0
        assert success_count + failure_count == 200
