"""Test core.smart_router — routing strategies and SmartRouter."""
import pytest
from a2a_mesh.core.registry import AgentRegistry, AgentCard, HealthRecord, HealthScorer
from a2a_mesh.core.smart_router import (
    SmartRouter,
    RoundRobinStrategy,
    LeastLoadedStrategy,
    CapabilityBasedStrategy,
    HealthWeightedStrategy,
    ExplainStrategy,
    RoutingStrategy,
    STRATEGIES,
)


def _make_registry_with_agents():
    """Create a registry with 3 test agents."""
    registry = AgentRegistry()
    cards = [
        AgentCard(name="alpha", capabilities=["web_search", "summarize"], max_concurrent=5),
        AgentCard(name="beta", capabilities=["web_search", "translate"], max_concurrent=10),
        AgentCard(name="gamma", capabilities=["code_review"], max_concurrent=3),
    ]
    for card in cards:
        registry.register(card)
    return registry


# ─── RoutingStrategy base ────────────────────────────────────────────────

class TestRoutingStrategy:
    """Test base routing strategy."""

    def test_base_select_returns_first(self):
        strat = RoutingStrategy()
        card = AgentCard(name="test")
        hr = HealthRecord()
        agents = [(card, hr)]
        result = strat.select(agents)
        assert result.name == "test"

    def test_base_select_empty(self):
        strat = RoutingStrategy()
        result = strat.select([])
        assert result is None


# ─── RoundRobinStrategy ──────────────────────────────────────────────────

class TestRoundRobinStrategy:
    """Test round-robin routing."""

    def test_round_robin_cycles(self):
        strat = RoundRobinStrategy()
        agents = [
            (AgentCard(name="a"), HealthRecord()),
            (AgentCard(name="b"), HealthRecord()),
            (AgentCard(name="c"), HealthRecord()),
        ]
        first = strat.select(agents)
        second = strat.select(agents)
        third = strat.select(agents)
        fourth = strat.select(agents)
        # Should cycle: a, b, c, a (sorted by name)
        names = [first.name, second.name, third.name, fourth.name]
        assert names[0] != names[1]  # At least different each call
        assert names[3] == names[0]   # Cycles back

    def test_round_robin_empty(self):
        strat = RoundRobinStrategy()
        assert strat.select([]) is None

    def test_round_robin_single(self):
        strat = RoundRobinStrategy()
        agents = [(AgentCard(name="only"), HealthRecord())]
        for _ in range(5):
            result = strat.select(agents)
            assert result.name == "only"


# ─── LeastLoadedStrategy ─────────────────────────────────────────────────

class TestLeastLoadedStrategy:
    """Test least-loaded routing."""

    def test_selects_least_loaded(self):
        strat = LeastLoadedStrategy()
        hr1 = HealthRecord(current_load=5)
        hr2 = HealthRecord(current_load=2)
        hr3 = HealthRecord(current_load=10)
        agents = [
            (AgentCard(name="heavy"), hr1),
            (AgentCard(name="light"), hr2),
            (AgentCard(name="overloaded"), hr3),
        ]
        result = strat.select(agents)
        assert result.name == "light"

    def test_selects_by_health_on_tie(self):
        """When loads are equal, prefer higher health score."""
        strat = LeastLoadedStrategy()
        hr1 = HealthRecord(current_load=2, health_score=0.9)
        hr2 = HealthRecord(current_load=2, health_score=0.5)
        agents = [
            (AgentCard(name="healthy"), hr1),
            (AgentCard(name="degraded"), hr2),
        ]
        result = strat.select(agents)
        assert result.name == "healthy"

    def test_empty(self):
        strat = LeastLoadedStrategy()
        assert strat.select([]) is None


# ─── CapabilityBasedStrategy ─────────────────────────────────────────────

class TestCapabilityBasedStrategy:
    """Test capability-based routing."""

    def test_matches_capabilities(self):
        strat = CapabilityBasedStrategy()
        agents = [
            (AgentCard(name="a", capabilities=["search", "translate"]), HealthRecord(health_score=0.9)),
            (AgentCard(name="b", capabilities=["search"]), HealthRecord(health_score=0.7)),
        ]
        result = strat.select(agents, required_capabilities=["translate"])
        assert result.name == "a"

    def test_no_match(self):
        strat = CapabilityBasedStrategy()
        agents = [
            (AgentCard(name="a", capabilities=["search"]), HealthRecord()),
        ]
        result = strat.select(agents, required_capabilities=["translate"])
        assert result is None

    def test_no_capability_filter_falls_back(self):
        """Without required caps, falls back to health-weighted."""
        strat = CapabilityBasedStrategy()
        agents = [
            (AgentCard(name="a"), HealthRecord(health_score=0.9)),
            (AgentCard(name="b"), HealthRecord(health_score=0.5)),
        ]
        result = strat.select(agents, required_capabilities=None)
        # Falls back to HealthWeightedStrategy (random weighted)
        assert result is not None

    def test_empty(self):
        strat = CapabilityBasedStrategy()
        assert strat.select([], required_capabilities=["search"]) is None


# ─── HealthWeightedStrategy ──────────────────────────────────────────────

class TestHealthWeightedStrategy:
    """Test health-weighted random routing."""

    def test_empty(self):
        strat = HealthWeightedStrategy()
        assert strat.select([]) is None

    def test_single(self):
        strat = HealthWeightedStrategy()
        agents = [(AgentCard(name="only"), HealthRecord())]
        result = strat.select(agents)
        assert result.name == "only"

    def test_prefers_healthier(self):
        """Run many selections; healthier agent should be chosen more often."""
        strat = HealthWeightedStrategy()
        agents = [
            (AgentCard(name="healthy"), HealthRecord(health_score=0.99)),
            (AgentCard(name="sick"), HealthRecord(health_score=0.01)),
        ]
        counts = {"healthy": 0, "sick": 0}
        for _ in range(1000):
            result = strat.select(agents)
            counts[result.name] += 1
        # Healthy should be selected >80% of the time
        assert counts["healthy"] > 800


# ─── ExplainStrategy ─────────────────────────────────────────────────────

class TestExplainStrategy:
    """Test explainable routing wrapper."""

    def test_explain_returns_agent_and_reason(self):
        inner = HealthWeightedStrategy()
        explain = ExplainStrategy(inner)
        agents = [
            (AgentCard(name="a", capabilities=["search"], max_concurrent=5), HealthRecord(health_score=0.9)),
        ]
        result, explanation = explain.select(agents)
        assert result is not None
        assert "health_weighted" in explanation
        assert "a" in explanation

    def test_explain_empty(self):
        explain = ExplainStrategy()
        result, explanation = explain.select([])
        assert result is None
        assert "No agents" in explanation


# ─── SmartRouter ─────────────────────────────────────────────────────────

class TestSmartRouter:
    """Test the SmartRouter integration."""

    def test_default_strategy(self):
        registry = AgentRegistry()
        router = SmartRouter(registry)
        assert router.default_strategy == "health_weighted"

    def test_route_by_capability(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        result = router.route(required_capabilities=["web_search"])
        assert result is not None
        assert "web_search" in result.capabilities

    def test_route_capability_not_found(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        result = router.route(required_capabilities=["nonexistent"])
        assert result is None

    def test_route_exclude_agents(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        result = router.route(
            required_capabilities=["web_search"],
            exclude_agents=["alpha"],
        )
        assert result is not None
        assert result.name != "alpha"

    def test_route_min_health_score(self):
        registry = _make_registry_with_agents()
        # Make one agent unhealthy
        registry.health_records["alpha"].health_score = 0.1
        router = SmartRouter(registry)
        result = router.route(
            required_capabilities=["web_search"],
            min_health_score=0.3,
        )
        if result is not None:
            assert result.name != "alpha"

    def test_route_no_capability_filter(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        result = router.route()
        assert result is not None  # Any healthy agent

    def test_route_with_explanation(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        agent, explanation = router.route_with_explanation(
            required_capabilities=["web_search"],
        )
        assert agent is not None
        assert "web_search" in agent.capabilities
        assert isinstance(explanation, str)
        assert len(explanation) > 0

    def test_route_explanation_no_agents(self):
        registry = AgentRegistry()
        router = SmartRouter(registry)
        agent, explanation = router.route_with_explanation(
            required_capabilities=["nonexistent"],
        )
        assert agent is None
        assert "No agents" in explanation

    def test_get_all_routes(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        routes = router.get_all_routes(required_capabilities=["web_search"])
        assert len(routes) == 2  # alpha and beta have web_search
        assert all("web_search" in r["capabilities"] for r in routes)

    def test_get_all_routes_sorted_by_health(self):
        registry = _make_registry_with_agents()
        registry.health_records["alpha"].health_score = 0.5
        registry.health_records["beta"].health_score = 0.9
        router = SmartRouter(registry)
        routes = router.get_all_routes(required_capabilities=["web_search"])
        assert routes[0]["name"] == "beta"  # Higher health first

    def test_set_custom_strategy(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        custom = RoundRobinStrategy()
        router.set_strategy("custom_rr", custom)
        assert router.get_strategy("custom_rr") is custom

    def test_get_all_strategies(self):
        assert "round_robin" in STRATEGIES
        assert "least_loaded" in STRATEGIES
        assert "capability_based" in STRATEGIES
        assert "health_weighted" in STRATEGIES
        assert "explain" in STRATEGIES

    def test_route_round_robin(self):
        registry = _make_registry_with_agents()
        router = SmartRouter(registry)
        # Only agents with web_search should be considered
        first = router.route(required_capabilities=["web_search"], strategy="round_robin")
        assert first is not None

    def test_route_least_loaded(self):
        registry = _make_registry_with_agents()
        registry.health_records["alpha"].current_load = 3
        registry.health_records["beta"].current_load = 1
        router = SmartRouter(registry)
        result = router.route(
            required_capabilities=["web_search"],
            strategy="least_loaded",
        )
        assert result is not None
        assert result.name == "beta"  # Lower load
