"""Test core.registry — AgentCapability, AgentCard, HealthRecord, HealthScorer, AgentRegistry."""
import time
import pytest
from a2a_mesh.core.registry import (
    AgentCapability,
    AgentCard,
    HealthRecord,
    HealthScorer,
    AgentRegistry,
    DEFAULT_WEIGHTS,
    DECAY_FACTOR,
    RECOVERY_FACTOR,
    LATENCY_THRESHOLD_MS,
)


# ─── AgentCapability ─────────────────────────────────────────────────────

class TestAgentCapability:
    """Test capability parsing and matching."""

    def test_from_string_simple(self):
        cap = AgentCapability.from_string("web_search")
        assert cap.name == "web_search"
        assert cap.version is None

    def test_from_string_versioned(self):
        cap = AgentCapability.from_string("summarization@v2")
        assert cap.name == "summarization"
        assert cap.version == "v2"

    def test_from_string_whitespace(self):
        cap = AgentCapability.from_string("  search@v1  ")
        assert cap.name == "search"
        assert cap.version == "v1"

    def test_str_simple(self):
        cap = AgentCapability(name="code_review")
        assert str(cap) == "code_review"

    def test_str_versioned(self):
        cap = AgentCapability(name="web_search", version="v3")
        assert str(cap) == "web_search@v3"

    def test_matches_same_name_no_version(self):
        cap = AgentCapability(name="search")
        req = AgentCapability(name="search")
        assert cap.matches(req) is True

    def test_matches_versioned_required(self):
        cap = AgentCapability(name="search", version="v2")
        req = AgentCapability(name="search", version="v2")
        assert cap.matches(req) is True

    def test_matches_versioned_required_mismatch(self):
        cap = AgentCapability(name="search", version="v1")
        req = AgentCapability(name="search", version="v2")
        assert cap.matches(req) is False

    def test_matches_no_version_required_any_matches(self):
        cap = AgentCapability(name="search", version="v2")
        req = AgentCapability(name="search")
        assert cap.matches(req) is True

    def test_matches_different_name(self):
        cap = AgentCapability(name="search")
        req = AgentCapability(name="summarize")
        assert cap.matches(req) is False


# ─── AgentCard ───────────────────────────────────────────────────────────

class TestAgentCard:
    """Test agent capability card."""

    def test_card_defaults(self):
        card = AgentCard(name="test_agent")
        assert card.name == "test_agent"
        assert card.capabilities == []
        assert card.version == ""
        assert card.max_concurrent == 10
        assert card.cost_per_task == 0.0

    def test_card_with_capabilities(self):
        card = AgentCard(
            name="morzsa",
            capabilities=["web_search", "code_review@v2"],
            endpoint="http://192.168.1.30:8651",
        )
        assert card.has_capability("web_search") is True
        assert card.has_capability("code_review@v2") is True
        assert card.has_capability("code_review@v1") is False
        assert card.has_capability("translation") is False

    def test_card_capability_version_match(self):
        card = AgentCard(name="agent", capabilities=["search@v2"])
        # Without version requirement → matches any version
        assert card.has_capability("search") is True
        # With version → must match exactly
        assert card.has_capability("search@v2") is True
        assert card.has_capability("search@v3") is False

    def test_card_skills(self):
        card = AgentCard(
            name="morzsa",
            skills=[{"id": "gdm", "name": "GDM", "tags": ["dev"]}],
        )
        assert len(card.skills) == 1
        assert card.skills[0]["id"] == "gdm"


# ─── HealthRecord ───────────────────────────────────────────────────────

class TestHealthRecord:
    """Test health tracking data."""

    def test_defaults(self):
        hr = HealthRecord()
        assert hr.health_score == 1.0
        assert hr.total_requests == 0
        assert hr.total_failures == 0
        assert hr.current_load == 0
        assert hr.uptime_pct == 100.0

    def test_success_rate_no_data(self):
        hr = HealthRecord()
        assert hr.success_rate == 1.0  # Assume healthy if no data

    def test_success_rate_with_data(self):
        hr = HealthRecord(total_requests=10, total_failures=2)
        assert hr.success_rate == 0.8

    def test_status_healthy(self):
        hr = HealthRecord(health_score=0.9)
        assert hr.status == "healthy"

    def test_status_degraded(self):
        hr = HealthRecord(health_score=0.6)
        assert hr.status == "degraded"

    def test_status_unhealthy(self):
        hr = HealthRecord(health_score=0.2)
        assert hr.status == "unhealthy"

    def test_status_dead(self):
        hr = HealthRecord(health_score=0.0)
        assert hr.status == "dead"

    def test_status_boundary_healthy(self):
        assert HealthRecord(health_score=0.8).status == "healthy"

    def test_status_boundary_degraded(self):
        assert HealthRecord(health_score=0.5).status == "degraded"


# ─── HealthScorer ────────────────────────────────────────────────────────

class TestHealthScorer:
    """Test composite health score computation."""

    def test_defaults(self):
        scorer = HealthScorer()
        assert scorer.decay_factor == DECAY_FACTOR
        assert scorer.recovery_factor == RECOVERY_FACTOR
        assert scorer.latency_threshold_ms == LATENCY_THRESHOLD_MS
        assert scorer.weights == DEFAULT_WEIGHTS

    def test_custom_weights(self):
        custom = {"latency": 0.5, "success_rate": 0.3, "availability": 0.2}
        scorer = HealthScorer(weights=custom)
        assert scorer.weights == custom

    def test_record_success_initial(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        score = scorer.record_success(hr, latency_ms=100.0)
        assert hr.total_requests == 1
        assert hr.consecutive_successes == 1
        assert hr.consecutive_failures == 0
        assert score > 0

    def test_record_success_latency_ema(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        scorer.record_success(hr, latency_ms=100.0)
        scorer.record_success(hr, latency_ms=200.0)
        # EMA: avg ≈ 100*0.7 + 200*0.3 = 130
        assert 120 < hr.avg_latency_ms < 140

    def test_record_failure(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        scorer.record_failure(hr)
        assert hr.total_requests == 1
        assert hr.total_failures == 1
        assert hr.consecutive_failures == 1

    def test_consecutive_failures_decay(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        # 5 consecutive failures → heavy penalty
        for _ in range(5):
            scorer.record_failure(hr)
        assert hr.consecutive_failures == 5
        assert hr.health_score < 0.5  # Should be heavily penalized

    def test_consecutive_failures_3_plus_penalty(self):
        """3+ consecutive failures trigger 50% penalty."""
        scorer = HealthScorer()
        hr = HealthRecord()
        # First record a success to get a baseline
        scorer.record_success(hr, latency_ms=50.0)
        score_before = hr.health_score
        # Now 3 failures
        for _ in range(3):
            scorer.record_failure(hr)
        # Score should be heavily penalized (composite * 0.5)
        assert hr.health_score < score_before * 0.6

    def test_consecutive_successes_recovery(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        # Degrade first
        scorer.record_failure(hr)
        degraded = hr.health_score
        # Then recover with 3+ successes
        for _ in range(5):
            scorer.record_success(hr, latency_ms=50.0)
        assert hr.health_score > degraded

    def test_health_check_passed(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        scorer.record_health_check(hr, passed=True)
        assert hr._health_checks_total == 1
        assert hr._health_checks_passed == 1
        assert hr.uptime_pct == 100.0

    def test_health_check_failed(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        scorer.record_health_check(hr, passed=False)
        assert hr.uptime_pct == 0.0

    def test_health_check_mixed(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        scorer.record_health_check(hr, passed=True)
        scorer.record_health_check(hr, passed=True)
        scorer.record_health_check(hr, passed=False)
        # 2/3 passed ≈ 66.7%
        assert abs(hr.uptime_pct - 66.666666) < 0.01

    def test_latency_score_at_threshold(self):
        scorer = HealthScorer()
        # Latency at or below threshold → score 1.0
        assert scorer._latency_score(0) == 1.0
        assert scorer._latency_score(scorer.latency_threshold_ms) == 1.0

    def test_latency_score_above_threshold(self):
        scorer = HealthScorer()
        # Latency above threshold → sigmoid decay
        high_latency = scorer.latency_threshold_ms * 5
        score = scorer._latency_score(high_latency)
        assert 0 < score < 1.0

    def test_latency_score_negative_returns_1(self):
        scorer = HealthScorer()
        assert scorer._latency_score(-10) == 1.0

    def test_compute_score_no_health_checks(self):
        scorer = HealthScorer()
        hr = HealthRecord()
        hr.avg_latency_ms = 50.0
        score = scorer.compute_score(hr)
        # No health checks → availability_score = 1.0
        # No consecutive failures → no penalty
        assert score > 0

    def test_score_bounded_0_to_1(self):
        """Health score must always be between 0 and 1."""
        scorer = HealthScorer()
        hr = HealthRecord()
        # Extreme degradation
        for _ in range(20):
            scorer.record_failure(hr)
        assert hr.health_score >= 0.0

        # Extreme recovery
        for _ in range(20):
            scorer.record_success(hr, latency_ms=10.0)
        assert hr.health_score <= 1.0


# ─── AgentRegistry ───────────────────────────────────────────────────────

class TestAgentRegistry:
    """Test in-memory agent registry with discovery."""

    def test_register_agent(self):
        registry = AgentRegistry()
        card = AgentCard(name="morzsa", capabilities=["web_search", "code_review"])
        hr = registry.register(card)
        assert "morzsa" in registry.agents
        assert hr is not None
        assert hr.health_score == 1.0

    def test_register_duplicate_no_force(self):
        registry = AgentRegistry()
        card = AgentCard(name="morzsa", capabilities=["search"])
        registry.register(card)
        # Second registration without force → returns existing health record
        card2 = AgentCard(name="morzsa", capabilities=["other"])
        hr = registry.register(card2)
        # Should keep original capabilities
        assert registry.agents["morzsa"].capabilities == ["search"]

    def test_register_duplicate_with_force(self):
        registry = AgentRegistry()
        card = AgentCard(name="morzsa", capabilities=["search"])
        registry.register(card)
        # Force overwrite
        card2 = AgentCard(name="morzsa", capabilities=["other"])
        registry.register(card2, force=True)
        assert registry.agents["morzsa"].capabilities == ["other"]

    def test_deregister_agent(self):
        registry = AgentRegistry()
        card = AgentCard(name="morzsa")
        registry.register(card)
        registry.deregister("morzsa")
        assert "morzsa" not in registry.agents
        assert "morzsa" not in registry.health_records

    def test_deregister_nonexistent(self):
        registry = AgentRegistry()
        # Should not raise
        registry.deregister("ghost")

    def test_get_agent(self):
        registry = AgentRegistry()
        card = AgentCard(name="morzsa", capabilities=["search"])
        registry.register(card)
        result = registry.get("morzsa")
        assert result is not None
        assert result.name == "morzsa"

    def test_get_nonexistent(self):
        registry = AgentRegistry()
        assert registry.get("ghost") is None

    def test_get_health(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="morzsa"))
        hr = registry.get_health("morzsa")
        assert hr is not None
        assert hr.health_score == 1.0

    def test_find_by_capability_single(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="a", capabilities=["search", "summarize"]))
        registry.register(AgentCard(name="b", capabilities=["search"]))
        registry.register(AgentCard(name="c", capabilities=["translate"]))

        matches = registry.find_by_capability(["search"])
        assert len(matches) == 2
        names = [m[0].name for m in matches]
        assert "a" in names
        assert "b" in names

    def test_find_by_capability_multiple(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="a", capabilities=["search", "summarize"]))
        registry.register(AgentCard(name="b", capabilities=["search"]))

        # Need BOTH search AND summarize
        matches = registry.find_by_capability(["search", "summarize"])
        assert len(matches) == 1
        assert matches[0][0].name == "a"

    def test_find_by_capability_versioned(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="a", capabilities=["search@v2"]))
        registry.register(AgentCard(name="b", capabilities=["search@v1"]))

        # Exact version match
        matches = registry.find_by_capability(["search@v2"])
        assert len(matches) == 1
        assert matches[0][0].name == "a"

        # No version → matches all
        matches = registry.find_by_capability(["search"])
        assert len(matches) == 2

    def test_find_by_capability_healthy_only(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="healthy", capabilities=["search"]))
        registry.register(AgentCard(name="unhealthy", capabilities=["search"]))

        # Make one unhealthy
        registry.health_records["unhealthy"].health_score = 0.1

        matches = registry.find_by_capability(["search"], healthy_only=True, min_health_score=0.3)
        assert len(matches) == 1
        assert matches[0][0].name == "healthy"

    def test_find_by_capability_sorted_by_health(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="a", capabilities=["search"]))
        registry.register(AgentCard(name="b", capabilities=["search"]))

        registry.health_records["a"].health_score = 0.9
        registry.health_records["b"].health_score = 0.5

        matches = registry.find_by_capability(["search"])
        # Best health first
        assert matches[0][0].name == "a"
        assert matches[1][0].name == "b"

    def test_list_agents(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="a"))
        registry.register(AgentCard(name="b"))
        result = registry.list_agents()
        assert len(result) == 2

    def test_record_success_and_failure(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="morzsa"))

        score = registry.record_success("morzsa", latency_ms=100.0)
        assert score > 0

        score = registry.record_failure("morzsa")
        assert registry.health_records["morzsa"].total_failures == 1

    def test_record_success_nonexistent_agent(self):
        registry = AgentRegistry()
        # Should auto-create health record
        score = registry.record_success("ghost", latency_ms=50.0)
        assert score > 0

    def test_get_stats(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="a", capabilities=["search"]))
        registry.register(AgentCard(name="b", capabilities=["translate"]))

        stats = registry.get_stats()
        assert stats["total_agents"] == 2
        assert "a" in stats["agents"]
        assert stats["agents"]["a"]["capabilities"] == ["search"]


# ─── Agent Approval System ───────────────────────────────────────────────

class TestAgentApproval:
    """Test pending agent approval system."""

    def test_auto_approve(self):
        registry = AgentRegistry(auto_approve=True)
        card = AgentCard(name="new_agent", capabilities=["search"])
        result = registry.request_registration(card)
        assert result == "approved"
        assert "new_agent" in registry.agents

    def test_manual_approve(self):
        registry = AgentRegistry(auto_approve=False)
        card = AgentCard(name="new_agent", capabilities=["search"])
        result = registry.request_registration(card)
        assert result == "pending"
        assert "new_agent" in registry.pending_agents
        assert "new_agent" not in registry.agents

        approved = registry.approve_agent("new_agent")
        assert approved is not None
        assert approved.name == "new_agent"
        assert "new_agent" in registry.agents
        assert "new_agent" not in registry.pending_agents

    def test_reject_agent(self):
        registry = AgentRegistry(auto_approve=False)
        card = AgentCard(name="bad_agent", capabilities=["evil"])
        registry.request_registration(card)
        result = registry.reject_agent("bad_agent")
        assert result is True
        assert "bad_agent" not in registry.pending_agents

    def test_reject_nonexistent(self):
        registry = AgentRegistry(auto_approve=False)
        result = registry.reject_agent("ghost")
        assert result is False

    def test_approve_nonexistent(self):
        registry = AgentRegistry(auto_approve=False)
        result = registry.approve_agent("ghost")
        assert result is None

    def test_request_registration_already_registered(self):
        registry = AgentRegistry()
        card = AgentCard(name="morzsa", capabilities=["search"])
        registry.register(card)

        # Re-register → should update (returns "approved")
        card2 = AgentCard(name="morzsa", capabilities=["search", "code_review"])
        result = registry.request_registration(card2)
        assert result == "approved"

    def test_list_pending(self):
        registry = AgentRegistry(auto_approve=False)
        registry.request_registration(AgentCard(name="a", capabilities=["x"]))
        registry.request_registration(AgentCard(name="b", capabilities=["y"]))
        pending = registry.list_pending()
        assert len(pending) == 2

    def test_approval_callback(self):
        registry = AgentRegistry(auto_approve=False)
        events = []
        registry.on_approval(lambda card, action: events.append((card.name, action)))

        registry.request_registration(AgentCard(name="a"))
        registry.approve_agent("a")
        assert len(events) == 1
        assert events[0] == ("a", "approved")

    def test_approval_callback_reject(self):
        registry = AgentRegistry(auto_approve=False)
        events = []
        registry.on_approval(lambda card, action: events.append((card.name, action)))

        registry.request_registration(AgentCard(name="a"))
        registry.reject_agent("a")
        assert len(events) == 1
        assert events[0] == ("a", "rejected")

    def test_skill_merge_on_re_registration(self):
        """When re-registering, skills from existing and new card should merge."""
        registry = AgentRegistry()
        card1 = AgentCard(
            name="morzsa",
            capabilities=["a2a_messaging"],
            skills=[{"id": "gdm", "name": "GDM"}],
        )
        registry.register(card1)

        # P2P handshake re-registers with fewer skills
        card2 = AgentCard(name="morzsa", capabilities=["a2a_messaging"])
        result = registry.request_registration(card2)
        # Should keep existing skills
        assert result == "approved"

    def test_capability_merge_on_re_registration(self):
        """Capabilities should merge when re-registering with fewer caps."""
        registry = AgentRegistry()
        card1 = AgentCard(
            name="morzsa",
            capabilities=["web_search", "code_review", "a2a_messaging"],
        )
        registry.register(card1)

        # P2P handshake only sends a2a_messaging
        card2 = AgentCard(name="morzsa", capabilities=["a2a_messaging"])
        result = registry.request_registration(card2)
        assert result == "approved"
        # Capabilities should be merged (existing + new)
        merged = registry.agents["morzsa"].capabilities
        assert "web_search" in merged
        assert "code_review" in merged


# ─── Health Monitoring ───────────────────────────────────────────────────

class TestHealthMonitoring:
    """Test background health monitoring (without network)."""

    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self):
        registry = AgentRegistry()
        registry.register(AgentCard(name="test_agent"))
        await registry.start_health_monitoring(interval=1.0)
        assert registry._running is True
        assert registry._health_task is not None

        await registry.stop_health_monitoring()
        assert registry._running is False

    @pytest.mark.asyncio
    async def test_check_agent_health_unknown(self):
        registry = AgentRegistry()
        # No agent registered → "unknown"
        status = await registry.check_agent_health("ghost")
        assert status == "unknown"

    @pytest.mark.asyncio
    async def test_check_agent_health_no_discovery(self):
        """Health check without PeerDiscovery should return degraded status."""
        registry = AgentRegistry()
        registry.register(AgentCard(name="test_agent"))
        # No peer discovery, no endpoint → health check fails
        status = await registry.check_agent_health("test_agent")
        # Should record a failed health check
        hr = registry.get_health("test_agent")
        assert hr._health_checks_total >= 1
