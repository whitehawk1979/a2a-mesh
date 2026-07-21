"""Tests for core.health_scorer — Agent health score computation."""

import pytest

from a2a_mesh.core.health_scorer import HealthScorer, AgentHealthRecord


class TestAgentHealthRecord:
    def test_default_values(self):
        record = AgentHealthRecord(agent_name="test_agent")
        assert record.agent_name == "test_agent"
        assert record.health_score == 1.0
        assert record.total_requests == 0
        assert record.total_failures == 0
        assert record.total_successes == 0
        assert record.avg_latency_ms == 0.0
        assert record.consecutive_failures == 0
        assert record.consecutive_successes == 0


class TestHealthScorerCreation:
    def test_default_parameters(self):
        scorer = HealthScorer()
        assert scorer.decay_factor == 0.15
        assert scorer.recovery_factor == 0.05
        assert scorer.latency_threshold_ms == 5000.0

    def test_custom_parameters(self):
        scorer = HealthScorer(decay_factor=0.3, recovery_factor=0.1, latency_threshold_ms=1000.0)
        assert scorer.decay_factor == 0.3
        assert scorer.recovery_factor == 0.1
        assert scorer.latency_threshold_ms == 1000.0


class TestHealthScorerSuccess:
    def test_record_success_increments(self):
        scorer = HealthScorer()
        scorer.record_success("agent_a", latency_ms=100)
        record = scorer.get_record("agent_a")
        assert record.total_successes == 1
        assert record.total_requests == 1
        assert record.consecutive_successes == 1
        assert record.consecutive_failures == 0

    def test_record_success_updates_latency(self):
        scorer = HealthScorer()
        scorer.record_success("agent_a", latency_ms=200)
        record = scorer.get_record("agent_a")
        assert record.avg_latency_ms == 200.0

    def test_record_success_running_avg_latency(self):
        scorer = HealthScorer()
        scorer.record_success("agent_a", latency_ms=100)
        scorer.record_success("agent_a", latency_ms=300)
        # Running average: 0.9 * 100 + 0.1 * 300 = 120
        record = scorer.get_record("agent_a")
        assert record.avg_latency_ms == pytest.approx(120.0, abs=1.0)

    def test_success_recovers_score(self):
        scorer = HealthScorer(recovery_factor=0.05)
        # First degrade the score
        scorer.record_failure("agent_a")
        degraded_score = scorer.get_score("agent_a")
        assert degraded_score < 1.0
        # Then recover
        scorer.record_success("agent_a", latency_ms=100)
        recovered_score = scorer.get_score("agent_a")
        assert recovered_score > degraded_score

    def test_score_capped_at_1(self):
        scorer = HealthScorer(recovery_factor=0.05)
        for _ in range(50):
            scorer.record_success("agent_a", latency_ms=100)
        assert scorer.get_score("agent_a") <= 1.0


class TestHealthScorerFailure:
    def test_record_failure_degrades_score(self):
        scorer = HealthScorer(decay_factor=0.15)
        initial = scorer.get_score("agent_a")
        assert initial == 1.0
        new_score = scorer.record_failure("agent_a")
        assert new_score < initial

    def test_consecutive_failures_exponential_decay(self):
        scorer = HealthScorer(decay_factor=0.15)
        scorer.record_failure("agent_a")
        score_after_1 = scorer.get_score("agent_a")
        scorer.record_failure("agent_a")
        score_after_2 = scorer.get_score("agent_a")
        # Second failure should decay more than first
        decay_1 = 1.0 - score_after_1
        decay_2 = score_after_1 - score_after_2
        assert decay_2 > decay_1

    def test_score_floor_at_zero(self):
        scorer = HealthScorer(decay_factor=0.15)
        for _ in range(20):
            scorer.record_failure("agent_a")
        assert scorer.get_score("agent_a") >= 0.0

    def test_failure_resets_consecutive_successes(self):
        scorer = HealthScorer()
        scorer.record_success("agent_a", latency_ms=100)
        scorer.record_success("agent_a", latency_ms=100)
        assert scorer.get_record("agent_a").consecutive_successes == 2
        scorer.record_failure("agent_a")
        assert scorer.get_record("agent_a").consecutive_successes == 0


class TestHealthScorerLatencyPenalty:
    def test_high_latency_reduces_recovery(self):
        scorer = HealthScorer(recovery_factor=0.05, latency_threshold_ms=500.0)
        # First degrade, then recover with low latency
        scorer.record_failure("agent_a")
        degraded = scorer.get_score("agent_a")
        scorer.record_success("agent_a", latency_ms=50)
        low_latency_recovery = scorer.get_score("agent_a") - degraded

        # Now test with high latency on a fresh agent
        scorer2 = HealthScorer(recovery_factor=0.05, latency_threshold_ms=500.0)
        scorer2.record_failure("agent_b")
        degraded2 = scorer2.get_score("agent_b")
        scorer2.record_success("agent_b", latency_ms=5000)
        high_latency_recovery = scorer2.get_score("agent_b") - degraded2

        assert low_latency_recovery > high_latency_recovery


class TestHealthScorerIsHealthy:
    def test_healthy_by_default(self):
        scorer = HealthScorer()
        assert scorer.is_healthy("agent_a") is True

    def test_unhealthy_after_failures(self):
        scorer = HealthScorer(decay_factor=0.15)
        for _ in range(5):
            scorer.record_failure("agent_a")
        assert scorer.is_healthy("agent_a") is False

    def test_custom_threshold(self):
        scorer = HealthScorer(decay_factor=0.15)
        scorer.record_failure("agent_a")
        # Score should be 0.85, healthy at 0.5 but not at 0.9
        assert scorer.is_healthy("agent_a", threshold=0.9) is False
        assert scorer.is_healthy("agent_a", threshold=0.5) is True


class TestHealthScorerGetAllScores:
    def test_all_scores_multiple_agents(self):
        scorer = HealthScorer()
        scorer.record_success("agent_a", latency_ms=100)
        scorer.record_failure("agent_b")
        scores = scorer.get_all_scores()
        assert "agent_a" in scores
        assert "agent_b" in scores
        assert scores["agent_a"] > scores["agent_b"]


class TestHealthScorerStats:
    def test_stats_structure(self):
        scorer = HealthScorer()
        scorer.record_success("agent_a", latency_ms=100)
        scorer.record_failure("agent_b")
        stats = scorer.stats
        assert stats["agent_count"] == 2
        assert "agent_a" in stats["agents"]
        assert "agent_b" in stats["agents"]
        assert stats["agents"]["agent_a"]["score"] > stats["agents"]["agent_b"]["score"]
        assert stats["agents"]["agent_a"]["successes"] == 1
        assert stats["agents"]["agent_b"]["failures"] == 1
