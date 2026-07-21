"""A2A Mesh Health Scorer — Agent health score computation.

Tracks response times and error rates per agent and computes a composite
health score between 0.0 (completely unhealthy) and 1.0 (perfect).

Inspired by sushaan-k/a2a-mesh HealthScorer with adaptations for our mesh:
- Decay factor: how much a single failure degrades the score
- Recovery factor: how much a single success recovers
- Latency threshold: above this, soft penalty applies
- Score clamped to [0.0, 1.0] range
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional

log = logging.getLogger("a2a_mesh.health_scorer")


@dataclass
class AgentHealthRecord:
    """Health record for a single agent."""
    agent_name: str
    health_score: float = 1.0
    total_requests: int = 0
    total_failures: int = 0
    total_successes: int = 0
    avg_latency_ms: float = 0.0
    last_success: Optional[float] = None
    last_failure: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class HealthScorer:
    """Computes and updates composite health scores for agents.

    The score combines error rate and latency into a single 0-1 value.
    Failures cause fast degradation; successes cause slow recovery —
    mirroring real-world trust dynamics.

    Attributes:
        decay_factor: Score penalty per failure (0-1).
        recovery_factor: Score recovery per success (0-1).
        latency_threshold_ms: Latency above which a soft penalty applies.
    """

    def __init__(
        self,
        decay_factor: float = 0.15,
        recovery_factor: float = 0.05,
        latency_threshold_ms: float = 5000.0,
    ) -> None:
        self.decay_factor = decay_factor
        self.recovery_factor = recovery_factor
        self.latency_threshold_ms = latency_threshold_ms
        self._records: Dict[str, AgentHealthRecord] = {}

    def get_record(self, agent_name: str) -> AgentHealthRecord:
        """Get or create a health record for an agent."""
        if agent_name not in self._records:
            self._records[agent_name] = AgentHealthRecord(agent_name=agent_name)
        return self._records[agent_name]

    def record_success(
        self,
        agent_name: str,
        latency_ms: float = 0.0,
    ) -> float:
        """Record a successful request and update the health score.

        Args:
            agent_name: The agent that completed the request.
            latency_ms: Observed response latency in milliseconds.

        Returns:
            The updated health score.
        """
        record = self.get_record(agent_name)
        record.total_requests += 1
        record.total_successes += 1
        record.consecutive_failures = 0
        record.consecutive_successes += 1
        record.last_success = time.time()

        # Update running average latency
        if record.avg_latency_ms == 0:
            record.avg_latency_ms = latency_ms
        else:
            record.avg_latency_ms = 0.9 * record.avg_latency_ms + 0.1 * latency_ms

        # Recover score
        recovery = self.recovery_factor

        # Latency penalty: if above threshold, reduce recovery
        if latency_ms > self.latency_threshold_ms:
            latency_penalty = min(0.5, (latency_ms - self.latency_threshold_ms) / self.latency_threshold_ms)
            recovery *= (1.0 - latency_penalty)

        record.health_score = min(1.0, record.health_score + recovery)
        return record.health_score

    def record_failure(self, agent_name: str) -> float:
        """Record a failed request and update the health score.

        Args:
            agent_name: The agent that failed the request.

        Returns:
            The updated health score.
        """
        record = self.get_record(agent_name)
        record.total_requests += 1
        record.total_failures += 1
        record.consecutive_successes = 0
        record.consecutive_failures += 1
        record.last_failure = time.time()

        # Exponential decay for consecutive failures
        decay = self.decay_factor * (1 + 0.5 * min(record.consecutive_failures - 1, 5))
        record.health_score = max(0.0, record.health_score - decay)
        return record.health_score

    def get_score(self, agent_name: str) -> float:
        """Get the current health score for an agent."""
        return self.get_record(agent_name).health_score

    def get_all_scores(self) -> Dict[str, float]:
        """Get all agent health scores."""
        return {name: rec.health_score for name, rec in self._records.items()}

    def is_healthy(self, agent_name: str, threshold: float = 0.5) -> bool:
        """Check if an agent is healthy (score >= threshold)."""
        return self.get_score(agent_name) >= threshold

    @property
    def stats(self) -> dict:
        """Return health scorer statistics."""
        return {
            "agent_count": len(self._records),
            "agents": {
                name: {
                    "score": round(rec.health_score, 3),
                    "requests": rec.total_requests,
                    "failures": rec.total_failures,
                    "successes": rec.total_successes,
                    "avg_latency_ms": round(rec.avg_latency_ms, 1),
                    "consecutive_failures": rec.consecutive_failures,
                }
                for name, rec in self._records.items()
            },
        }