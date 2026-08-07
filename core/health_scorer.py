"""A2A Mesh Health Scorer — Agent health score computation with PG persistence.

Tracks response times and error rates per agent and computes a composite
health score between 0.0 (completely unhealthy) and 1.0 (perfect).

Inspired by sushaan-k/a2a-mesh HealthScorer with adaptations for our mesh:
- Decay factor: how much a single failure degrades the score
- Recovery factor: how much a single success recovers
- Latency threshold: above this, a soft penalty applies
- Score clamped to [0.0, 1.0] range
- PG persistence: health scores survive restarts (mesh.mesh_health_history)
- Provider status integration: LLM provider health feeds into the score
"""

import asyncio
import json
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
    # Provider status fields (from provider_health)
    provider_primary: str = ""
    provider_fallback: str = ""


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
        pg_pool=None,
        node_name: str = "",
    ) -> None:
        self.decay_factor = decay_factor
        self.recovery_factor = recovery_factor
        self.latency_threshold_ms = latency_threshold_ms
        self._records: Dict[str, AgentHealthRecord] = {}
        self._pg_pool = pg_pool
        self._node_name = node_name
        self._persist_task: Optional[asyncio.Task] = None

    def set_pg_pool(self, pg_pool, node_name: str = ""):
        """Set PG pool for persistence. Call after mesh connects to PG."""
        self._pg_pool = pg_pool
        if node_name:
            self._node_name = node_name

    async def start_persistence(self):
        """Start background task to persist health scores to PG every 60s."""
        if self._persist_task:
            return
        if self._pg_pool:
            self._persist_task = asyncio.create_task(self._persist_loop())
            log.info("📊 Health scorer persistence started (60s interval)")

    async def stop_persistence(self):
        """Stop the persistence background task."""
        if self._persist_task:
            self._persist_task.cancel()
            try:
                await self._persist_task
            except asyncio.CancelledError:
                pass
            self._persist_task = None

    async def _persist_loop(self):
        """Persist all health records to PG every 60 seconds."""
        while True:
            try:
                await asyncio.sleep(60)
                await self.persist_to_pg()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Health persist loop error: {e}")
                await asyncio.sleep(30)

    async def persist_to_pg(self):
        """Persist current health records to mesh.mesh_health_history."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            return
        try:
            for name, rec in self._records.items():
                await self._pg_pool.execute(
                    """INSERT INTO mesh.mesh_health_history
                       (node_name, health_score, avg_latency_ms, total_requests,
                        total_failures, total_successes, consecutive_failures,
                        provider_primary, provider_fallback)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                    name,
                    round(rec.health_score, 4),
                    round(rec.avg_latency_ms, 1),
                    rec.total_requests,
                    rec.total_failures,
                    rec.total_successes,
                    rec.consecutive_failures,
                    rec.provider_primary,
                    rec.provider_fallback,
                )
            if self._records:
                log.debug(f"📊 Persisted {len(self._records)} health records to PG")
        except Exception as e:
            log.error(f"Failed to persist health scores to PG: {e}")

    async def load_from_pg(self, node_name: str = ""):
        """Load the most recent health record per node from PG."""
        if not self._pg_pool or not self._pg_pool.is_connected():
            return
        try:
            # Get latest record per node
            rows = await self._pg_pool.fetch(
                """SELECT DISTINCT ON (node_name)
                          node_name, health_score, avg_latency_ms,
                          total_requests, total_failures, total_successes,
                          consecutive_failures, provider_primary, provider_fallback
                   FROM mesh.mesh_health_history
                   WHERE recorded_at > NOW() - INTERVAL '24 hours'
                   ORDER BY node_name, recorded_at DESC"""
            )
            for row in rows:
                name = row["node_name"]
                rec = self.get_record(name)
                rec.health_score = float(row["health_score"])
                rec.avg_latency_ms = float(row["avg_latency_ms"] or 0)
                rec.total_requests = int(row["total_requests"] or 0)
                rec.total_failures = int(row["total_failures"] or 0)
                rec.total_successes = int(row["total_successes"] or 0)
                rec.consecutive_failures = int(row["consecutive_failures"] or 0)
                rec.provider_primary = row["provider_primary"] or ""
                rec.provider_fallback = row["provider_fallback"] or ""
            if rows:
                log.info(f"📊 Loaded {len(rows)} health records from PG")
        except Exception as e:
            log.error(f"Failed to load health scores from PG: {e}")

    def update_provider_status(self, agent_name: str, provider_status: dict):
        """Update provider status in a health record.

        Called from the heartbeat loop when provider_health data arrives.
        If primary provider is down, apply a health penalty.
        """
        record = self.get_record(agent_name)
        primary = provider_status.get("primary", {})
        fallback = provider_status.get("fallback", {})

        record.provider_primary = primary.get("status", "unknown")
        record.provider_fallback = fallback.get("status", "unknown")

        # If primary provider is down, apply penalty
        if record.provider_primary == "fail":
            penalty = 0.1
            record.health_score = max(0.0, record.health_score - penalty)
            log.warning(f"📉 Provider penalty: {agent_name} primary fail (-{penalty})")
        # If both providers down, bigger penalty
        if record.provider_primary == "fail" and record.provider_fallback == "fail":
            penalty = 0.2
            record.health_score = max(0.0, record.health_score - penalty)
            log.warning(f"📉 Double provider penalty: {agent_name} all providers fail (-{penalty})")

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
                    "provider_primary": rec.provider_primary,
                    "provider_fallback": rec.provider_fallback,
                }
                for name, rec in self._records.items()
            },
        }