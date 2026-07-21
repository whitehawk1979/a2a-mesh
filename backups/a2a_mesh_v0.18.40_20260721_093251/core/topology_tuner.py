"""A2A Mesh Topology Tuner — Health-score-based automatic topology adjustment.

Integrates HealthScorer with the topology module to automatically:
- Promote healthy end_devices to routers when they're consistently reliable
- Demote struggling routers to end_devices to protect mesh stability
- Suggest coordinator changes when the current coordinator degrades
- Log topology change recommendations without auto-applying them (safety)

This is the "auto-tuning" layer that sits between HealthScorer and Topology,
translating health metrics into topology role adjustments.

Usage:
    from core.topology_tuner import TopologyTuner
    tuner = TopologyTuner(node)
    recommendations = await tuner.evaluate_and_recommend()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum

from .health_scorer import HealthScorer
from .topology import NodeRole

log = logging.getLogger("a2a_mesh.topology_tuner")


class TuningAction(Enum):
    """Actions the tuner can recommend."""
    PROMOTE_TO_ROUTER = "promote_to_router"
    DEMOTE_TO_END_DEVICE = "demote_to_end_device"
    SUGGEST_COORDINATOR_CHANGE = "suggest_coordinator_change"
    NO_CHANGE = "no_change"


@dataclass
class TuningRecommendation:
    """A topology change recommendation."""
    action: TuningAction
    node_name: str
    current_role: str
    recommended_role: str
    health_score: float
    reason: str
    confidence: float  # 0.0 to 1.0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "action": self.action.value,
            "node": self.node_name,
            "current_role": self.current_role,
            "recommended_role": self.recommended_role,
            "health_score": round(self.health_score, 3),
            "reason": self.reason,
            "confidence": round(self.confidence, 3),
            "timestamp": self.timestamp,
        }


class TopologyTuner:
    """Health-score-based topology auto-tuning.

    Evaluates node health scores and recommends topology role changes.
    Designed for Zsolt's P2P-first mesh: promotes reliable nodes,
    demotes struggling ones, and suggests coordinator handover.

    Configuration (in mesh_config.yaml):
        topology_tuning:
            enabled: true
            check_interval: 300        # seconds between evaluations (5 min)
            promote_threshold: 0.9      # health score to promote to router
            demote_threshold: 0.3       # health score to demote to end_device
            coordinator_threshold: 0.5  # health score below which coordinator change is suggested
            min_observations: 10         # minimum requests before making recommendations
            cooldown: 600                # seconds between role changes for the same node
            auto_apply: false            # whether to auto-apply recommendations (dangerous!)
    """

    def __init__(self, node, config=None):
        self.node = node
        self.config = config

        # Configuration with defaults
        tuning_config = getattr(config, 'topology_tuning', None) if config else None
        if isinstance(tuning_config, dict):
            self.enabled = tuning_config.get('enabled', True)
            self.check_interval = tuning_config.get('check_interval', 300)
            self.promote_threshold = tuning_config.get('promote_threshold', 0.9)
            self.demote_threshold = tuning_config.get('demote_threshold', 0.3)
            self.coordinator_threshold = tuning_config.get('coordinator_threshold', 0.5)
            self.min_observations = tuning_config.get('min_observations', 10)
            self.cooldown = tuning_config.get('cooldown', 600)
            self.auto_apply = tuning_config.get('auto_apply', False)
        elif hasattr(tuning_config, 'enabled'):
            self.enabled = getattr(tuning_config, 'enabled', True)
            self.check_interval = getattr(tuning_config, 'check_interval', 300)
            self.promote_threshold = getattr(tuning_config, 'promote_threshold', 0.9)
            self.demote_threshold = getattr(tuning_config, 'demote_threshold', 0.3)
            self.coordinator_threshold = getattr(tuning_config, 'coordinator_threshold', 0.5)
            self.min_observations = getattr(tuning_config, 'min_observations', 10)
            self.cooldown = getattr(tuning_config, 'cooldown', 600)
            self.auto_apply = getattr(tuning_config, 'auto_apply', False)
        else:
            self.enabled = True
            self.check_interval = 300
            self.promote_threshold = 0.9
            self.demote_threshold = 0.3
            self.coordinator_threshold = 0.5
            self.min_observations = 10
            self.cooldown = 600
            self.auto_apply = False

        # Track recent recommendations and changes
        self._recommendations: List[TuningRecommendation] = []
        self._last_change: Dict[str, float] = {}  # node_name → timestamp of last role change
        self._task: Optional[asyncio.Task] = None

        # Stats
        self._stats = {
            "evaluations": 0,
            "promotions": 0,
            "demotions": 0,
            "coordinator_suggestions": 0,
            "no_changes": 0,
        }

    async def start(self):
        """Start the periodic topology tuning loop."""
        if not self.enabled:
            log.info("Topology tuner disabled — skipping start")
            return

        self._task = asyncio.create_task(self._tuning_loop())
        log.info(f"Topology tuner started — evaluating every {self.check_interval}s "
                 f"(promote>={self.promote_threshold}, demote<{self.demote_threshold})")

    async def stop(self):
        """Stop the tuning loop."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("Topology tuner stopped")

    async def _tuning_loop(self):
        """Periodic evaluation loop."""
        while True:
            try:
                await asyncio.sleep(self.check_interval)
                recommendations = await self.evaluate_and_recommend()
                if recommendations:
                    for rec in recommendations:
                        log.info(f"Topology tuning: {rec.action.value} for {rec.node_name} "
                                 f"(score={rec.health_score:.2f}, reason={rec.reason})")
                        if self.auto_apply and rec.confidence >= 0.8:
                            await self._apply_recommendation(rec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Topology tuning loop error: {e}")

    async def evaluate_and_recommend(self) -> List[TuningRecommendation]:
        """Evaluate all known nodes and generate recommendations.

        Returns a list of TuningRecommendation objects.
        """
        if not self.node or not hasattr(self.node, '_health_scorer'):
            return []

        self._stats["evaluations"] += 1
        recommendations = []

        # Get health scores from the HealthScorer
        health_scorer: HealthScorer = self.node._health_scorer
        all_scores = health_scorer.get_all_scores()

        # Get known peers from discovery
        known_peers = {}
        if hasattr(self.node, 'peer_discovery') and self.node.peer_discovery:
            known_peers = self.node.peer_discovery._peers

        # Evaluate each node
        for node_name, score in all_scores.items():
            record = health_scorer.get_record(node_name)

            # Skip if not enough observations
            if record.total_requests < self.min_observations:
                continue

            # Check cooldown
            now = time.time()
            last_change = self._last_change.get(node_name, 0)
            if now - last_change < self.cooldown:
                continue

            # Determine current role
            current_role = "router"  # Default
            if node_name in known_peers:
                current_role = known_peers[node_name].role

            # PROMOTE: end_device with high health → router
            if current_role == "end_device" and score >= self.promote_threshold:
                rec = TuningRecommendation(
                    action=TuningAction.PROMOTE_TO_ROUTER,
                    node_name=node_name,
                    current_role=current_role,
                    recommended_role="router",
                    health_score=score,
                    reason=f"Consistently healthy (score={score:.2f}, {record.total_successes}/{record.total_requests} successes, avg_latency={record.avg_latency_ms:.0f}ms)",
                    confidence=min(1.0, score * (record.total_requests / 50)),
                )
                recommendations.append(rec)

            # DEMOTE: router with low health → end_device
            elif current_role in ("router", "coordinator") and score < self.demote_threshold:
                rec = TuningRecommendation(
                    action=TuningAction.DEMOTE_TO_END_DEVICE,
                    node_name=node_name,
                    current_role=current_role,
                    recommended_role="end_device",
                    health_score=score,
                    reason=f"Degrading health (score={score:.2f}, {record.consecutive_failures} consecutive failures, avg_latency={record.avg_latency_ms:.0f}ms)",
                    confidence=min(1.0, (1.0 - score) * min(record.total_requests / 20, 1.0)),
                )
                recommendations.append(rec)

            # COORDINATOR: suggest handover if coordinator is unhealthy
            elif current_role == "coordinator" and score < self.coordinator_threshold:
                # Find the healthiest router to suggest as new coordinator
                best_router = None
                best_score = 0.0
                for other_name, other_score in all_scores.items():
                    other_role = "router"
                    if other_name in known_peers:
                        other_role = known_peers[other_name].role
                    if other_name != node_name and other_role == "router" and other_score > best_score:
                        best_router = other_name
                        best_score = other_score

                rec = TuningRecommendation(
                    action=TuningAction.SUGGEST_COORDINATOR_CHANGE,
                    node_name=node_name,
                    current_role="coordinator",
                    recommended_role="router",
                    health_score=score,
                    reason=f"Coordinator unhealthy (score={score:.2f}) — suggest handover to {best_router} (score={best_score:.2f})",
                    confidence=min(1.0, (self.coordinator_threshold - score) / self.coordinator_threshold),
                )
                recommendations.append(rec)

        # Store recommendations
        self._recommendations.extend(recommendations)
        # Keep only last 100 recommendations
        if len(self._recommendations) > 100:
            self._recommendations = self._recommendations[-100:]

        return recommendations

    async def _apply_recommendation(self, rec: TuningRecommendation):
        """Apply a topology change recommendation (only if auto_apply is enabled)."""
        log.warning(f"Auto-applying topology change: {rec.action.value} for {rec.node_name}")

        # Update role in peer discovery
        if hasattr(self.node, 'peer_discovery') and self.node.peer_discovery:
            if rec.node_name in self.node.peer_discovery._peers:
                peer = self.node.peer_discovery._peers[rec.node_name]
                old_role = peer.role
                peer.role = rec.recommended_role
                log.info(f"Role changed: {rec.node_name} {old_role} → {rec.recommended_role}")

                # Broadcast role change to mesh
                if hasattr(self.node, 'send_direct'):
                    await self.node.send_direct(
                        recipient="broadcast",
                        msg_type="topology_change",
                        payload={
                            "action": rec.action.value,
                            "node": rec.node_name,
                            "old_role": old_role,
                            "new_role": rec.recommended_role,
                            "health_score": rec.health_score,
                            "reason": rec.reason,
                        },
                        priority=7,
                    )

        self._last_change[rec.node_name] = time.time()

        # Update stats
        if rec.action == TuningAction.PROMOTE_TO_ROUTER:
            self._stats["promotions"] += 1
        elif rec.action == TuningAction.DEMOTE_TO_END_DEVICE:
            self._stats["demotions"] += 1
        elif rec.action == TuningAction.SUGGEST_COORDINATOR_CHANGE:
            self._stats["coordinator_suggestions"] += 1

    @property
    def stats(self) -> dict:
        """Return tuner statistics."""
        return {
            **self._stats,
            "enabled": self.enabled,
            "auto_apply": self.auto_apply,
            "recent_recommendations": [r.to_dict() for r in self._recommendations[-10:]],
        }