"""A2A Mesh Alert Manager — Custom alert rules evaluated against mesh metrics.

Rules are defined as Python expressions evaluated against current metrics.
When a rule fires, it triggers a callback (e.g., Telegram alert).

Rule format:
    {
        "id": "peers_low",
        "name": "Peer count below 2",
        "metric": "peers_connected",
        "operator": "<",
        "threshold": 2,
        "severity": "critical",  # critical, warning, info
        "cooldown": 300,  # seconds between repeated alerts
        "enabled": True,
    }

The alert manager evaluates rules periodically and fires on state change
(up→down, down→up) to avoid spam.
"""

import time
import logging
import asyncio
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum

log = logging.getLogger("a2a_mesh.alerts")


class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertState(Enum):
    """Alert firing state."""
    OK = "ok"          # Metric is within threshold
    FIRING = "firing"   # Metric violates threshold
    RESOLVED = "resolved"  # Was firing, now back to OK


@dataclass
class AlertRule:
    """A custom alert rule."""
    id: str
    name: str
    metric: str                        # Metric name (e.g., "peers_connected")
    operator: str = "<"                # <, >, <=, >=, ==, !=
    threshold: float = 0
    severity: AlertSeverity = AlertSeverity.WARNING
    cooldown: float = 300.0             # Seconds between repeated alerts
    enabled: bool = True
    # Internal state
    state: AlertState = AlertState.OK
    last_fired: float = 0.0
    fire_count: int = 0

    def evaluate(self, value: float) -> bool:
        """Evaluate the rule against a value. Returns True if firing."""
        if self.operator == "<":
            return value < self.threshold
        elif self.operator == ">":
            return value > self.threshold
        elif self.operator == "<=":
            return value <= self.threshold
        elif self.operator == ">=":
            return value >= self.threshold
        elif self.operator == "==":
            return value == self.threshold
        elif self.operator == "!=":
            return value != self.threshold
        return False

    @property
    def can_fire(self) -> bool:
        """Check if enough time has passed since last fire (cooldown)."""
        if self.state == AlertState.FIRING:
            return time.time() - self.last_fired >= self.cooldown
        return True  # State transitions always fire

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "metric": self.metric,
            "operator": self.operator,
            "threshold": self.threshold,
            "severity": self.severity.value,
            "cooldown": self.cooldown,
            "enabled": self.enabled,
            "state": self.state.value,
            "fire_count": self.fire_count,
            "last_fired": self.last_fired if self.last_fired else None,
        }


class AlertManager:
    """Manages custom alert rules and evaluates them against mesh metrics.

    Default rules:
    - peers_connected < 2 → critical
    - transport_errors > 0 → warning
    - uptime_seconds < 60 (after startup) → warning
    """

    def __init__(self, alert_callback: Optional[Callable] = None):
        self._rules: Dict[str, AlertRule] = {}
        self._callback = alert_callback  # async def callback(rule: AlertRule, value: float)
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._check_interval = 30.0  # seconds between checks
        self._metrics: Dict[str, float] = {}

        # Register default rules
        self._register_defaults()

    def _register_defaults(self):
        """Register default alert rules."""
        defaults = [
            AlertRule(
                id="peers_low",
                name="Peer count below 2",
                metric="peers_connected",
                operator="<",
                threshold=2,
                severity=AlertSeverity.CRITICAL,
                cooldown=300,
            ),
            AlertRule(
                id="transport_errors",
                name="Transport errors detected",
                metric="transport_errors",
                operator=">",
                threshold=0,
                severity=AlertSeverity.WARNING,
                cooldown=60,
            ),
            AlertRule(
                id="dedup_cache_large",
                name="Dedup cache size above 500",
                metric="dedup_cache_size",
                operator=">",
                threshold=500,
                severity=AlertSeverity.INFO,
                cooldown=600,
            ),
        ]
        for rule in defaults:
            self._rules[rule.id] = rule

    def add_rule(self, rule: AlertRule) -> bool:
        """Add or update an alert rule."""
        self._rules[rule.id] = rule
        log.info(f"Alert rule added: {rule.id} ({rule.name})")
        return True

    def remove_rule(self, rule_id: str) -> bool:
        """Remove an alert rule."""
        if rule_id in self._rules:
            del self._rules[rule_id]
            log.info(f"Alert rule removed: {rule_id}")
            return True
        return False

    def get_rules(self) -> List[dict]:
        """Get all alert rules as dicts."""
        return [r.to_dict() for r in self._rules.values()]

    def update_metrics(self, metrics: Dict[str, float]):
        """Update the current metrics snapshot."""
        self._metrics = metrics

    def evaluate(self, metrics: Dict[str, float]) -> List[dict]:
        """Synchronously evaluate all rules against given metrics.

        Returns list of fired alert dicts. Does NOT use the callback.
        Useful for testing and for manual evaluation from the health loop.
        """
        self._metrics.update(metrics)
        fired = []
        for rule in self._rules.values():
            if not rule.enabled:
                continue
            value = self._metrics.get(rule.metric)
            if value is None:
                continue
            is_firing = rule.evaluate(value)
            if is_firing and rule.can_fire:
                rule.state = AlertState.FIRING
                rule.last_fired = time.time()
                rule.fire_count += 1
                fired.append({
                    "rule_id": rule.id,
                    "name": rule.name,
                    "message": f"{rule.metric}={value} {rule.operator} {rule.threshold}",
                    "severity": rule.severity.value,
                    "fire_count": rule.fire_count,
                })
            elif not is_firing and rule.state == AlertState.FIRING:
                rule.state = AlertState.RESOLVED
        return fired

    async def start(self):
        """Start the alert evaluation loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._eval_loop())
        log.info(f"AlertManager started — {len(self._rules)} rules, check every {self._check_interval}s")

    async def stop(self):
        """Stop the alert evaluation loop."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        log.info("AlertManager stopped")

    async def _eval_loop(self):
        """Periodically evaluate all rules against current metrics."""
        while self._running:
            try:
                await asyncio.sleep(self._check_interval)
                if not self._running:
                    break
                await self._evaluate_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Alert eval loop error: {e}")

    async def _evaluate_all(self):
        """Evaluate all enabled rules against current metrics."""
        for rule in self._rules.values():
            if not rule.enabled:
                continue

            value = self._metrics.get(rule.metric)
            if value is None:
                continue  # Metric not available

            is_firing = rule.evaluate(value)

            if is_firing and rule.state != AlertState.FIRING:
                # State transition: OK → FIRING
                rule.state = AlertState.FIRING
                rule.last_fired = time.time()
                rule.fire_count += 1
                log.warning(f"ALERT FIRING: {rule.name} — {rule.metric}={value} {rule.operator} {rule.threshold}")
                if self._callback:
                    try:
                        await self._callback(rule, value)
                    except Exception as e:
                        log.error(f"Alert callback error: {e}")

            elif is_firing and rule.state == AlertState.FIRING and rule.can_fire:
                # Still firing, but cooldown expired — re-fire
                rule.last_fired = time.time()
                rule.fire_count += 1
                log.warning(f"ALERT RE-FIRE: {rule.name} — {rule.metric}={value} (fire #{rule.fire_count})")
                if self._callback:
                    try:
                        await self._callback(rule, value)
                    except Exception as e:
                        log.error(f"Alert callback error: {e}")

            elif not is_firing and rule.state == AlertState.FIRING:
                # State transition: FIRING → RESOLVED
                rule.state = AlertState.RESOLVED
                log.info(f"ALERT RESOLVED: {rule.name} — {rule.metric}={value}")
                # Don't fire callback on resolve (Zsolt preference: no success alerts)

    def get_status(self) -> dict:
        """Get alert manager status — all rules and their states."""
        return {
            "running": self._running,
            "total_rules": len(self._rules),
            "firing": sum(1 for r in self._rules.values() if r.state == AlertState.FIRING),
            "rules": [r.to_dict() for r in self._rules.values()],
        }