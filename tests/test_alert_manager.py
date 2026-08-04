"""Test core.alert_manager — Custom alert rules evaluation."""
import pytest
import time
from a2a_mesh.core.alert_manager import AlertManager, AlertRule, AlertSeverity


class TestAlertRule:
    def test_defaults(self):
        rule = AlertRule(id="test", name="Test", metric="peers_connected", operator="<", threshold=1)
        assert rule.severity == AlertSeverity.WARNING
        assert rule.cooldown == 300
        assert rule.enabled is True

    def test_severity_levels(self):
        assert AlertSeverity("critical") == AlertSeverity.CRITICAL
        assert AlertSeverity("warning") == AlertSeverity.WARNING
        assert AlertSeverity("info") == AlertSeverity.INFO

    def test_to_dict(self):
        rule = AlertRule(id="r1", name="R1", metric="errors", operator=">", threshold=10)
        d = rule.to_dict()
        assert d["id"] == "r1"
        assert d["metric"] == "errors"
        assert d["threshold"] == 10


class TestAlertManager:
    def test_default_rules_loaded(self):
        mgr = AlertManager()
        assert len(mgr._rules) > 0
        assert "peers_low" in mgr._rules

    def test_add_custom_rule(self):
        mgr = AlertManager()
        rule = AlertRule(id="custom", name="Custom", metric="transport_errors", operator=">", threshold=5)
        mgr.add_rule(rule)
        assert "custom" in mgr._rules

    def test_remove_rule(self):
        mgr = AlertManager()
        mgr.add_rule(AlertRule(id="temp", name="Temp", metric="x", operator=">", threshold=0))
        assert mgr.remove_rule("temp") is True
        assert mgr.remove_rule("nonexistent") is False

    def test_evaluate_below_threshold(self):
        mgr = AlertManager()
        # peers_low: metric=peers_connected, operator=<, threshold=2
        fired = mgr.evaluate({"peers_connected": 1})
        assert len(fired) == 1
        assert fired[0]["rule_id"] == "peers_low"

    def test_evaluate_above_threshold_ok(self):
        mgr = AlertManager()
        fired = mgr.evaluate({"peers_connected": 2})
        assert len(fired) == 0

    def test_evaluate_multiple_rules(self):
        mgr = AlertManager()
        mgr.add_rule(AlertRule(id="errors_high", name="Errors High",
                               metric="transport_errors", operator=">", threshold=5))
        fired = mgr.evaluate({"peers_connected": 1, "transport_errors": 10})
        assert len(fired) == 3  # peers_low + transport_errors (default) + errors_high

    def test_cooldown_prevents_refire(self):
        mgr = AlertManager()
        fired1 = mgr.evaluate({"peers_connected": 1})
        assert len(fired1) == 1
        # Second evaluation within cooldown should not fire
        fired2 = mgr.evaluate({"peers_connected": 1})
        assert len(fired2) == 0

    def test_disabled_rule_doesnt_fire(self):
        mgr = AlertManager()
        mgr._rules["peers_low"].enabled = False
        fired = mgr.evaluate({"peers_connected": 1})
        assert len(fired) == 0

    def test_get_status(self):
        mgr = AlertManager()
        status = mgr.get_status()
        assert "rules" in status
        assert "total_rules" in status
        assert isinstance(status["rules"], list)