"""Test core.election — Coordinator election"""
import pytest
from a2a_mesh.core.election import CoordinatorElection, ElectionConfig, CoordinatorState
from a2a_mesh.core.topology import NodeRole


class TestCoordinatorElection:
    """Test Zigbee-inspired coordinator election."""

    def test_election_config_defaults(self):
        config = ElectionConfig()
        assert config.heartbeat_interval > 0
        assert config.suspect_threshold > 0
        assert config.down_threshold > 0

    def test_election_coordinator_role(self):
        election = CoordinatorElection(
            self_name="nova",
            self_addr=0x0000,
            self_role=NodeRole.COORDINATOR,
            config=ElectionConfig(heartbeat_interval=10),
        )
        # Coordinator should report status
        status = election.get_status()
        assert status is not None

class TestCoordinatorLivenessFix:
    """v0.44.1 regressziós tesztek: coordinator self-claim loop fix."""

    def test_self_coordinator_never_goes_down_while_alive(self):
        """Ha MI vagyunk a coordinator, a last_heartbeat frissítése életben tartja."""
        election = CoordinatorElection(
            self_name="nova",
            self_addr=0x0000,
            self_role=NodeRole.COORDINATOR,
            config=ElectionConfig(heartbeat_interval=10, suspect_threshold=180, down_threshold=420),
        )
        election.register_coordinator("nova", 0x0000, is_original=True)
        # Szimuláljuk a self-liveness refresh-t (node.py monitor loop)
        import time as _t
        election.coordinator.last_heartbeat = _t.time()
        routers = [("nova", 0x0000), ("morzsa", 0x1000)]
        state = election.check_coordinator_health(routers)
        assert state == CoordinatorState.ACTIVE

    def test_junior_claim_rejected_when_live_coordinator_is_self(self):
        """Élő, szeniorabb coordinator elutasítja a junior claimet (split-brain guard)."""
        election = CoordinatorElection(
            self_name="nova",
            self_addr=0x0000,
            self_role=NodeRole.COORDINATOR,
            config=ElectionConfig(heartbeat_interval=10),
        )
        election.register_coordinator("nova", 0x0000, is_original=True)
        claim = {"node_name": "morzsa", "short_addr": 0x1000, "claim_reason": "coordinator_down"}
        assert election.handle_election_claim(claim) is False

    def test_senior_claim_accepted_when_coordinator_is_other(self):
        """Ha más a coordinator és a claimer szeniorabb, a claim elfogadott."""
        election = CoordinatorElection(
            self_name="morzsa",
            self_addr=0x1000,
            self_role=NodeRole.ROUTER,
            config=ElectionConfig(heartbeat_interval=10),
        )
        election.register_coordinator("nova", 0x0000, is_original=True)
        claim = {"node_name": "tor", "short_addr": 0x0001, "claim_reason": "coordinator_down"}
        assert election.handle_election_claim(claim) is True

    def test_peer_heartbeat_refreshes_coordinator_liveness(self):
        """Peer-oldal: a coordinator P2P heartbeatje frissíti az election view-t."""
        import time as _t
        election = CoordinatorElection(
            self_name="morzsa",
            self_addr=0x1000,
            self_role=NodeRole.ROUTER,
            config=ElectionConfig(heartbeat_interval=10, suspect_threshold=180, down_threshold=420),
        )
        election.register_coordinator("nova", 0x0000, is_original=True)
        # 500s-re öregítjük — DOWN lenne
        election.coordinator.last_heartbeat = _t.time() - 500
        routers = []
        state = election.check_coordinator_health(routers)
        assert state == CoordinatorState.DOWN
        # v0.44.1 peer-side refresh (node.py _on_p2p_heartbeat): újraregisztráljuk élőként
        election.register_coordinator("nova", 0x0000, is_original=False)
        state = election.check_coordinator_health(routers)
        assert state == CoordinatorState.ACTIVE
