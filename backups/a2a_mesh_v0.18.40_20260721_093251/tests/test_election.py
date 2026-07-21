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