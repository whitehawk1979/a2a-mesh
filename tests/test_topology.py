"""Test core.topology — Zigbee-inspired address assignment"""
import pytest
from a2a_mesh.core.topology import AddressManager, NodeRole, MeshAddress


class TestAddressManager:
    """Test Zigbee-inspired address assignment."""

    def setup_method(self):
        self.am = AddressManager(max_children=20, max_routers=6, max_depth=5)

    def test_coordinator_address(self):
        addr = self.am.assign_address("nova", NodeRole.COORDINATOR)
        assert addr.short == 0
        assert addr.role == NodeRole.COORDINATOR
        assert addr.depth == 0

    def test_router_address_assignment(self):
        self.am.assign_address("coordinator", NodeRole.COORDINATOR)
        addr = self.am.assign_address("morzsa", NodeRole.ROUTER)
        assert addr.short > 0
        assert addr.role == NodeRole.ROUTER
        assert addr.depth == 1

    def test_multiple_router_addresses(self):
        self.am.assign_address("coordinator", NodeRole.COORDINATOR)
        addresses = []
        for i in range(3):
            addr = self.am.assign_address(f"router_{i}", NodeRole.ROUTER)
            addresses.append(addr.short)
        # All addresses should be unique
        assert len(set(addresses)) == 3
        assert all(a > 0 for a in addresses)

    def test_end_device_address(self):
        self.am.assign_address("coordinator", NodeRole.COORDINATOR)
        addr = self.am.assign_address("device1", NodeRole.END_DEVICE)
        assert addr.role == NodeRole.END_DEVICE

    def test_address_uniqueness(self):
        self.am.assign_address("coordinator", NodeRole.COORDINATOR)
        addrs = set()
        for i in range(10):
            addr = self.am.assign_address(f"node_{i}", NodeRole.ROUTER)
            assert addr.short not in addrs, f"Duplicate address: {addr.short}"
            addrs.add(addr.short)

    def test_mesh_address_creation(self):
        addr = MeshAddress(short=0x0001, extended="morzsa", role=NodeRole.ROUTER)
        assert addr.short == 0x0001
        assert addr.role == NodeRole.ROUTER
        assert addr.extended == "morzsa"