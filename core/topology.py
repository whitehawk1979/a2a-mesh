"""A2A Mesh Topology — Zigbee-inspired coordinator/router/end_device roles.

Provides hierarchical addressing, tree routing, trust center, and
sleepy end device support for agent mesh networks.

Roles:
  COORDINATOR — Network root, trust center, address assigner (0x0000)
  ROUTER — Always-on, routes messages, buffers for sleepy children
  END_DEVICE — Lightweight, can disconnect, communicates via parent

Address scheme (Cskip-based):
  Coordinator:   0x0000
  Router 1:       0x0001 (depth 1)
  Router 2:       0x0002 (depth 1)
  End Device 1:   0x0100 (depth 2, under Router 1)
  End Device 2:   0x0101 (depth 2, under Router 1)
"""

import enum
import hashlib
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

log = logging.getLogger("a2a_mesh.topology")



class NodeRole(enum.Enum):
    """Zigbee-inspired node roles for A2A mesh."""
    COORDINATOR = "coordinator"   # Network root, trust center, address assigner
    ROUTER = "router"             # Always-on, routes messages, can have children
    END_DEVICE = "end_device"     # Lightweight, can disconnect, communicates via parent


@dataclass
class MeshAddress:
    """Zigbee-inspired hierarchical address for A2A nodes.
    
    Short address: hierarchical, deterministic from tree position
    Extended address: UUID, stable across re-associations
    """
    short: int              # 16-bit tree address (0x0000 for coordinator)
    extended: str           # UUID, stable identity
    role: NodeRole = NodeRole.END_DEVICE
    depth: int = 0          # Tree depth (0 = coordinator)
    parent_short: Optional[int] = None   # Parent's short address
    joined_at: float = 0    # Timestamp when joined

    @property
    def is_coordinator(self) -> bool:
        return self.role == NodeRole.COORDINATOR

    @property
    def is_router(self) -> bool:
        return self.role in (NodeRole.COORDINATOR, NodeRole.ROUTER)

    @property
    def is_end_device(self) -> bool:
        return self.role == NodeRole.END_DEVICE

    def is_descendant_of(self, other_short: int, cskip: int = 256) -> bool:
        """Check if this address is in the subtree of another node.
        
        Uses Cskip-based subtree range calculation.
        """
        if other_short == 0:
            return self.short > 0  # All nodes are descendants of coordinator
        parent_range_start = other_short
        parent_range_end = other_short + cskip
        return parent_range_start <= self.short < parent_range_end

    def to_dict(self) -> dict:
        return {
            "short": self.short,
            "extended": self.extended,
            "role": self.role.value,
            "depth": self.depth,
            "parent_short": self.parent_short,
            "joined_at": self.joined_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'MeshAddress':
        return cls(
            short=d["short"],
            extended=d["extended"],
            role=NodeRole(d.get("role", "end_device")),
            depth=d.get("depth", 0),
            parent_short=d.get("parent_short"),
            joined_at=d.get("joined_at", 0),
        )

    def __hash__(self):
        return hash(self.extended)

    def __eq__(self, other):
        if isinstance(other, MeshAddress):
            return self.extended == other.extended
        return False

    def __repr__(self):
        role_char = {"coordinator": "C", "router": "R", "end_device": "E"}
        return f"MeshAddr(0x{self.short:04X}/{role_char[self.role.value]}/{self.extended[:8]})"


class AddressManager:
    """Manages hierarchical address assignment (Coordinator role).
    
    Uses Cskip-based addressing scheme from Zigbee specification:
    - Cm: max children per node
    - Rm: max router children per node  
    - Lm: max tree depth
    
    Address ranges:
    - Coordinator: 0x0000
    - Routers: sequential from 0x0001
    - End devices: start after router range
    """

    def __init__(self, max_children: int = 20, max_routers: int = 6,
                 max_depth: int = 5):
        self.max_children = max_children   # Cm
        self.max_routers = max_routers     # Rm
        self.max_depth = max_depth         # Lm

        # Address counters
        self._next_router_addr = 1
        self._next_end_device_addr = max_routers * (2 ** max_depth) + 1

        # Registry: extended_uuid -> MeshAddress
        self.assigned: Dict[str, MeshAddress] = {}

        # Reverse: short_addr -> extended_uuid
        self._short_to_extended: Dict[int, str] = {}

        # Children registry: parent_short -> list of child shorts
        self._children: Dict[int, List[int]] = {}

    def compute_cskip(self, depth: int) -> int:
        """Compute Cskip(d) — number of addresses in each subtree at depth d.
        
        Cskip(d) = {
            1 + Cm * (Lm - d - 1)          if Rm = 1
            (1 + Cm - Rm * Cskip(d+1)) / (1 - Rm)  if Rm ≠ 1
            0                               if d = Lm
        }
        """
        if depth >= self.max_depth:
            return 0

        if self.max_routers == 1:
            return 1 + self.max_children * (self.max_depth - depth - 1)

        cskip_next = self.compute_cskip(depth + 1)
        if cskip_next == 0:
            return self.max_children + 1

        return (1 + self.max_children - self.max_routers * cskip_next) // (1 - self.max_routers)

    def assign_address(self, extended_uuid: str, role: NodeRole,
                       parent_short: int = 0) -> MeshAddress:
        """Assign a hierarchical address to a joining node.
        
        Args:
            extended_uuid: Stable UUID of the node
            role: Node role (coordinator, router, end_device)
            parent_short: Parent's short address (0 for coordinator)
            
        Returns:
            MeshAddress with assigned short address
            
        Raises:
            ValueError: If address space is exhausted or UUID already assigned
        """
        # Check if already assigned
        if extended_uuid in self.assigned:
            existing = self.assigned[extended_uuid]
            if existing.role == role:
                return existing  # Return existing assignment
            # Role changed — release old and reassign
            self.release_address(extended_uuid)

        if role == NodeRole.COORDINATOR:
            addr = MeshAddress(
                short=0,
                extended=extended_uuid,
                role=role,
                depth=0,
                parent_short=None,
                joined_at=time.time(),
            )
        elif role == NodeRole.ROUTER:
            addr = MeshAddress(
                short=self._next_router_addr,
                extended=extended_uuid,
                role=role,
                depth=self._get_depth_for_address(self._next_router_addr, parent_short),
                parent_short=parent_short,
                joined_at=time.time(),
            )
            self._next_router_addr += 1
        else:  # END_DEVICE
            addr = MeshAddress(
                short=self._next_end_device_addr,
                extended=extended_uuid,
                role=role,
                depth=self._get_depth_for_address(self._next_end_device_addr, parent_short),
                parent_short=parent_short,
                joined_at=time.time(),
            )
            self._next_end_device_addr += 1

        # Register
        self.assigned[extended_uuid] = addr
        self._short_to_extended[addr.short] = extended_uuid
        if parent_short not in self._children:
            self._children[parent_short] = []
        self._children[parent_short].append(addr.short)

        return addr

    def release_address(self, extended_uuid: str) -> Optional[MeshAddress]:
        """Release an address when a node leaves the network.
        
        Returns the released MeshAddress, or None if not found.
        """
        if extended_uuid not in self.assigned:
            return None

        addr = self.assigned.pop(extended_uuid)
        self._short_to_extended.pop(addr.short, None)

        # Remove from parent's children list
        if addr.parent_short is not None and addr.parent_short in self._children:
            self._children[addr.parent_short] = [
                c for c in self._children[addr.parent_short] if c != addr.short
            ]

        return addr

    def get_address(self, extended_uuid: str) -> Optional[MeshAddress]:
        """Look up address by extended UUID."""
        return self.assigned.get(extended_uuid)

    def get_by_short(self, short: int) -> Optional[MeshAddress]:
        """Look up address by short address."""
        extended = self._short_to_extended.get(short)
        if extended:
            return self.assigned.get(extended)
        return None

    def get_children(self, parent_short: int) -> List[MeshAddress]:
        """Get all children of a node."""
        child_shorts = self._children.get(parent_short, [])
        return [
            self.assigned[self._short_to_extended[cs]]
            for cs in child_shorts
            if cs in self._short_to_extended
            and self._short_to_extended[cs] in self.assigned
        ]

    def get_routers(self) -> List[MeshAddress]:
        """Get all router nodes (including coordinator)."""
        return [a for a in self.assigned.values() if a.is_router]

    def get_end_devices(self) -> List[MeshAddress]:
        """Get all end device nodes."""
        return [a for a in self.assigned.values() if a.is_end_device]

    def get_network_stats(self) -> dict:
        """Return network topology statistics."""
        return {
            "total_nodes": len(self.assigned),
            "coordinators": sum(1 for a in self.assigned.values() if a.role == NodeRole.COORDINATOR),
            "routers": sum(1 for a in self.assigned.values() if a.role == NodeRole.ROUTER),
            "end_devices": sum(1 for a in self.assigned.values() if a.role == NodeRole.END_DEVICE),
            "next_router_addr": self._next_router_addr,
            "next_end_device_addr": self._next_end_device_addr,
            "max_children": self.max_children,
            "max_routers": self.max_routers,
            "max_depth": self.max_depth,
        }

    def _get_depth_for_address(self, short_addr: int, parent_short: int) -> int:
        """Calculate tree depth for a new address."""
        if parent_short == 0:
            return 1  # Direct child of coordinator
        parent = self.get_by_short(parent_short)
        if parent:
            return parent.depth + 1
        return 1  # Default to depth 1

    def to_dict(self) -> dict:
        """Serialize address table for persistence."""
        return {
            "max_children": self.max_children,
            "max_routers": self.max_routers,
            "max_depth": self.max_depth,
            "next_router_addr": self._next_router_addr,
            "next_end_device_addr": self._next_end_device_addr,
            "assigned": {uuid: addr.to_dict() for uuid, addr in self.assigned.items()},
            "children": {str(k): v for k, v in self._children.items()},
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'AddressManager':
        """Deserialize address table from persistence."""
        mgr = cls(
            max_children=d.get("max_children", 20),
            max_routers=d.get("max_routers", 6),
            max_depth=d.get("max_depth", 5),
        )
        mgr._next_router_addr = d.get("next_router_addr", 1)
        mgr._next_end_device_addr = d.get("next_end_device_addr", 256)
        mgr.assigned = {
            uuid: MeshAddress.from_dict(addr_d)
            for uuid, addr_d in d.get("assigned", {}).items()
        }
        mgr._short_to_extended = {
            addr.short: uuid for uuid, addr in mgr.assigned.items()
        }
        mgr._children = {
            int(k): v for k, v in d.get("children", {}).items()
        }
        return mgr