"""
Coordinator Election & Failover for A2A Mesh.

Zigbee-inspired: if the coordinator becomes unreachable, the senior-most
router (lowest short_addr among routers) takes over as acting coordinator.

Election rules:
- Only routers can become acting coordinator (not end devices)
- Seniority = lowest short_addr among routers
- Coordinator heartbeat timeout = 3x heartbeat_interval (default: 15 min)
- Election takes ~5 seconds (wait + claim)
- When original coordinator returns, it reclaims the role gracefully

States:
  - COORDINATOR_ACTIVE: coordinator is alive and well
  - COORDINATOR_SUSPECTED: missed 1-2 heartbeats (warning)
  - COORDINATOR_DOWN: missed 3+ heartbeats → election triggered
  - ELECTION_IN_PROGRESS: routers claiming coordinator role
  - ACTING_COORDINATOR: a router has taken over
"""

import time
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


class CoordinatorState(Enum):
    """Coordinator health states."""
    ACTIVE = "active"           # Coordinator is alive
    SUSPECTED = "suspected"     # Missed 1-2 heartbeats
    DOWN = "down"               # Missed 3+ heartbeats
    ELECTION = "election"       # Election in progress
    ACTING = "acting"           # Acting coordinator (failover)


@dataclass
class CoordinatorInfo:
    """Tracks coordinator node info and health."""
    node_name: str
    short_addr: int
    last_heartbeat: float = 0.0
    state: CoordinatorState = CoordinatorState.ACTIVE
    is_original: bool = True     # True = designated coordinator (0x0000)
    is_acting: bool = False      # True = currently acting as coordinator


@dataclass
class ElectionConfig:
    """Election configuration."""
    heartbeat_interval: float = 300.0    # 5 minutes (same as mesh heartbeat)
    suspect_threshold: float = 600.0     # 2 missed heartbeats = 10 min
    down_threshold: float = 900.0        # 3 missed heartbeats = 15 min
    election_timeout: float = 5.0        # Wait 5 seconds for claims
    reclaim_delay: float = 30.0          # Original coordinator waits before reclaiming


class CoordinatorElection:
    """
    Handles coordinator failover election in the mesh.

    Uses a simple seniority-based election: the router with the lowest
    short_addr becomes the acting coordinator when the real coordinator
    is down. This avoids complex voting while guaranteeing deterministic
    results.
    """

    def __init__(self, self_name: str, self_addr: int, self_role: str, config: ElectionConfig = None):
        self.self_name = self_name
        self.self_addr = self_addr
        self.self_role = self_role
        self.config = config or ElectionConfig()
        self.coordinator: Optional[CoordinatorInfo] = None
        self.election_start_time: float = 0.0
        self.is_acting_coordinator: bool = False
        self._last_check_time: float = time.time()

    def register_coordinator(self, node_name: str, short_addr: int, is_original: bool = True):
        """Register or update the coordinator info."""
        now = time.time()
        if self.coordinator and self.coordinator.node_name == node_name:
            # Update heartbeat
            self.coordinator.last_heartbeat = now
            if self.coordinator.state in (CoordinatorState.SUSPECTED, CoordinatorState.DOWN):
                logger.info(f"Coordinator {node_name} (0x{short_addr:04X}) heartbeat recovered")
                self.coordinator.state = CoordinatorState.ACTIVE
        else:
            self.coordinator = CoordinatorInfo(
                node_name=node_name,
                short_addr=short_addr,
                last_heartbeat=now,
                state=CoordinatorState.ACTIVE,
                is_original=is_original,
            )
            logger.info(f"Registered coordinator: {node_name} (0x{short_addr:04X}), original={is_original}")

    def check_coordinator_health(self, known_routers: list) -> CoordinatorState:
        """
        Check coordinator health and trigger election if needed.

        Args:
            known_routers: List of (node_name, short_addr) tuples of routers
                          sorted by short_addr (seniority order)

        Returns:
            Current CoordinatorState
        """
        if not self.coordinator:
            # No coordinator registered yet
            return CoordinatorState.DOWN

        now = time.time()
        age = now - self.coordinator.last_heartbeat

        if age < self.config.suspect_threshold:
            # Coordinator is healthy
            if self.coordinator.state != CoordinatorState.ACTIVE:
                logger.info(f"Coordinator {self.coordinator.node_name} recovered (age={age:.0f}s)")
            self.coordinator.state = CoordinatorState.ACTIVE
            self.is_acting_coordinator = False
            return CoordinatorState.ACTIVE

        elif age < self.config.down_threshold:
            # Suspected — missed heartbeats
            if self.coordinator.state == CoordinatorState.ACTIVE:
                logger.warning(f"Coordinator {self.coordinator.node_name} SUSPECTED (age={age:.0f}s)")
            self.coordinator.state = CoordinatorState.SUSPECTED
            return CoordinatorState.SUSPECTED

        else:
            # Down — election needed
            if self.coordinator.state != CoordinatorState.DOWN:
                logger.error(f"Coordinator {self.coordinator.node_name} DOWN (age={age:.0f}s)")
            self.coordinator.state = CoordinatorState.DOWN
            return CoordinatorState.DOWN

    def should_initiate_election(self, known_routers: list) -> bool:
        """
        Determine if this node should initiate or participate in election.

        Returns True if this node is the senior-most router (lowest addr)
        and the coordinator is down.
        """
        if not self.coordinator or self.coordinator.state != CoordinatorState.DOWN:
            return False

        if self.self_role not in ("router", "coordinator"):
            return False

        # Find senior-most router (lowest addr, excluding the down coordinator)
        if not known_routers:
            # Only this node — it becomes acting coordinator
            return True

        # Sort by short_addr — lowest addr is senior
        senior_router = min(known_routers, key=lambda r: r[1])

        # This node is senior if its addr is lowest
        return self.self_addr <= senior_router[1]

    def initiate_election(self) -> dict:
        """
        Initiate coordinator election. Returns election claim message.

        The senior-most router claims the acting coordinator role.
        """
        self.election_start_time = time.time()
        self.is_acting_coordinator = True

        claim = {
            "type": "coordinator_claim",
            "node_name": self.self_name,
            "short_addr": self.self_addr,
            "original_coordinator": self.coordinator.node_name if self.coordinator else "unknown",
            "timestamp": time.time(),
            "claim_reason": "coordinator_down",
        }

        logger.info(f"🏛️ Election: {self.self_name} (0x{self.self_addr:04X}) claiming acting coordinator")
        return claim

    def handle_election_claim(self, claim: dict) -> bool:
        """
        Handle an election claim from another node.

        Returns True if we accept the claim (they are senior to us),
        False if we reject it (we are more senior).
        """
        claimer_addr = claim.get("short_addr", 0xFFFF)
        claimer_name = claim.get("node_name", "unknown")

        # If we're also trying to be acting coordinator, compare seniority
        if self.is_acting_coordinator and self.self_addr < claimer_addr:
            logger.info(f"Election: rejecting claim from {claimer_name} (0x{claimer_addr:04X}) — we are more senior")
            return False

        # Accept the claim — they are senior
        self.is_acting_coordinator = False
        logger.info(f"Election: accepting claim from {claimer_name} (0x{claimer_addr:04X}) as acting coordinator")

        # Update coordinator info to point to acting coordinator
        self.coordinator = CoordinatorInfo(
            node_name=claimer_name,
            short_addr=claimer_addr,
            last_heartbeat=time.time(),
            state=CoordinatorState.ACTING,
            is_original=False,
            is_acting=True,
        )

        return True

    def handle_coordinator_return(self, node_name: str, short_addr: int) -> dict:
        """
        Handle the original coordinator returning after failover.

        The original coordinator (0x0000) reclaims its role after a
        brief delay to ensure network stability.
        """
        logger.info(f"🏛️ Original coordinator {node_name} (0x{short_addr:04X}) returning")

        # Stand down as acting coordinator
        self.is_acting_coordinator = False

        # Update coordinator info
        self.coordinator = CoordinatorInfo(
            node_name=node_name,
            short_addr=short_addr,
            last_heartbeat=time.time(),
            state=CoordinatorState.ACTIVE,
            is_original=True,
            is_acting=False,
        )

        return {
            "type": "coordinator_return",
            "node_name": node_name,
            "short_addr": short_addr,
            "previous_acting": self.self_name,
            "timestamp": time.time(),
        }

    def get_status(self) -> dict:
        """Get current election/coordinator status."""
        return {
            "coordinator": {
                "node_name": self.coordinator.node_name if self.coordinator else None,
                "short_addr": f"0x{self.coordinator.short_addr:04X}" if self.coordinator else None,
                "state": self.coordinator.state.value if self.coordinator else "none",
                "is_original": self.coordinator.is_original if self.coordinator else None,
                "last_heartbeat_age": f"{time.time() - self.coordinator.last_heartbeat:.0f}s" if self.coordinator else None,
            },
            "self": {
                "name": self.self_name,
                "addr": f"0x{self.self_addr:04X}",
                "role": self.self_role,
                "is_acting_coordinator": self.is_acting_coordinator,
            },
            "election": {
                "in_progress": self.coordinator.state == CoordinatorState.ELECTION if self.coordinator else False,
                "start_time": self.election_start_time,
            },
        }