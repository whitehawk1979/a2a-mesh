"""A2A Mesh Tree Router — Zigbee-inspired hierarchical tree routing.

Routes messages through the mesh using tree topology:
- Directed messages: follow tree path (O(depth) hops)
- Broadcast messages: flood to all children + parent
- Hybrid mode: tree for unicast, flood for broadcast/discovery
- Sleepy end device support: buffer messages for offline children
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict

from .message import A2AMessage, SendResult
from .topology import MeshAddress, NodeRole, AddressManager
from .dedup import DedupCache

log = logging.getLogger("a2a_mesh.tree_router")


class TreeRouter:
    """Zigbee-inspired hierarchical tree router.
    
    Routes messages through the mesh using the tree topology defined
    by AddressManager. Supports three routing modes:
    
    - FLOOD: Broadcast to all peers (current behavior)
    - TREE: Route through tree hierarchy (Zigbee-style)
    - HYBRID: Tree for directed, flood for broadcast (recommended)
    """

    def __init__(self, local_address: MeshAddress,
                 address_manager: AddressManager,
                 dedup_cache: Optional[DedupCache] = None):
        self.local_address = local_address
        self.address_manager = address_manager
        self.dedup = dedup_cache or DedupCache()

        # Route cache: dest_short -> next_hop_short (AODV-like discovery)
        self._route_cache: Dict[int, int] = {}
        self._route_cache_ttl: Dict[int, float] = {}

        # Message buffer for sleepy end devices
        # child_short -> list of (timestamp, A2AMessage)
        self._message_buffer: Dict[int, List[Tuple[float, A2AMessage]]] = defaultdict(list)
        self._buffer_max_size: int = 1000
        self._buffer_ttl: int = 86400  # 24 hours

        # Peer connections: short_addr -> transport_name
        self._peer_transports: Dict[int, str] = {}

        # Statistics
        self._stats = {
            "tree_routes": 0,
            "flood_routes": 0,
            "buffered_messages": 0,
            "delivered_from_buffer": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }

    def route_message(self, message: A2AMessage,
                      routing_mode: str = "hybrid") -> List[int]:
        """Determine next hop(s) for a message.
        
        Args:
            message: The message to route
            routing_mode: "flood", "tree", or "hybrid"
            
        Returns:
            List of short addresses to forward the message to
        """
        dst = message.recipient

        # Dedup check
        if self.dedup.check_and_add(message.id):
            return []  # Already processed

        # Broadcast message
        if message.is_broadcast() or message.recipient == "broadcast":
            if routing_mode == "tree":
                return self._tree_broadcast()
            else:
                return self._flood_route()

        # Directed message
        if routing_mode == "flood":
            self._stats["flood_routes"] += 1
            return self._flood_route()

        # Try tree routing first
        tree_hops = self._tree_route(message)
        if tree_hops is not None:
            self._stats["tree_routes"] += 1
            return tree_hops

        # Tree routing failed — fall back to flood
        self._stats["flood_routes"] += 1
        return self._flood_route()

    def _tree_route(self, message: A2AMessage) -> Optional[List[int]]:
        """Route through tree hierarchy. Returns None if destination unknown."""
        dst_name = message.recipient

        # Check if destination is in address table
        dst_addr = self.address_manager.get_address(dst_name)
        if dst_addr is None:
            # Destination not in address table — try route cache
            return self._try_route_cache(dst_name)

        dst_short = dst_addr.short

        # Is this message for me?
        if dst_short == self.local_address.short:
            return []  # Delivered locally

        # Is destination in my subtree?
        if self._is_my_child(dst_short):
            # Forward directly to child
            return [dst_short]

        # Is destination an ancestor (parent/grandparent)?
        if self._is_my_ancestor(dst_short):
            # Forward to parent
            if self.local_address.parent_short is not None:
                return [self.local_address.parent_short]
            return None  # I'm coordinator, can't go up

        # Destination is in a different subtree — route via parent
        # (or via route cache if we have a better path)
        cached_next_hop = self._route_cache.get(dst_short)
        if cached_next_hop is not None:
            self._stats["cache_hits"] += 1
            return [cached_next_hop]

        # No cached route — send to parent
        self._stats["cache_misses"] += 1
        if self.local_address.parent_short is not None:
            return [self.local_address.parent_short]

        # I'm coordinator — forward to appropriate subtree
        for child in self.address_manager.get_children(self.local_address.short):
            if dst_addr.is_descendant_of(child.short):
                return [child.short]

        # No path found
        return None

    def _tree_broadcast(self) -> List[int]:
        """Tree broadcast: send to all children + parent."""
        hops = []

        # Send to all children
        children = self.address_manager.get_children(self.local_address.short)
        for child in children:
            # Only send to routers and awake end devices
            if child.is_router or True:  # Send to all for now
                hops.append(child.short)

        # Send to parent (so it can relay to other subtrees)
        if self.local_address.parent_short is not None:
            hops.append(self.local_address.parent_short)

        return hops

    def _flood_route(self) -> List[int]:
        """Flood to all known peers."""
        all_addrs = list(self.address_manager.assigned.values())
        hops = []
        for addr in all_addrs:
            if addr.short != self.local_address.short:
                hops.append(addr.short)
        return hops

    def _try_route_cache(self, dst_name: str) -> Optional[List[int]]:
        """Try to find destination in route cache."""
        # Route cache maps dest_name -> next_hop_short
        # This is populated by AODV-like route discovery
        # For now, return None (will be implemented with AODV)
        return None

    def _is_my_child(self, short_addr: int) -> bool:
        """Check if a short address is one of my direct children."""
        children = self.address_manager.get_children(self.local_address.short)
        return any(c.short == short_addr for c in children)

    def _is_my_ancestor(self, short_addr: int) -> bool:
        """Check if a short address is my ancestor (parent, grandparent, etc.)."""
        if self.local_address.parent_short is None:
            return False
        # Walk up the tree
        current = self.local_address.parent_short
        max_depth = self.address_manager.max_depth
        for _ in range(max_depth):
            if current == short_addr:
                return True
            parent_addr = self.address_manager.get_by_short(current)
            if parent_addr is None or parent_addr.parent_short is None:
                break
            current = parent_addr.parent_short
        return False

    # --- Sleepy End Device Support ---

    def buffer_message(self, child_short: int, message: A2AMessage) -> bool:
        """Buffer a message for a sleepy end device child.
        
        The child will retrieve this message when it polls.
        
        Args:
            child_short: Short address of the child
            message: Message to buffer
            
        Returns:
            True if buffered, False if buffer full
        """
        if len(self._message_buffer.get(child_short, [])) >= self._buffer_max_size:
            log.warning(f"Buffer full for child 0x{child_short:04X}, dropping oldest")
            self._message_buffer[child_short].pop(0)

        self._message_buffer[child_short].append((time.time(), message))
        self._stats["buffered_messages"] += 1
        return True

    def get_buffered_messages(self, child_short: int) -> List[A2AMessage]:
        """Retrieve buffered messages for a child that just polled.
        
        Also removes expired messages (TTL-based cleanup).
        """
        if child_short not in self._message_buffer:
            return []

        now = time.time()
        buffered = self._message_buffer[child_short]

        # Remove expired messages
        valid = [(ts, msg) for ts, msg in buffered if now - ts < self._buffer_ttl]
        self._message_buffer[child_short] = valid

        # Return all valid messages
        messages = [msg for _, msg in valid]
        self._message_buffer[child_short] = []  # Clear after delivery

        self._stats["delivered_from_buffer"] += len(messages)
        return messages

    def get_buffer_size(self, child_short: int) -> int:
        """Return number of buffered messages for a child."""
        return len(self._message_buffer.get(child_short, []))

    # --- Route Cache ---

    def update_route_cache(self, dest_short: int, next_hop_short: int,
                           ttl: int = 300):
        """Add or update a route cache entry (AODV-like)."""
        self._route_cache[dest_short] = next_hop_short
        self._route_cache_ttl[dest_short] = time.time() + ttl

    def expire_route_cache(self):
        """Remove expired route cache entries."""
        now = time.time()
        expired = [
            dest for dest, ttl in self._route_cache_ttl.items()
            if now > ttl
        ]
        for dest in expired:
            self._route_cache.pop(dest, None)
            self._route_cache_ttl.pop(dest, None)

    # --- Statistics ---

    def get_stats(self) -> dict:
        """Return routing statistics."""
        return {
            **self._stats,
            "local_address": self.local_address.to_dict(),
            "route_cache_size": len(self._route_cache),
            "buffered_children": len(self._message_buffer),
            "total_buffered": sum(len(v) for v in self._message_buffer.values()),
        }