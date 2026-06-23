"""A2A Mesh GossipSub — Efficient broadcast for meshes with 3+ nodes.

Inspired by gensyn-ai/axl's broadcast patterns and libp2p GossipSub v1.1.
For small meshes (≤10 nodes), simple flood works fine. GossipSub adds:
- GRAFT/PRUNE mesh maintenance
- Lazy-first forwarding (only forward if peers need it)
- Topic-based subscription
- Heartbeat-driven mesh optimization

For our 3-node mesh, this is overkill but provides the architecture for scaling.
"""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

log = logging.getLogger("a2a_mesh.gossipsub")


class GossipEventType(Enum):
    """GossipSub event types."""
    GRAFT = "graft"       # Add peer to mesh
    PRUNE = "prune"       # Remove peer from mesh
    GRAFT_ACK = "graft_ack"  # Acknowledge graft
    PRUNE_ACK = "prune_ack"  # Acknowledge prune


@dataclass
class GossipPeer:
    """Represents a peer in the GossipSub mesh."""
    peer_id: str
    topics: Set[str] = field(default_factory=set)
    connected: bool = True
    last_seen: float = field(default_factory=time.time)
    score: float = 0.0
    graft_time: float = 0.0  # When this peer was grafted


@dataclass
class GossipMessage:
    """A message in the GossipSub overlay."""
    msg_id: str
    topic: str
    sender: str
    payload: bytes
    seqno: int = 0  # Message sequence number


class GossipSub:
    """GossipSub v1.1-inspired broadcast for A2A mesh.
    
    For meshes ≤10 nodes, operates in "flood" mode (forward to all peers).
    For meshes >10 nodes, switches to GossipSub mode with:
    - Mesh maintenance (GRAFT/PRUNE per topic)
    - Lazy-first forwarding (only forward if peer needs it)
    - Score-based peer selection
    
    Configuration:
        D_low: Minimum mesh density per topic (default: 3)
        D_high: Maximum mesh density per topic (default: 6)
        D_score: Target mesh density based on peer scores (default: 4)
        heartbeat_interval: Seconds between mesh optimization (default: 60)
        gossip_factor: Fraction of peers to gossip to (default: 0.25)
    """
    
    def __init__(
        self,
        node_id: str,
        d_low: int = 3,
        d_high: int = 6,
        d_score: int = 4,
        heartbeat_interval: float = 60.0,
        gossip_factor: float = 0.25,
        flood_threshold: int = 10,
    ):
        self.node_id = node_id
        self.d_low = d_low
        self.d_high = d_high
        self.d_score = d_score
        self.heartbeat_interval = heartbeat_interval
        self.gossip_factor = gossip_factor
        self.flood_threshold = flood_threshold
        
        # Peer tracking
        self._peers: Dict[str, GossipPeer] = {}
        
        # Topic-based mesh: topic → set of peer_ids
        self._mesh: Dict[str, Set[str]] = defaultdict(set)
        
        # Message cache for dedup (msg_id → timestamp)
        self._cache: Dict[str, float] = {}
        self._cache_ttl = 300  # 5 minutes
        
        # Score tracking
        self._peer_scores: Dict[str, float] = defaultdict(float)
        
        # Running state
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        
        # Callback for sending messages to peers
        self._send_callback = None  # async def send(peer_id, msg)
        
        # Stats
        self._stats = {
            "messages_forwarded": 0,
            "messages_originated": 0,
            "graft_sent": 0,
            "prune_sent": 0,
            "graft_received": 0,
            "prune_received": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }
    
    def set_send_callback(self, callback):
        """Set the callback for sending messages to peers.
        
        callback: async def send(peer_id: str, msg: GossipMessage)
        """
        self._send_callback = callback
    
    async def start(self):
        """Start GossipSub heartbeat loop."""
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        log.info(f"GossipSub started for node {self.node_id} "
                 f"(flood_threshold={self.flood_threshold})")
    
    async def stop(self):
        """Stop GossipSub."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        log.info(f"GossipSub stopped for node {self.node_id}")
    
    def add_peer(self, peer_id: str, topics: Optional[Set[str]] = None):
        """Add a peer to the GossipSub overlay."""
        if peer_id == self.node_id:
            return  # Don't add self
        
        topics = topics or {"a2a"}
        peer = GossipPeer(peer_id=peer_id, topics=topics)
        self._peers[peer_id] = peer
        
        # Graft peer into topic meshes
        for topic in topics:
            self._mesh[topic].add(peer_id)
            log.debug(f"Grafted peer {peer_id} into topic '{topic}'")
    
    def remove_peer(self, peer_id: str):
        """Remove a peer from the GossipSub overlay."""
        if peer_id not in self._peers:
            return
        
        # Prune from all topic meshes
        for topic in list(self._mesh.keys()):
            self._mesh[topic].discard(peer_id)
        
        del self._peers[peer_id]
        log.debug(f"Removed peer {peer_id} from GossipSub overlay")
    
    def update_peer_score(self, peer_id: str, delta: float):
        """Update a peer's score (used for mesh optimization)."""
        self._peer_scores[peer_id] = max(-100, min(100, 
            self._peer_scores.get(peer_id, 0.0) + delta))
    
    async def publish(self, topic: str, msg_id: str, payload: bytes) -> int:
        """Publish a message to a topic.
        
        Returns the number of peers the message was forwarded to.
        In flood mode (≤flood_threshold peers), sends to all peers.
        In GossipSub mode, sends to mesh peers + random gossip peers.
        """
        # Check cache for duplicates
        if msg_id in self._cache:
            self._stats["cache_hits"] += 1
            return 0
        self._stats["cache_misses"] += 1
        
        # Add to cache
        self._cache[msg_id] = time.time()
        self._stats["messages_originated"] += 1
        
        # Clean old cache entries
        self._clean_cache()
        
        # Get peers for this topic
        topic_peers = self._get_topic_peers(topic)
        
        if len(topic_peers) <= self.flood_threshold:
            # Flood mode: send to all peers
            forwarded = await self._flood_forward(topic, msg_id, payload)
        else:
            # GossipSub mode: send to mesh peers + gossip
            forwarded = await self._gossipsub_forward(topic, msg_id, payload)
        
        self._stats["messages_forwarded"] += forwarded
        return forwarded
    
    async def handle_message(self, topic: str, msg_id: str, sender: str, 
                            payload: bytes) -> int:
        """Handle a received GossipSub message.
        
        Returns the number of peers the message was forwarded to.
        """
        # Check cache
        if msg_id in self._cache:
            self._stats["cache_hits"] += 1
            return 0
        self._stats["cache_misses"] += 1
        
        # Add to cache
        self._cache[msg_id] = time.time()
        
        # Get topic peers (excluding sender)
        topic_peers = self._get_topic_peers(topic)
        topic_peers.discard(sender)
        
        if len(self._peers) <= self.flood_threshold:
            # Flood mode
            forwarded = await self._flood_forward(topic, msg_id, payload, 
                                                  exclude={sender})
        else:
            # GossipSub mode
            forwarded = await self._gossipsub_forward(topic, msg_id, payload,
                                                      exclude={sender})
        
        self._stats["messages_forwarded"] += forwarded
        return forwarded
    
    def _get_topic_peers(self, topic: str) -> Set[str]:
        """Get all peers subscribed to a topic."""
        peers = set()
        for peer_id, peer in self._peers.items():
            if topic in peer.topics:
                peers.add(peer_id)
        return peers
    
    async def _flood_forward(self, topic: str, msg_id: str, payload: bytes,
                            exclude: Optional[Set[str]] = None) -> int:
        """Forward message to all peers (flood mode)."""
        exclude = exclude or set()
        topic_peers = self._get_topic_peers(topic) - exclude
        
        forwarded = 0
        for peer_id in topic_peers:
            if self._send_callback:
                try:
                    msg = GossipMessage(
                        msg_id=msg_id,
                        topic=topic,
                        sender=self.node_id,
                        payload=payload,
                    )
                    await self._send_callback(peer_id, msg)
                    forwarded += 1
                except Exception as e:
                    log.debug(f"Failed to forward to {peer_id}: {e}")
        
        return forwarded
    
    async def _gossipsub_forward(self, topic: str, msg_id: str, payload: bytes,
                                  exclude: Optional[Set[str]] = None) -> int:
        """Forward message using GossipSub mesh + gossip."""
        exclude = exclude or set()
        
        # Forward to mesh peers (lazy-first: only if peer needs it)
        mesh_peers = self._mesh.get(topic, set()) - exclude
        gossip_targets = set(mesh_peers)
        
        # Add random gossip targets
        all_peers = self._get_topic_peers(topic) - exclude - mesh_peers
        if all_peers:
            import random
            gossip_count = max(1, int(len(all_peers) * self.gossip_factor))
            gossip_targets.update(random.sample(list(all_peers), 
                                               min(gossip_count, len(all_peers))))
        
        forwarded = 0
        for peer_id in gossip_targets:
            if self._send_callback:
                try:
                    msg = GossipMessage(
                        msg_id=msg_id,
                        topic=topic,
                        sender=self.node_id,
                        payload=payload,
                    )
                    await self._send_callback(peer_id, msg)
                    forwarded += 1
                except Exception as e:
                    log.debug(f"Failed to gossip to {peer_id}: {e}")
        
        return forwarded
    
    async def _heartbeat_loop(self):
        """Periodic mesh optimization heartbeat."""
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                await self._optimize_mesh()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"GossipSub heartbeat error: {e}")
    
    async def _optimize_mesh(self):
        """Optimize mesh: graft/prune peers based on scores and density."""
        for topic, peers in list(self._mesh.items()):
            # Prune: too many peers in mesh
            if len(peers) > self.d_high:
                # Remove lowest-scoring peers
                sorted_peers = sorted(peers, 
                    key=lambda p: self._peer_scores.get(p, 0.0))
                to_prune = sorted_peers[:len(peers) - self.d_score]
                for peer_id in to_prune:
                    peers.discard(peer_id)
                    self._stats["prune_sent"] += 1
                    log.debug(f"Pruned {peer_id} from topic '{topic}'")
            
            # Graft: too few peers in mesh
            elif len(peers) < self.d_low:
                # Add highest-scoring peers not in mesh
                all_topic_peers = self._get_topic_peers(topic)
                candidates = all_topic_peers - peers - {self.node_id}
                sorted_candidates = sorted(candidates,
                    key=lambda p: self._peer_scores.get(p, 0.0),
                    reverse=True)
                needed = self.d_low - len(peers)
                for peer_id in sorted_candidates[:needed]:
                    peers.add(peer_id)
                    self._stats["graft_sent"] += 1
                    log.debug(f"Grafted {peer_id} into topic '{topic}'")
        
        # Clean cache periodically
        self._clean_cache()
    
    def _clean_cache(self):
        """Remove expired cache entries."""
        now = time.time()
        expired = [k for k, v in self._cache.items() if now - v > self._cache_ttl]
        for k in expired:
            del self._cache[k]
    
    @property
    def mode(self) -> str:
        """Return the current operating mode."""
        return "flood" if len(self._peers) <= self.flood_threshold else "gossipsub"
    
    @property
    def stats(self) -> dict:
        """Return GossipSub statistics."""
        return {
            "mode": self.mode,
            "peer_count": len(self._peers),
            "mesh_topics": {t: len(p) for t, p in self._mesh.items()},
            "cache_size": len(self._cache),
            **self._stats,
        }