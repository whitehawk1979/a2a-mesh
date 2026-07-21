"""A2A Mesh Router — GossipSub/flood routing with TTL and loop prevention."""

import logging
import asyncio
import json
from typing import Dict, List, Optional, Callable
from ..core.message import A2AMessage, SendResult, ProcessResult, A2A_PROTOCOL_VERSION, MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK
from ..core.dedup import DedupCache
from ..core.bounded_queue import BoundedQueue
from ..core.stream_mux import StreamMultiplexer, create_default_mux
from ..core.gossipsub import GossipSub
from ..core.health_scorer import HealthScorer

log = logging.getLogger("a2a_mesh.router")

class MeshRouter:
    """Routes messages through the mesh using flood/gossip protocol.

    For small meshes (<20 nodes): flood to all peers.
    For large meshes (>20 nodes): gossip to K random peers.

    Features:
    - TTL-based message propagation (prevents infinite flood)
    - Dedup cache (prevents processing same message twice)
    - Loop prevention (self-reference, not-for-me, RE-chain)
    - Priority-based routing (P10+ immediate, P1-9 queued)
    - Stream multiplexer (AXL-inspired content-based routing)
    - Bounded queues with oldest-drop overflow protection
    - Protocol versioning for compatibility
    """

    def __init__(self, node_name: str, config=None, local_store=None):
        self.node_name = node_name
        self.config = config
        self.local_store = local_store

        # Transport adapters: name → adapter instance
        self.transports: Dict[str, 'TransportAdapter'] = {}

        # Dedup cache
        self.dedup = DedupCache(
            max_size=config.loop_prevention.dedup_cache_size if config else 5000,
            ttl_seconds=config.loop_prevention.dedup_ttl if config else 300,
        )

        # Loop prevention config
        self.self_reference_filter = config.loop_prevention.self_reference_filter if config else True
        self.not_for_me_filter = config.loop_prevention.not_for_me_filter if config else True
        self.re_chain_limit = config.loop_prevention.re_chain_limit if config else 4

        # Message handlers
        self._handlers: List[Callable] = []

        # Bounded priority queue (AXL-inspired: oldest-drop overflow protection)
        self._inbound_queue = BoundedQueue(capacity=200, name="inbound")
        self._outbound_queue = BoundedQueue(capacity=200, name="outbound")

        # Stream multiplexer (AXL-inspired: content-based routing)
        self._mux = create_default_mux()

        # GossipSub broadcast (AXL-inspired: efficient broadcast for >10 nodes)
        self._gossipsub = GossipSub(node_name)

        # Health Scorer (sushaan-k/a2a-mesh inspired: trust-based agent scoring)
        self._health_scorer = HealthScorer()

        # Connection semaphore for P2P (AXL-inspired: limit concurrent connections)
        self._p2p_semaphore = asyncio.Semaphore(128)
        self._pq_running = False

        # Statistics
        self._stats = {
            "sent": 0,
            "received": 0,
            "forwarded": 0,
            "duplicates": 0,
            "self_ref_filtered": 0,
            "not_for_me_filtered": 0,
            "re_chain_filtered": 0,
            "ttl_expired": 0,
            "invalid_signature": 0,
            "errors": 0,
        }

    def register_transport(self, name: str, transport: 'TransportAdapter'):
        """Register a transport adapter."""
        self.transports[name] = transport
        log.info(f"Registered transport: {name}")

    def add_handler(self, handler: Callable):
        """Add a message handler (called when a message is for this node)."""
        self._handlers.append(handler)

    def start_priority_queue(self):
        """Start the priority queue processor (P10 first, P1 last)."""
        self._pq_running = True
        self._pq_task = asyncio.create_task(self._pq_process_loop())
        log.info(f"Priority queue processor started (P10→P1) [inbound_queue: {self._inbound_queue.capacity}, outbound_queue: {self._outbound_queue.capacity}]")

    async def stop_priority_queue(self):
        """Stop the priority queue processor."""
        self._pq_running = False
        if self._pq_task:
            self._pq_task.cancel()
            try:
                await self._pq_task
            except asyncio.CancelledError:
                pass
        log.info("Priority queue processor stopped")

    async def enqueue(self, message: A2AMessage):
        """Enqueue message for priority-based processing. P10=urgent, P1=low.
        
        Uses bounded queue with oldest-drop overflow protection.
        """
        await self._inbound_queue.put(message, priority=message.priority)

    async def _pq_process_loop(self):
        """Process messages from bounded priority queue (P10→P1 order)."""
        while self._pq_running:
            try:
                message = await self._inbound_queue.get(timeout=1.0)
                # Route message through stream multiplexer
                raw_data = message.to_json().encode('utf-8') if hasattr(message, 'to_json') else b'{}'
                stream = self._mux.match(raw_data)
                stream_id = stream.stream_id if stream else "default"
                log.debug(f"Processing message {message.id[:8]} via stream '{stream_id}' (P{message.priority})")
                
                # Call handlers
                for handler in self._handlers:
                    try:
                        result = handler(message)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        log.error(f"Handler error for {message.id[:8]}: {e}")
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Priority queue error: {e}")

    async def send(self, message: A2AMessage) -> SendResult:
        """Send a message via best available transport with fallback.

        Priority order: P2P → PG → HTTP (configurable via transport_priority)
        
        Key optimization: For directed messages to a known peer,
        P2P is always preferred (direct connection = lower latency).
        PG NOTIFY is used as fallback when P2P is not connected to that peer.
        If a transport fails, automatically falls back to the next.
        Messages are also stored in LocalStore for offline resilience.
        Enforces protocol version for compatibility.
        """
        # Ensure protocol version is set (AXL-inspired versioning)
        if not message.protocol_version:
            message.protocol_version = A2A_PROTOCOL_VERSION

        # Override sender to node name — UNLESS this is a dashboard or agent reply message
        # Dashboard messages have payload.source = "web_dashboard" (human user)
        # Agent replies have payload.source = "agent_reply" (peer agent)
        if isinstance(message.payload, dict) and message.payload.get("source") in ("web_dashboard", "agent_reply"):
            # Keep the original sender (human user or peer agent)
            pass
        else:
            message.sender = self.node_name

        # Sign message
        if self.config and self.config.security.signing_key:
            from ..core.encryption import MeshEncryption
            enc = MeshEncryption(self.config.security.signing_key)
            content = message.sign_content()
            message.signature = enc.sign_message(content)

        # Store in local_store for offline resilience
        # Skip transient messages (heartbeat, ack) — they don't need persistence
        # and would otherwise fill the DB with noise
        if self.local_store and message.type not in (MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK):
            try:
                payload_str = message.payload if isinstance(message.payload, str) else json.dumps(message.payload)
                self.local_store.enqueue_outbound(
                    msg_id=message.id,
                    sender=message.sender,
                    recipient=message.recipient or "",
                    msg_type=message.type,
                    priority=message.priority,
                    payload=payload_str,
                    routing_mode=message.routing_mode or "hybrid",
                )
            except Exception as e:
                log.debug(f"LocalStore enqueue skipped: {e}")

        # Broadcast: send on all transports
        if message.is_broadcast():
            return await self._send_broadcast(message)

        # Directed: try transports in priority order with fallback
        # P2P optimization: if P2P transport is connected to the recipient,
        # prefer P2P directly and skip PG for latency-sensitive messages.
        priority = self.config.transport_priority if self.config else ["p2p", "pg_notify", "http"]
        
        # ── P2P shortcut: check if P2P has a direct connection to the recipient ──
        p2p_transport = self.transports.get("p2p")
        if p2p_transport and p2p_transport.is_available():
            recipient = message.recipient
            # If P2P has a direct peer connection, try it first regardless of config order
            if recipient and hasattr(p2p_transport, '_peers') and recipient in p2p_transport._peers:
                try:
                    result = await p2p_transport.send(message)
                    if result.success:
                        self._stats["sent"] += 1
                        self._health_scorer.record_success(
                            recipient,
                            latency_ms=result.latency_ms if hasattr(result, 'latency_ms') else 0.0
                        )
                        # Mark as PG-synced if PG transport exists (P2P delivered, no PG needed)
                        if self.local_store:
                            try:
                                self.local_store.mark_outbound_pg_synced(message.id)
                            except Exception:
                                pass
                        return result
                except Exception as e:
                    log.debug(f"P2P direct send to {recipient} failed: {e}, trying fallback transports")

        failures = []
        for transport_name in priority:
            transport = self.transports.get(transport_name)
            if not transport or not transport.is_available():
                failures.append(f"{transport_name}: unavailable")
                continue
            try:
                result = await transport.send(message)
                if result.success:
                    self._stats["sent"] += 1
                    # Health scorer: record success for this transport/recipient
                    self._health_scorer.record_success(
                        message.recipient or transport_name,
                        latency_ms=result.latency_ms if hasattr(result, 'latency_ms') else 0.0
                    )
                    # Mark as PG-synced if we used PG
                    if transport_name == "pg_notify" and self.local_store:
                        try:
                            self.local_store.mark_outbound_pg_synced(message.id)
                        except Exception:
                            pass
                    return result
                failures.append(f"{transport_name}: {result.error}")
            except Exception as e:
                failures.append(f"{transport_name}: {str(e)}")
                log.warning(f"Transport {transport_name} failed: {e}")
                continue

        # All transports failed — message stays in local_store for later sync
        self._stats["errors"] += 1
        # Health scorer: record failure for recipient
        self._health_scorer.record_failure(message.recipient or "unknown")
        error_detail = "; ".join(failures)
        log.warning(f"All transports failed for {message.id[:8]}: {error_detail}")
        return SendResult(transport="none", success=False, error=f"all transports failed: {error_detail}")

    async def _send_broadcast(self, message: A2AMessage) -> SendResult:
        """Send broadcast message on all available transports."""
        results = []
        for name, transport in self.transports.items():
            if not transport.is_available():
                continue
            try:
                result = await transport.send(message)
                results.append(result)
            except Exception as e:
                log.warning(f"Broadcast on {name} failed: {e}")
                results.append(SendResult(transport=name, success=False, error=str(e)))

        successes = sum(1 for r in results if r.success)
        if successes > 0:
            self._stats["sent"] += 1
            # Mark broadcast as pg_synced in local_store if PG transport succeeded
            if self.local_store:
                pg_ok = any(r.success and r.transport == "pg_notify" for r in results)
                if pg_ok:
                    try:
                        self.local_store.mark_outbound_pg_synced(message.id)
                    except Exception:
                        pass
            return SendResult(transport="broadcast", success=True,
                            latency_ms=min(r.latency_ms for r in results if r.success))
        return SendResult(transport="broadcast", success=False, error="all transports failed")

    async def receive(self, message: A2AMessage, from_transport: str) -> ProcessResult:
        """Process a received message (from any transport).

        Applies loop prevention, dedup, and routing logic.
        """
        self._stats["received"] += 1

        # 1. Dedup check
        if self.dedup.check_and_add(message.id):
            self._stats["duplicates"] += 1
            return ProcessResult(status="duplicate", message=message)

        # 2. Self-reference filter
        if self.self_reference_filter and message.sender == self.node_name:
            self._stats["self_ref_filtered"] += 1
            return ProcessResult(status="self_reference", message=message)

        # 3. Not-for-me filter (but allow broadcast)
        if self.not_for_me_filter and not message.is_broadcast() and message.recipient != self.node_name:
            # Not for me — re-flood to my peers
            if message.ttl > 0:
                fwd_msg = message.decrement_ttl().add_hop(self.node_name)
                asyncio.create_task(self._reflood(fwd_msg, from_transport))
                self._stats["forwarded"] += 1
                return ProcessResult(status="forwarded", message=message)
            else:
                self._stats["ttl_expired"] += 1
                return ProcessResult(status="ttl_expired", message=message)

        # 4. RE-chain filter (skip for heartbeat and ACK messages)
        if self.re_chain_limit > 0 and message.type not in ("heartbeat", "ack"):
            re_count = message.payload.get("subject", "").count("RE:")
            if re_count >= self.re_chain_limit:
                self._stats["re_chain_filtered"] += 1
                return ProcessResult(status="re_chain_filtered", message=message)

        # 5. Message is for me — process it
        for handler in self._handlers:
            try:
                await handler(message)
            except Exception as e:
                log.error(f"Handler error: {e}")
                self._stats["errors"] += 1

        return ProcessResult(status="processed", message=message)

    async def _reflood(self, message: A2AMessage, exclude_transport: str):
        """Re-flood message to all peers except the sender transport."""
        for name, transport in self.transports.items():
            if name == exclude_transport or not transport.is_available():
                continue
            try:
                await transport.send(message)
            except Exception:
                continue

    def get_stats(self) -> dict:
        """Return routing statistics including stream mux and bounded queue stats."""
        return {
            **self._stats,
            "dedup": self.dedup.stats,
            "inbound_queue": self._inbound_queue.stats,
            "outbound_queue": self._outbound_queue.stats,
            "stream_mux": self._mux.get_stats(),
            "gossipsub": self._gossipsub.stats,
            "health_scorer": self._health_scorer.stats,
            "protocol_version": A2A_PROTOCOL_VERSION,
            "transports": {
                name: transport.get_status()
                for name, transport in self.transports.items()
            },
        }