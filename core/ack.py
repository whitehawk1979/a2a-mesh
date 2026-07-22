"""A2A Mesh Acknowledgment System — Reliable message delivery with ACK tracking.

Features:
- Per-message ACK tracking with timeout and retry
- ACK types: delivered, read, processed, error
- Automatic retry with exponential backoff (3 attempts)
- ACK timeout configurable per priority level
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Callable, Awaitable

from .message import A2AMessage, MSG_TYPE_ACK

log = logging.getLogger("a2a_mesh.ack")


class AckType(Enum):
    """Types of acknowledgment."""
    DELIVERED = "delivered"       # Message received by recipient
    READ = "read"                # Message read/seen by recipient
    PROCESSED = "processed"      # Message processed/acted upon
    ERROR = "error"              # Error processing message


class AckStatus(Enum):
    """Status of an ACK tracking entry."""
    PENDING = "pending"          # Waiting for ACK
    ACKNOWLEDGED = "acknowledged"  # ACK received
    TIMEOUT = "timeout"          # No ACK received in time
    FAILED = "failed"            # All retries exhausted


# Priority-based ACK timeouts (seconds)
PRIORITY_TIMEOUTS = {
    10: 5,    # Critical: 5s
    9: 10,    # High: 10s
    8: 15,    # Important: 15s
    7: 20,    # Above normal: 20s
    6: 30,    # Normal+: 30s
    5: 45,    # Normal: 45s
    4: 60,    # Below normal: 60s
    3: 90,    # Low: 90s
    2: 120,   # Very low: 120s
    1: 300,   # Background: 5min
}

DEFAULT_TIMEOUT = 60
MAX_RETRIES = 3


@dataclass
class AckTracker:
    """Tracks a single message waiting for ACK."""
    message_id: str
    recipient: str
    priority: int
    status: AckStatus = AckStatus.PENDING
    ack_type: Optional[AckType] = None
    sent_at: float = field(default_factory=time.time)
    acked_at: Optional[float] = None
    retry_count: int = 0
    next_retry_at: Optional[float] = None
    error: str = ""


class AckManager:
    """Manages message acknowledgment tracking and retries.

    Usage:
        ack_mgr = AckManager()

        # Send a message and track it
        ack_mgr.track(message, on_timeout=retry_send)

        # Process an incoming ACK
        ack_mgr.process_ack(ack_message)

        # Run the background loop
        await ack_mgr.start()
    """

    def __init__(self, node_name: str = "", max_retries: int = MAX_RETRIES):
        self.node_name = node_name
        self.max_retries = max_retries
        self._tracked: Dict[str, AckTracker] = {}  # message_id → tracker
        self._on_timeout: Optional[Callable[[A2AMessage, int], Awaitable[None]]] = None
        self._on_ack: Optional[Callable[[A2AMessage, AckType], Awaitable[None]]] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def set_callbacks(
        self,
        on_timeout: Optional[Callable[[A2AMessage, int], Awaitable[None]]] = None,
        on_ack: Optional[Callable[[A2AMessage, AckType], Awaitable[None]]] = None,
    ):
        """Set callbacks for timeout and ACK events."""
        self._on_timeout = on_timeout
        self._on_ack = on_ack

    def track(self, message: A2AMessage):
        """Start tracking a message for ACK."""
        tracker = AckTracker(
            message_id=message.id,
            recipient=message.recipient,
            priority=message.priority,
        )
        self._tracked[message.id] = tracker
        log.debug(f"Tracking message {message.id[:8]} → {message.recipient} (P{message.priority})")

    def create_ack(self, original_message: A2AMessage, ack_type: AckType, error: str = "") -> A2AMessage:
        """Create an ACK message for a received message."""
        ack = A2AMessage.create(
            sender=self.node_name,
            recipient=original_message.sender,
            msg_type=MSG_TYPE_ACK,
            priority=min(original_message.priority + 1, 10),  # ACK has higher priority
            payload={
                "ack_for": original_message.id,
                "ack_type": ack_type.value,
                "original_type": original_message.type,
                "original_sender": original_message.sender,
                "timestamp": time.time(),
                "error": error,
            },
        )
        return ack

    def process_ack(self, ack_message: A2AMessage) -> Optional[AckTracker]:
        """Process an incoming ACK message. Returns the tracker if found."""
        payload = ack_message.payload
        original_id = payload.get("ack_for", "")
        ack_type_str = payload.get("ack_type", "delivered")

        if not original_id or original_id not in self._tracked:
            log.debug(f"ACK for unknown message {original_id[:8] if original_id else '???'}")
            return None

        tracker = self._tracked[original_id]
        try:
            tracker.ack_type = AckType(ack_type_str)
        except ValueError:
            tracker.ack_type = AckType.DELIVERED

        tracker.status = AckStatus.ACKNOWLEDGED
        tracker.acked_at = time.time()
        elapsed = tracker.acked_at - tracker.sent_at

        log.info(
            f"ACK received: {original_id[:8]} ← {ack_type_str} "
            f"from {ack_message.sender} ({elapsed:.1f}s, {tracker.retry_count} retries)"
        )

        # Remove from tracking (ACK received)
        del self._tracked[original_id]

        # Callback
        if self._on_ack:
            # Reconstruct minimal message info
            original = A2AMessage.create(
                sender=tracker.recipient,
                recipient=self.node_name,
                msg_type=payload.get("original_type", ""),
                payload={},
            )
            original.id = original_id
            asyncio.create_task(self._on_ack(original, tracker.ack_type))

        return tracker

    def get_timeout(self, priority: int) -> float:
        """Get ACK timeout for a priority level."""
        return PRIORITY_TIMEOUTS.get(priority, DEFAULT_TIMEOUT)

    async def start(self):
        """Start the ACK monitoring loop."""
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        log.info("ACK manager started")

    async def stop(self):
        """Stop the ACK monitoring loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("ACK manager stopped")

    async def _monitor_loop(self):
        """Periodically check for timed-out messages."""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                if not self._running:
                    break

                now = time.time()
                timed_out = []

                for msg_id, tracker in list(self._tracked.items()):
                    timeout = self.get_timeout(tracker.priority)
                    elapsed = now - tracker.sent_at

                    if elapsed > timeout and tracker.status == AckStatus.PENDING:
                        tracker.retry_count += 1

                        if tracker.retry_count >= self.max_retries:
                            tracker.status = AckStatus.FAILED
                            log.warning(
                                f"ACK failed: {msg_id[:8]} → {tracker.recipient} "
                                f"({tracker.retry_count} retries exhausted)"
                            )
                            timed_out.append(msg_id)
                        else:
                            # Schedule retry
                            backoff = min(5 * (2 ** (tracker.retry_count - 1)), 120)
                            tracker.next_retry_at = now + backoff
                            tracker.status = AckStatus.PENDING
                            log.info(
                                f"ACK timeout: {msg_id[:8]} → {tracker.recipient} "
                                f"(retry {tracker.retry_count}/{self.max_retries}, next in {backoff:.0f}s)"
                            )
                            # Fire timeout callback for retry
                            if self._on_timeout:
                                original = A2AMessage.create(
                                    sender=self.node_name,
                                    recipient=tracker.recipient,
                                    msg_type="",
                                    payload={},
                                )
                                original.id = msg_id
                                await self._on_timeout(original, tracker.retry_count)

                # Clean up failed entries (keep for 5 min then remove)
                cutoff = now - 300
                for msg_id in timed_out:
                    tracker = self._tracked.get(msg_id)
                    if tracker and tracker.status == AckStatus.FAILED:
                        # Remove failed entries older than 5 minutes (use sent_at since acked_at is None for failures)
                        if tracker.sent_at and (now - tracker.sent_at) > 300:
                            del self._tracked[msg_id]

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"ACK monitor error: {e}")

    def get_stats(self) -> dict:
        """Get ACK tracking statistics."""
        pending = sum(1 for t in self._tracked.values() if t.status == AckStatus.PENDING)
        acknowledged = sum(1 for t in self._tracked.values() if t.status == AckStatus.ACKNOWLEDGED)
        failed = sum(1 for t in self._tracked.values() if t.status == AckStatus.FAILED)
        return {
            "pending": pending,
            "acknowledged": acknowledged,
            "failed": failed,
            "total_tracked": len(self._tracked),
        }