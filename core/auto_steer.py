"""A2A Mesh Auto-Steer — Priority-based message dispatch and steer processing.

Implements:
- P10+ messages: immediate webhook interrupt + handler dispatch
- P7-9 messages: high-priority queue + handler dispatch  
- P1-6 messages: normal-priority queue (backlog processing)
- Steer directives: execution tracking with ACK
"""
import asyncio
import json
import logging
import time
from typing import Optional, Callable, Dict, List, Any
from dataclasses import dataclass, field

from .message import A2AMessage, MSG_TYPE_STEER, MSG_TYPE_DIRECTIVE, MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK

log = logging.getLogger("a2a_mesh.auto_steer")


@dataclass
class SteerDirective:
    """A steer directive to be processed."""
    id: str
    sender: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    priority: int = 5
    received_at: float = field(default_factory=time.time)
    status: str = "pending"  # pending, executing, completed, failed
    result: Optional[str] = None


class AutoSteerProcessor:
    """Process incoming messages based on priority and type.

    Priority routing:
    - P10: Immediate webhook interrupt + handler dispatch
    - P7-9: High-priority queue + handler dispatch
    - P1-6: Normal-priority queue (backlog)

    Steer directives:
    - Tracked from receipt to completion
    - ACK sent on receipt
    - Result reported on completion
    """

    def __init__(self, node_name: str, config: Optional[Any] = None):
        self.node_name = node_name
        self.config = config
        self.auto_steer_config = getattr(config, 'auto_steer', None) if config else None

        # Priority threshold from config
        self.interrupt_threshold = (
            getattr(self.auto_steer_config, 'priority_threshold', 10)
            if self.auto_steer_config else 10
        )
        self.queue_lower = (
            getattr(self.auto_steer_config, 'queue_lower_priorities', True)
            if self.auto_steer_config else True
        )

        # Active steer directives
        self._active_steers: Dict[str, SteerDirective] = {}

        # Stats — cumulative counters + processed counters for accurate queue depth
        self._stats = {
            "interrupts_triggered": 0,
            "steers_received": 0,
            "steers_completed": 0,
            "steers_failed": 0,
            "high_priority_queued": 0,
            "high_priority_dispatched": 0,
            "normal_priority_queued": 0,
            "normal_priority_dispatched": 0,
            "skipped_internal": 0,  # heartbeat/ack/skills_announcement bypassed
        }

    def classify_message(self, message: A2AMessage) -> str:
        """Classify message priority level and action.

        Returns:
            'interrupt' — P10+: immediate webhook + handler dispatch
            'high' — P7-9: high-priority queue + handler dispatch
            'normal' — P1-6: backlog queue
        """
        if message.priority >= self.interrupt_threshold:
            return "interrupt"
        elif message.priority >= 7:
            return "high"
        else:
            return "normal"

    async def process_message(self, message: A2AMessage) -> Optional[str]:
        """Process incoming message based on priority and type.

        Internal housekeeping messages (heartbeat, ack, skills_announcement)
        are skipped — they don't need steer processing and were inflating
        the normal_priority_queued counter.

        Returns:
            Action taken: 'interrupt', 'high', 'normal', 'steer', or 'skipped'
        """
        # Skip internal mesh housekeeping messages — they don't need classification
        if message.type in (MSG_TYPE_HEARTBEAT, MSG_TYPE_ACK, "skills_announcement"):
            self._stats["skipped_internal"] += 1
            return "skipped"

        level = self.classify_message(message)

        # Handle steer directives specially
        if message.type == MSG_TYPE_STEER:
            return await self._process_steer(message)

        # Handle by priority level
        if level == "interrupt":
            self._stats["interrupts_triggered"] += 1
            log.info(f"⚡ INTERRUPT: Message {message.id[:8]} P{message.priority} from {message.sender}")
            # P10+ triggers immediate webhook
            await self._trigger_webhook(message)
            return "interrupt"

        elif level == "high":
            self._stats["high_priority_queued"] += 1
            log.info(f"🔴 HIGH: Message {message.id[:8]} P{message.priority} from {message.sender}")
            return "high"

        else:
            self._stats["normal_priority_queued"] += 1
            log.debug(f"📋 NORMAL: Message {message.id[:8]} P{message.priority} from {message.sender}")
            return "normal"

    async def _process_steer(self, message: A2AMessage) -> str:
        """Process a steer directive with tracking and ACK."""
        self._stats["steers_received"] += 1

        payload = message.payload if hasattr(message, 'payload') else {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                payload = {"raw": payload}

        action = payload.get("action", "unknown")
        params = payload.get("params", {})

        directive = SteerDirective(
            id=message.id,
            sender=message.sender,
            action=action,
            params=params,
            priority=message.priority,
        )
        self._active_steers[message.id] = directive

        # P10+ steer = immediate action + webhook
        if message.priority >= self.interrupt_threshold:
            self._stats["interrupts_triggered"] += 1
            directive.status = "executing"
            log.info(f"🎯 STEER INTERRUPT: {action} from {message.sender} (P{message.priority})")
            await self._trigger_webhook(message)
            return "steer_interrupt"
        else:
            # Normal steer = queued
            log.info(f"🎯 STEER queued: {action} from {message.sender} (P{message.priority})")
            return "steer_queued"

    def update_steer_status(self, steer_id: str, status: str, result: Optional[str] = None):
        """Update steer directive status."""
        if steer_id in self._active_steers:
            directive = self._active_steers[steer_id]
            directive.status = status
            directive.result = result
            if status == "completed":
                self._stats["steers_completed"] += 1
            elif status == "failed":
                self._stats["steers_failed"] += 1

    def get_active_steers(self) -> List[SteerDirective]:
        """Get all active steer directives."""
        return list(self._active_steers.values())

    def cleanup_old_steers(self, max_age_seconds: int = 3600):
        """Remove completed/failed steers older than max_age_seconds."""
        now = time.time()
        to_remove = [
            k for k, v in self._active_steers.items()
            if v.status in ("completed", "failed") and (now - v.received_at) > max_age_seconds
        ]
        for k in to_remove:
            del self._active_steers[k]

    async def _trigger_webhook(self, message: A2AMessage):
        """Trigger webhook for high-priority messages.

        This calls the Hermes webhook endpoint to wake the agent.
        """
        import aiohttp
        webhook_url = f"http://localhost:8644/webhook"
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "message_id": message.id,
                    "sender": message.sender,
                    "recipient": message.recipient,
                    "type": message.type,
                    "priority": message.priority,
                    "payload": message.payload if hasattr(message, 'payload') else {},
                    "timestamp": message.timestamp,
                }
                async with session.post(
                    webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        log.info(f"✅ Webhook triggered for {message.id[:8]}")
                    else:
                        log.warning(f"⚠️ Webhook returned {resp.status} for {message.id[:8]}")
        except Exception as e:
            log.warning(f"⚠️ Webhook failed for {message.id[:8]}: {e}")

    def get_stats(self) -> Dict:
        """Get auto-steer statistics.

        'queued' counters are cumulative totals (not current queue depth).
        For current queue depth, see 'high_priority_pending' and
        'normal_priority_pending' which subtract dispatched from queued.
        """
        return {
            **self._stats,
            "interrupt_threshold": self.interrupt_threshold,
            "queue_lower": self.queue_lower,
            "active_steers": len(self._active_steers),
            "high_priority_pending": max(0, self._stats["high_priority_queued"] - self._stats["high_priority_dispatched"]),
            "normal_priority_pending": max(0, self._stats["normal_priority_queued"] - self._stats["normal_priority_dispatched"]),
        }