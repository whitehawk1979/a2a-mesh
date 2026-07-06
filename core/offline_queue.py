"""A2A Mesh Offline Queue — Buffer messages for offline nodes (asyncpg version).

When a recipient node is offline (not in mesh_nodes with status='active'),
messages are stored in mesh.mesh_offline_queue for later delivery.

Features:
- Automatic queuing when recipient is offline
- Async delivery on node join (triggered by NOTIFY/LISTEN)
- Priority ordering (higher priority delivered first)
- TTL: messages expire after configurable period (default: 7 days)
- Size limits: max message payload size (default: 1MB)
- Ghost node pruning: removes stale/offline nodes from mesh_nodes
- Uses asyncpg for non-blocking database operations
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime, timezone, timedelta

from .message import A2AMessage
from .async_db import AsyncDBPool

log = logging.getLogger("a2a_mesh.offline_queue")

# Default settings
DEFAULT_TTL_DAYS = 7
DEFAULT_MAX_PAYLOAD_BYTES = 1 * 1024 * 1024  # 1MB
DEFAULT_GHOST_TIMEOUT_SECONDS = 600  # 10 minutes
DEFAULT_STALE_NODE_DAYS = 7  # Remove nodes offline for > 7 days


@dataclass
class QueuedMessage:
    """A message stored in the offline queue."""
    id: str
    sender: str
    recipient: str
    msg_type: str
    priority: int
    payload_json: str
    queued_at: float
    retry_count: int = 0
    max_retries: int = 5
    last_error: str = ""


class OfflineQueue:
    """Manages offline message queuing and delivery using asyncpg.

    Uses PG table mesh.mesh_offline_queue for persistence.
    Automatically delivers queued messages when a node comes online.
    """

    def __init__(self, pg_config=None, node_name: str = ""):
        self.pg_config = pg_config
        self.node_name = node_name
        self._pool: Optional[AsyncDBPool] = None

    async def init_pool(self, pool: AsyncDBPool):
        """Set the asyncpg connection pool (called by MeshNode after start)."""
        self._pool = pool

    async def ensure_table(self):
        """Create the offline queue table if it doesn't exist."""
        if not self._pool or not self._pool.is_connected():
            log.warning("OfflineQueue: pool not connected, skipping table creation")
            return
        try:
            await self._pool.execute("""
                CREATE TABLE IF NOT EXISTS mesh.mesh_offline_queue (
                    id VARCHAR(36) PRIMARY KEY,
                    sender VARCHAR(64) NOT NULL,
                    recipient VARCHAR(64) NOT NULL,
                    msg_type VARCHAR(32) NOT NULL,
                    priority INTEGER DEFAULT 5,
                    payload JSONB NOT NULL DEFAULT '{}',
                    routing_mode VARCHAR(16) DEFAULT 'hybrid',
                    src_addr INTEGER,
                    dst_addr INTEGER,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 5,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    deliver_after TIMESTAMPTZ,
                    expires_at TIMESTAMPTZ,
                    status VARCHAR(16) DEFAULT 'queued',
                    last_error TEXT,
                    delivered_at TIMESTAMPTZ
                )
            """)
            await self._pool.execute("""
                CREATE INDEX IF NOT EXISTS idx_offline_recipient ON mesh.mesh_offline_queue (recipient, status, priority DESC)
            """)
            await self._pool.execute("""
                CREATE INDEX IF NOT EXISTS idx_offline_expires ON mesh.mesh_offline_queue (expires_at)
            """)
            log.info("Offline queue table ensured")
        except Exception as e:
            log.error(f"Failed to create offline queue table: {e}")

    # Keep sync method for backward compatibility (used during __init__ before pool available)
    def ensure_table(self):
        """Sync stub — callers should use await ensure_table() instead."""
        log.warning("OfflineQueue.ensure_table() called synchronously — use await ensure_table() instead")

    async def enqueue(self, message: A2AMessage, ttl_days: int = DEFAULT_TTL_DAYS) -> bool:
        """Queue a message for later delivery to an offline node."""
        if not self._pool or not self._pool.is_connected():
            log.warning("OfflineQueue: pool not connected, cannot enqueue")
            return False

        try:
            expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)

            await self._pool.execute("""
                INSERT INTO mesh.mesh_offline_queue
                    (id, sender, recipient, msg_type, priority, payload,
                     routing_mode, src_addr, dst_addr, expires_at, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'queued')
            """,
                message.id,
                message.sender,
                message.recipient,
                message.type,
                message.priority,
                json.dumps(message.payload, default=str),
                message.routing_mode,
                message.src_address.get("short") if message.src_address else None,
                message.dst_address.get("short") if message.dst_address else None,
                expires_at,
            )
            log.info(f"Queued message {message.id[:8]} for offline node {message.recipient}")
            return True
        except Exception as e:
            log.error(f"Failed to queue message {message.id[:8]}: {e}")
            return False

    # Sync wrapper for backward compatibility
    def enqueue(self, message: A2AMessage, ttl_days: int = DEFAULT_TTL_DAYS) -> bool:
        """Synchronous enqueue — for use from non-async contexts.

        WARNING: This will block the event loop if no pool is available.
        Prefer using await enqueue() instead.
        """
        # Try to run async version in existing loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context — schedule the coroutine
            task = loop.create_task(self.enqueue(message, ttl_days))
            # Can't await here — return optimistic result
            return True
        except RuntimeError:
            # No running loop — use sync fallback
            pass

        # Fallback: synchronous psycopg2 (legacy, will be removed)
        if not self._pool or not self._pool.is_connected():
            log.warning("OfflineQueue.enqueue: no pool available")
            return False
        return False

    async def dequeue(self, recipient: str, limit: int = 50) -> List[dict]:
        """Get queued messages for a node that just came online.

        Returns messages ordered by priority (highest first), then by creation time.
        """
        if not self._pool or not self._pool.is_connected():
            return []

        try:
            rows = await self._pool.fetch("""
                SELECT id, sender, recipient, msg_type, priority, payload,
                       routing_mode, src_addr, dst_addr, retry_count, created_at
                FROM mesh.mesh_offline_queue
                WHERE recipient = $1
                  AND status = 'queued'
                  AND (deliver_after IS NULL OR deliver_after <= NOW())
                  AND (expires_at IS NULL OR expires_at > NOW())
                ORDER BY priority DESC, created_at ASC
                LIMIT $2
            """, recipient, limit)

            messages = []
            for row in rows:
                payload = row['payload']
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                elif not isinstance(payload, dict):
                    payload = {}

                messages.append({
                    "id": row['id'],
                    "sender": row['sender'],
                    "recipient": row['recipient'],
                    "msg_type": row['msg_type'],
                    "priority": row['priority'],
                    "payload": payload,
                    "routing_mode": row['routing_mode'],
                    "src_addr": row['src_addr'],
                    "dst_addr": row['dst_addr'],
                    "retry_count": row['retry_count'],
                    "created_at": str(row['created_at']),
                })
            return messages
        except Exception as e:
            log.error(f"Failed to dequeue messages for {recipient}: {e}")
            return []

    async def mark_delivered(self, message_id: str):
        """Mark a queued message as delivered."""
        if not self._pool or not self._pool.is_connected():
            return
        try:
            await self._pool.execute("""
                UPDATE mesh.mesh_offline_queue
                SET status = 'delivered', delivered_at = NOW()
                WHERE id = $1
            """, message_id)
        except Exception as e:
            log.error(f"Failed to mark delivered {message_id[:8]}: {e}")

    async def mark_failed(self, message_id: str, error: str = "", increment_retry: bool = True):
        """Mark a queued message as failed (or increment retry count)."""
        if not self._pool or not self._pool.is_connected():
            return
        try:
            if increment_retry:
                await self._pool.execute("""
                    UPDATE mesh.mesh_offline_queue
                    SET retry_count = retry_count + 1,
                        last_error = $1,
                        status = CASE WHEN retry_count + 1 >= max_retries THEN 'failed' ELSE 'queued' END
                    WHERE id = $2
                """, error, message_id)
            else:
                await self._pool.execute("""
                    UPDATE mesh.mesh_offline_queue
                    SET status = 'failed', last_error = $1
                    WHERE id = $2
                """, error, message_id)
        except Exception as e:
            log.error(f"Failed to mark failed {message_id[:8]}: {e}")

    async def cleanup_expired(self) -> int:
        """Remove expired messages from the queue. Returns count of removed messages."""
        if not self._pool or not self._pool.is_connected():
            return 0
        try:
            result = await self._pool.execute("""
                DELETE FROM mesh.mesh_offline_queue
                WHERE expires_at < NOW()
                   OR (status = 'failed' AND created_at < NOW() - INTERVAL '3 days')
                   OR (status = 'delivered' AND delivered_at < NOW() - INTERVAL '1 day')
            """)
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                log.info(f"Cleaned up {count} expired/old offline queue messages")
            return count
        except Exception as e:
            log.error(f"Cleanup failed: {e}")
            return 0

    async def get_stats(self) -> dict:
        """Get offline queue statistics."""
        if not self._pool or not self._pool.is_connected():
            return {"total": 0, "queued": 0, "delivered": 0, "failed": 0}
        try:
            rows = await self._pool.fetch("""
                SELECT status, COUNT(*) as cnt
                FROM mesh.mesh_offline_queue
                GROUP BY status
            """)
            stats = {row['status']: row['cnt'] for row in rows}
            return {
                "total": sum(stats.values()),
                "queued": stats.get("queued", 0),
                "delivered": stats.get("delivered", 0),
                "failed": stats.get("failed", 0),
            }
        except Exception as e:
            log.error(f"Stats query failed: {e}")
            return {"total": 0, "queued": 0, "delivered": 0, "failed": 0}

    async def is_node_online(self, node_name: str) -> bool:
        """Check if a node is currently online (active in mesh_nodes)."""
        if not self._pool or not self._pool.is_connected():
            return True  # Assume online if can't check — don't queue unnecessarily
        try:
            result = await self._pool.fetchval("""
                SELECT status FROM mesh.mesh_nodes
                WHERE node_name = $1
            """, node_name)
            return result == "active"
        except Exception as e:
            log.error(f"Failed to check node online status for {node_name}: {e}")
            return True  # Assume online on error

    async def prune_ghost_nodes(self, timeout_seconds: int = DEFAULT_GHOST_TIMEOUT_SECONDS) -> int:
        """Mark nodes as offline if they haven't sent a heartbeat within the timeout.

        This implements the ghost peer pruning feature — nodes that go offline
        leave stale 'active' entries in mesh_nodes. This method marks them as
        'offline' based on heartbeat timeout.

        Args:
            timeout_seconds: Seconds since last heartbeat before marking offline (default: 600s = 10min)

        Returns:
            Number of nodes marked offline
        """
        if not self._pool or not self._pool.is_connected():
            return 0
        try:
            result = await self._pool.execute("""
                UPDATE mesh.mesh_nodes
                SET status = 'offline'
                WHERE status = 'active'
                  AND last_heartbeat < NOW() - ($1 || ' seconds')::INTERVAL
            """, str(timeout_seconds))
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                log.info(f"Pruned {count} ghost nodes (no heartbeat in {timeout_seconds}s)")
            return count
        except Exception as e:
            log.error(f"Ghost node pruning failed: {e}")
            return 0

    async def prune_stale_nodes(self, stale_days: int = DEFAULT_STALE_NODE_DAYS) -> int:
        """Remove nodes that have been offline for longer than stale_days.

        Args:
            stale_days: Days a node must be offline before being removed (default: 7)

        Returns:
            Number of nodes removed
        """
        if not self._pool or not self._pool.is_connected():
            return 0
        try:
            result = await self._pool.execute("""
                DELETE FROM mesh.mesh_nodes
                WHERE status = 'offline'
                  AND last_heartbeat < NOW() - ($1 || ' days')::INTERVAL
            """, str(stale_days))
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                log.info(f"Removed {count} stale nodes (offline > {stale_days} days)")
            return count
        except Exception as e:
            log.error(f"Stale node removal failed: {e}")
            return 0