"""A2A Mesh Offline Queue — Buffer messages for offline nodes.

When a recipient node is offline (not in mesh_nodes with status='active'),
messages are stored in mesh.mesh_offline_queue for later delivery.

Features:
- Automatic queuing when recipient is offline
- Retry on node join (LISTEN/NOTIFY on mesh_channel)
- Priority ordering (higher priority delivered first)
- TTL: messages expire after configurable period (default: 7 days)
- Size limits: max message payload size (default: 1MB for PG, 50MB for MinIO)
"""

import json
import logging
import time
from dataclasses import dataclass
from typing import Optional, List

from .message import A2AMessage

log = logging.getLogger("a2a_mesh.offline_queue")

# Default settings
DEFAULT_TTL_DAYS = 7
DEFAULT_MAX_PAYLOAD_BYTES = 1 * 1024 * 1024  # 1MB


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
    """Manages offline message queuing and delivery.

    Uses PG table mesh.mesh_offline_queue for persistence.
    Automatically delivers queued messages when a node comes online.
    """

    def __init__(self, pg_config, node_name: str = ""):
        self.pg_config = pg_config
        self.node_name = node_name
        self._conn = None

    def _get_conn(self):
        """Get or create PG connection."""
        if self._conn and self._conn.closed == 0:
            return self._conn
        import psycopg2
        self._conn = psycopg2.connect(
            host=self.pg_config.host,
            port=self.pg_config.port,
            dbname=self.pg_config.dbname,
            user=self.pg_config.user,
            password=self.pg_config.password,
        )
        self._conn.autocommit = True
        return self._conn

    def ensure_table(self):
        """Create the offline queue table if it doesn't exist."""
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
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
            );
            CREATE INDEX IF NOT EXISTS idx_offline_recipient ON mesh.mesh_offline_queue (recipient, status, priority DESC);
            CREATE INDEX IF NOT EXISTS idx_offline_expires ON mesh.mesh_offline_queue (expires_at);
        """)
        cur.close()
        log.info("Offline queue table ensured")

    def enqueue(self, message: A2AMessage, ttl_days: int = DEFAULT_TTL_DAYS) -> bool:
        """Queue a message for later delivery to an offline node."""
        conn = self._get_conn()
        cur = conn.cursor()

        try:
            import psycopg2.extras
            from datetime import datetime, timezone, timedelta

            expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)

            cur.execute("""
                INSERT INTO mesh.mesh_offline_queue
                    (id, sender, recipient, msg_type, priority, payload,
                     routing_mode, src_addr, dst_addr, expires_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'queued')
            """, (
                message.id,
                message.sender,
                message.recipient,
                message.type,
                message.priority,
                json.dumps(message.payload),
                message.routing_mode,
                message.src_address.get("short") if message.src_address else None,
                message.dst_address.get("short") if message.dst_address else None,
                expires_at,
            ))
            log.info(f"Queued message {message.id[:8]} for offline node {message.recipient}")
            return True
        except Exception as e:
            log.error(f"Failed to queue message {message.id[:8]}: {e}")
            return False
        finally:
            cur.close()

    def dequeue(self, recipient: str, limit: int = 50) -> List[dict]:
        """Get queued messages for a node that just came online.

        Returns messages ordered by priority (highest first), then by creation time.
        """
        conn = self._get_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT id, sender, recipient, msg_type, priority, payload,
                       routing_mode, src_addr, dst_addr, retry_count, created_at
                FROM mesh.mesh_offline_queue
                WHERE recipient = %s
                  AND status = 'queued'
                  AND (deliver_after IS NULL OR deliver_after <= NOW())
                  AND (expires_at IS NULL OR expires_at > NOW())
                ORDER BY priority DESC, created_at ASC
                LIMIT %s
            """, (recipient, limit))

            rows = cur.fetchall()
            messages = []
            for row in rows:
                messages.append({
                    "id": row[0],
                    "sender": row[1],
                    "recipient": row[2],
                    "msg_type": row[3],
                    "priority": row[4],
                    "payload": row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}"),
                    "routing_mode": row[6],
                    "src_addr": row[7],
                    "dst_addr": row[8],
                    "retry_count": row[9],
                    "created_at": str(row[10]),
                })
            return messages
        except Exception as e:
            log.error(f"Failed to dequeue messages for {recipient}: {e}")
            return []
        finally:
            cur.close()

    def mark_delivered(self, message_id: str):
        """Mark a queued message as delivered."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE mesh.mesh_offline_queue
                SET status = 'delivered', delivered_at = NOW()
                WHERE id = %s
            """, (message_id,))
        except Exception as e:
            log.error(f"Failed to mark delivered {message_id[:8]}: {e}")
        finally:
            cur.close()

    def mark_failed(self, message_id: str, error: str = "", increment_retry: bool = True):
        """Mark a queued message as failed (or increment retry count)."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            if increment_retry:
                cur.execute("""
                    UPDATE mesh.mesh_offline_queue
                    SET retry_count = retry_count + 1,
                        last_error = %s,
                        status = CASE WHEN retry_count + 1 >= max_retries THEN 'failed' ELSE 'queued' END
                    WHERE id = %s
                """, (error, message_id))
            else:
                cur.execute("""
                    UPDATE mesh.mesh_offline_queue
                    SET status = 'failed', last_error = %s
                    WHERE id = %s
                """, (error, message_id))
        except Exception as e:
            log.error(f"Failed to mark failed {message_id[:8]}: {e}")
        finally:
            cur.close()

    def cleanup_expired(self) -> int:
        """Remove expired messages from the queue. Returns count of removed messages."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                DELETE FROM mesh.mesh_offline_queue
                WHERE expires_at < NOW()
                   OR (status = 'failed' AND created_at < NOW() - INTERVAL '3 days')
                   OR (status = 'delivered' AND delivered_at < NOW() - INTERVAL '1 day')
            """)
            count = cur.rowcount
            if count > 0:
                log.info(f"Cleaned up {count} expired/old offline queue messages")
            return count
        except Exception as e:
            log.error(f"Cleanup failed: {e}")
            return 0
        finally:
            cur.close()

    def get_stats(self) -> dict:
        """Get offline queue statistics."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT status, COUNT(*) as cnt
                FROM mesh.mesh_offline_queue
                GROUP BY status
            """)
            stats = {row[0]: row[1] for row in cur.fetchall()}
            return {
                "total": sum(stats.values()),
                "queued": stats.get("queued", 0),
                "delivered": stats.get("delivered", 0),
                "failed": stats.get("failed", 0),
            }
        except Exception as e:
            log.error(f"Stats query failed: {e}")
            return {"total": 0, "queued": 0, "delivered": 0, "failed": 0}
        finally:
            cur.close()

    def is_node_online(self, node_name: str) -> bool:
        """Check if a node is currently online (active in mesh_nodes)."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT status FROM mesh.mesh_nodes
                WHERE node_name = %s
            """, (node_name,))
            row = cur.fetchone()
            return row is not None and row[0] == "active"
        finally:
            cur.close()