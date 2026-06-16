"""
Mesh Memory Sync — Synchronize memory across connected mesh agents.

Uses mesh.mesh_messages as the sole memory store. When a new agent joins,
it can request a memory dump from the coordinator. The coordinator broadcasts
memory updates to all connected agents via mesh_messages.

No dependency on shared_a2a_memory — mesh-only synchronization.
"""

import json
import logging
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

log = logging.getLogger("mesh.memory_sync")


class MemorySync:
    """Synchronize memory across mesh agents using mesh_messages."""

    def __init__(self, node):
        self.node = node
        self._pg_conn = None
        self._last_sync = 0  # timestamp of last sync
        self._local_memory = {}  # key -> value cache

    def set_pg_conn(self, conn):
        """Set PG connection for sync operations."""
        self._pg_conn = conn

    async def broadcast_memory(self, key: str, value: Any, msg_type: str = "memory_sync") -> bool:
        """Broadcast a memory update to all connected mesh agents.

        Stores the memory in mesh.mesh_messages with type 'memory_sync'.
        All agents listening on mesh_channel will receive the NOTIFY.
        """
        if not self.node._pg_transport or not self.node._pg_transport.is_available():
            log.warning("PG transport not available, cannot broadcast memory")
            return False

        from ..core.message import A2AMessage

        message = A2AMessage.create(
            sender=self.node.node_name,
            recipient="broadcast",
            msg_type=msg_type,
            payload={
                "memory_key": key,
                "memory_value": value,
                "timestamp": time.time(),
                "source": self.node.node_name,
            },
            priority=7,
        )
        # Set routing mode for mesh
        message.routing_mode = "broadcast"

        result = await self.node._pg_transport.send(message)
        if result.success:
            log.info(f"Memory broadcast: {key} → broadcast ({result.latency_ms:.1f}ms)")
            self._local_memory[key] = value
            return True
        else:
            log.error(f"Memory broadcast failed: {result.error}")
            return False

    async def request_sync(self, since: Optional[float] = None) -> List[Dict]:
        """Request memory sync from PG — pull all memory_sync messages since timestamp.

        Used when a new agent joins or reconnects to pull missed memory updates.
        """
        if not self._pg_conn:
            log.warning("PG connection not available for sync")
            return []

        try:
            import psycopg2
            cur = self._pg_conn.cursor()
            cur.execute("SET client_encoding TO UTF8")

            if since:
                cur.execute("""
                    SELECT id, sender, recipient, msg_type, payload, priority, created_at
                    FROM mesh.mesh_messages 
                    WHERE msg_type = 'memory_sync' AND created_at > %s
                    ORDER BY created_at ASC
                """, (datetime.fromtimestamp(since, tz=timezone.utc),))
            else:
                cur.execute("""
                    SELECT id, sender, recipient, msg_type, payload, priority, created_at
                    FROM mesh.mesh_messages 
                    WHERE msg_type = 'memory_sync'
                    ORDER BY created_at ASC
                """)

            rows = cur.fetchall()
            cur.close()

            memories = []
            for row in rows:
                msg_id, sender, recipient, msg_type, payload, priority, created_at = row
                try:
                    data = json.loads(payload) if isinstance(payload, str) else payload
                    memories.append({
                        "id": str(msg_id),
                        "sender": sender,
                        "key": data.get("memory_key", ""),
                        "value": data.get("memory_value"),
                        "timestamp": data.get("timestamp", 0),
                        "created_at": created_at.isoformat() if created_at else "",
                    })
                    # Update local cache
                    self._local_memory[data.get("memory_key", "")] = data.get("memory_value")
                except (json.JSONDecodeError, TypeError):
                    log.warning(f"Failed to parse memory sync payload {msg_id}")

            log.info(f"Synced {len(memories)} memory entries from PG")
            self._last_sync = time.time()
            return memories

        except Exception as e:
            log.error(f"Memory sync request failed: {e}")
            return []

    async def send_memory_to_agent(self, recipient: str, key: str, value: Any) -> bool:
        """Send a specific memory entry to a specific agent.

        Used for targeted memory sharing (not broadcast).
        """
        if not self.node._pg_transport or not self.node._pg_transport.is_available():
            log.warning("PG transport not available")
            return False

        from ..core.message import A2AMessage

        message = A2AMessage.create(
            sender=self.node.node_name,
            recipient=recipient,
            msg_type="memory_sync",
            payload={
                "memory_key": key,
                "memory_value": value,
                "timestamp": time.time(),
                "source": self.node.node_name,
            },
            priority=5,
        )

        result = await self.node._pg_transport.send(message)
        if result.success:
            log.info(f"Memory sent: {key} → {recipient} ({result.latency_ms:.1f}ms)")
            return True
        else:
            log.error(f"Memory send failed: {result.error}")
            return False

    def get_local_memory(self, key: str, default=None):
        """Get a value from local memory cache."""
        return self._local_memory.get(key, default)

    def set_local_memory(self, key: str, value):
        """Set a value in local memory cache."""
        self._local_memory[key] = value

    def get_all_local_memory(self) -> Dict:
        """Get all local memory entries."""
        return dict(self._local_memory)

    def handle_incoming_memory(self, payload: Dict):
        """Handle an incoming memory_sync message.

        Called by the message handler when a memory_sync message arrives.
        """
        key = payload.get("memory_key")
        value = payload.get("memory_value")
        source = payload.get("source", "unknown")

        if key:
            self._local_memory[key] = value
            log.info(f"Memory synced: {key} ← {source}")
            return True
        return False