"""Hindsight integration — save mesh delegation results to long-term memory.

When a delegation completes, the result is saved to Hindsight for later
context injection. This allows agents to "remember" past delegations and
use that knowledge in future tasks.

Usage:
    from core.hindsight_sync import HindsightSync
    hs = HindsightSync(node)
    await hs.save_delegation_result(task_row)
    context = await hs.get_context_for_prompt(subject)
"""
import json
import logging
import time
from typing import Optional, Dict, Any, List

log = logging.getLogger("mesh.hindsight_sync")


class HindsightSync:
    """Sync mesh delegation results to Hindsight long-term memory."""

    def __init__(self, node):
        self.node = node
        self._pg_pool = None
        self._enabled = getattr(node.config, 'hindsight_enabled', True)

    def set_pg_pool(self, pool):
        """Set PG pool for direct DB access."""
        self._pg_pool = pool

    async def save_delegation_result(self, task_row: dict) -> bool:
        """Save a completed delegation result to Hindsight via PG.

        Called when a delegation task completes. Stores the result
        in mesh.mesh_memory for later retrieval by context injection.
        """
        if not self._enabled or not self._pg_pool:
            return False

        try:
            task_id = task_row.get("task_id", "")
            from_agent = task_row.get("from_agent", "")
            to_agent = task_row.get("assigned_agent") or task_row.get("to_agent", "")
            subject = task_row.get("subject", "")
            result = task_row.get("result", "")
            status = task_row.get("status", "completed")

            if not result or not subject:
                return False

            # Store in mesh.mesh_memory table (create if not exists)
            await self._pg_pool.execute("""
                CREATE TABLE IF NOT EXISTS mesh.mesh_memory (
                    id SERIAL PRIMARY KEY,
                    memory_key TEXT NOT NULL,
                    memory_value TEXT NOT NULL,
                    source_agent TEXT NOT NULL,
                    target_agent TEXT,
                    memory_type TEXT DEFAULT 'delegation_result',
                    priority INTEGER DEFAULT 5,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    metadata JSONB DEFAULT '{}'
                )
            """)

            # Insert delegation result
            metadata = {
                "task_id": str(task_id),
                "from_agent": from_agent,
                "to_agent": to_agent,
                "status": status,
            }

            await self._pg_pool.execute("""
                INSERT INTO mesh.mesh_memory 
                    (memory_key, memory_value, source_agent, target_agent, memory_type, priority, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, f"delegation:{str(task_id)[:8]}", result[:5000], from_agent, to_agent,
                 "delegation_result", 5, json.dumps(metadata))

            log.info(f"Saved delegation result to mesh_memory: {subject[:50]} ({str(task_id)[:8]})")
            return True

        except Exception as e:
            log.error(f"Error saving delegation result to Hindsight: {e}")
            return False

    async def get_context_for_prompt(self, subject: str, limit: int = 5) -> str:
        """Retrieve relevant memory context for a given subject.

        Searches mesh.mesh_memory for delegation results matching the subject
        and returns a text summary for context injection into prompts.
        """
        if not self._enabled or not self._pg_pool:
            return ""

        try:
            # Simple keyword search — could be upgraded to vector search later
            rows = await self._pg_pool.fetch("""
                SELECT memory_value, source_agent, target_agent, created_at, metadata
                FROM mesh.mesh_memory
                WHERE memory_type = 'delegation_result'
                  AND (memory_key ILIKE $1 OR memory_value ILIKE $1)
                ORDER BY created_at DESC
                LIMIT $2
            """, f"%{subject[:50]}%", limit)

            if not rows:
                return ""

            lines = []
            for r in rows:
                ts = str(r["created_at"])[:19] if r["created_at"] else ""
                sender = r["source_agent"] or "?"
                value = r["memory_value"][:200] if r["memory_value"] else ""
                lines.append(f"[{ts}] {sender}: {value}")

            context = "\n".join(lines)
            log.info(f"Retrieved {len(rows)} memory entries for '{subject[:30]}'")
            return context

        except Exception as e:
            log.error(f"Error retrieving Hindsight context: {e}")
            return ""

    async def get_recent_memories(self, limit: int = 20) -> List[Dict]:
        """Get recent delegation results from mesh_memory."""
        if not self._enabled or not self._pg_pool:
            return []

        try:
            rows = await self._pg_pool.fetch("""
                SELECT memory_key, memory_value, source_agent, target_agent, 
                       memory_type, created_at, metadata
                FROM mesh.mesh_memory
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)

            return [dict(r) for r in rows] if rows else []

        except Exception as e:
            log.error(f"Error getting recent memories: {e}")
            return []