#!/usr/bin/env python3
"""Memory decay — archive/delete old, unaccessed mesh_memory entries.

Rules:
- Entries accessed in last 30 days: KEEP
- Entries created in last 7 days: KEEP
- Entries with access_count > 0: KEEP
- Everything else: DELETE (older than 30d, never accessed)

Run weekly via cron.
"""
import asyncio
import asyncpg
import os
import sys
import json
from datetime import datetime, timedelta

PG_DSN = os.environ.get("PG_DSN", "postgresql://nova:nova_agent_2026@192.168.1.30:5432/agent_memory")

DECAY_DAYS = 30  # Entries older than this AND never accessed → delete
RECENT_KEEP_DAYS = 7  # Always keep entries newer than this


async def run_decay():
    conn = await asyncpg.connect(PG_DSN)

    # Count before
    before = await conn.fetchval("SELECT count(*) FROM mesh.mesh_memory")

    # Delete old, never-accessed entries
    cutoff = datetime.utcnow() - timedelta(days=DECAY_DAYS)
    recent_cutoff = datetime.utcnow() - timedelta(days=RECENT_KEEP_DAYS)

    deleted = await conn.fetchval("""
        WITH deleted AS (
            DELETE FROM mesh.mesh_memory
            WHERE created_at < $1
              AND created_at < $2  -- older than recent_keep
              AND (access_count IS NULL OR access_count = 0)
              AND (last_accessed IS NULL OR last_accessed < $1)
            RETURNING id
        )
        SELECT count(*) FROM deleted
    """, cutoff, recent_cutoff)

    # Count after
    after = await conn.fetchval("SELECT count(*) FROM mesh.mesh_memory")

    # Stats
    stats = await conn.fetchrow("""
        SELECT 
            count(CASE WHEN access_count > 0 THEN 1 END) as accessed,
            count(CASE WHEN access_count = 0 OR access_count IS NULL THEN 1 END) as unaccessed,
            count(CASE WHEN embedding IS NOT NULL THEN 1 END) as embedded
        FROM mesh.mesh_memory
    """)

    print(f"Memory decay: {before} → {after} (deleted {deleted})")
    print(f"  Accessed: {stats['accessed']}, Unaccessed: {stats['unaccessed']}, Embedded: {stats['embedded']}")

    await conn.close()
    return {"before": before, "after": after, "deleted": deleted}


if __name__ == "__main__":
    result = asyncio.run(run_decay())
    sys.exit(0)