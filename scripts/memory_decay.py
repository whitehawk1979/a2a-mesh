#!/usr/bin/env python3
"""Memory decay — archive/delete old, unaccessed mesh_memory entries.

Rules (v0.40.0 — with LTP protection):
- Entries accessed in last 30 days: KEEP
- Entries created in last 7 days: KEEP
- Entries with access_count > 0: KEEP
- Entries with reference_count >= 3 (LTP-protected): KEEP — Morzsa javaslat
- Engramms (memory_type='engramm'): KEEP — matured conclusions are valuable
- Everything else: DELETE (older than 30d, never accessed, low refs)

Run weekly via cron.
"""
import asyncio
import asyncpg
import os
import sys
import json
import math
from datetime import datetime, timedelta

PG_DSN = os.environ.get("PG_DSN", "postgresql://nova:***@192.168.1.30:5432/agent_memory")

DECAY_DAYS = 30  # Base decay — entries older than this AND never accessed → delete
RECENT_KEEP_DAYS = 7  # Always keep entries newer than this
LTP_PROTECT_MIN_REFS = 3  # Entries with >=3 references are LTP-protected


async def run_decay():
    conn = await asyncpg.connect(PG_DSN)

    # Count before
    before = await conn.fetchval("SELECT count(*) FROM mesh.mesh_memory")

    # Delete old, never-accessed entries — but protect LTP-referenced and engramms
    cutoff = datetime.utcnow() - timedelta(days=DECAY_DAYS)
    recent_cutoff = datetime.utcnow() - timedelta(days=RECENT_KEEP_DAYS)

    # ── Asymmetric forgetting (Morzsa javaslat): ──
    # Delete content but preserve metadata for LTP-protected entries.
    # For entries with reference_count >= 3, archive (blank memory_value) instead of delete.
    
    # First: archive LTP-protected entries (blank content, keep metadata + embedding)
    archived = await conn.fetchval("""
        WITH archived AS (
            UPDATE mesh.mesh_memory
            SET memory_value = '[archived — content forgotten, metadata preserved]'
            WHERE created_at < $1
              AND created_at < $2
              AND memory_type = 'capsule'
              AND embedding IS NOT NULL
              AND metadata::text LIKE '%"reference_count":%'
              AND (metadata::json->>'reference_count')::int >= $3
              AND memory_value != '[archived — content forgotten, metadata preserved]'
            RETURNING id
        )
        SELECT count(*) FROM archived
    """, cutoff, recent_cutoff, LTP_PROTECT_MIN_REFS)
    
    # Then: delete non-LTP entries that are old and never accessed
    deleted = await conn.fetchval("""
        WITH deleted AS (
            DELETE FROM mesh.mesh_memory
            WHERE created_at < $1
              AND created_at < $2  -- older than recent_keep
              AND memory_type != 'engramm'  -- never delete engramms
              AND (access_count IS NULL OR access_count = 0)
              AND (last_accessed IS NULL OR last_accessed < $1)
              AND (
                NOT metadata::text LIKE '%"reference_count":%'
                OR (metadata::json->>'reference_count')::int < $3
              )
            RETURNING id
        )
        SELECT count(*) FROM deleted
    """, cutoff, recent_cutoff, LTP_PROTECT_MIN_REFS)

    # Count after
    after = await conn.fetchval("SELECT count(*) FROM mesh.mesh_memory")

    # Stats
    stats = await conn.fetchrow("""
        SELECT 
            count(CASE WHEN access_count > 0 THEN 1 END) as accessed,
            count(CASE WHEN access_count = 0 OR access_count IS NULL THEN 1 END) as unaccessed,
            count(CASE WHEN embedding IS NOT NULL THEN 1 END) as embedded,
            count(CASE WHEN memory_type = 'engramm' THEN 1 END) as engramms,
            count(CASE WHEN memory_type = 'capsule' THEN 1 END) as capsules,
            count(CASE WHEN memory_value = '[archived — content forgotten, metadata preserved]' THEN 1 END) as archived
        FROM mesh.mesh_memory
    """)

    print(f"Memory decay: {before} → {after} (deleted {deleted}, archived {archived})")
    print(f"  Accessed: {stats['accessed']}, Unaccessed: {stats['unaccessed']}, Embedded: {stats['embedded']}")
    print(f"  Engramms: {stats['engramms']}, Capsules: {stats['capsules']}, Archived (LTP): {stats['archived']}")

    await conn.close()
    return {"before": before, "after": after, "deleted": deleted, "archived": archived}


if __name__ == "__main__":
    result = asyncio.run(run_decay())
    sys.exit(0)