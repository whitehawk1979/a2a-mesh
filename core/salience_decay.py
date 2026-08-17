"""
Salience Decay — memory importance fading over time.

Concept (from Marveen):
  - Frequently accessed memories stay "hot" (high importance)
  - Unused memories slowly fade (importance decreases)
  - Prevents token bloat from stale memories
  - Auto-promotes frequently used memories

Decay formula:
  new_importance = max(base_importance, importance - decay_rate * hours_since_access)
  
  where decay_rate depends on category:
    - user preferences: 0 (never decay — critical)
    - environment facts: 0.1/hour (slow decay)
    - project state: 0.2/hour (medium decay)
    - general notes: 0.5/hour (fast decay)

Access boost:
  On each recall, importance += 5 (capped at 100)
  access_count += 1
  last_accessed_at = now()

Usage:
  Called by the self-healing loop every hour to apply decay.
  Called on memory recall to boost importance.
"""

import logging
import asyncio
from datetime import datetime, timezone

log = logging.getLogger("salience_decay")

# Decay rates per category (importance points per hour)
DECAY_RATES = {
    "user": 0.0,       # User preferences never decay
    "memory": 0.1,     # Environment facts — slow decay
    "project": 0.2,    # Project state — medium decay
    "skill": 0.0,      # Skills never decay
    "general": 0.5,    # General notes — fast decay
}

DEFAULT_DECAY = 0.3  # Default decay rate for unknown categories
MIN_IMPORTANCE = 10  # Never go below this
MAX_IMPORTANCE = 100
ACCESS_BOOST = 5     # Importance boost on each recall


async def apply_decay(pg_pool, hours: float = 1.0):
    """
    Apply salience decay to all memories.
    Called periodically by the self-healing loop.
    
    Args:
        pg_pool: asyncpg connection pool
        hours: hours since last decay (usually 1)
    
    Returns:
        dict: {total_decayed, min_importance, max_importance}
    """
    try:
        # Apply decay based on time since last access
        # Memories accessed recently decay less
        for category, rate in DECAY_RATES.items():
            if rate == 0:
                continue
            decay_amount = rate * hours
            await pg_pool.execute(
                """UPDATE agent_memory 
                   SET importance = GREATEST($1, importance - $2 * 
                       LEAST(hours_since_access, 24))
                   WHERE collection = $3 
                   AND importance > $1""",
                MIN_IMPORTANCE, decay_amount, "shared"
            )
        
        # Simpler approach: decay all non-accessed memories
        await pg_pool.execute(
            """UPDATE agent_memory 
               SET importance = GREATEST($1, 
                   importance - $2 * 
                   EXTRACT(EPOCH FROM (NOW() - COALESCE(last_accessed_at, updated_at, created_at))) / 3600.0)
               WHERE importance > $1
               AND category NOT IN ('user', 'skill')""",
            MIN_IMPORTANCE, DEFAULT_DECAY
        )
        
        # Get stats
        stats = await pg_pool.fetchrow(
            """SELECT 
               COUNT(*) as total,
               MIN(importance) as min_imp,
               MAX(importance) as max_imp,
               AVG(importance) as avg_imp,
               COUNT(*) FILTER (WHERE last_accessed_at IS NOT NULL) as accessed
               FROM agent_memory"""
        )
        
        result = {
            "total": stats["total"],
            "min_importance": stats["min_imp"],
            "max_importance": stats["max_imp"],
            "avg_importance": round(stats["avg_imp"], 1) if stats["avg_imp"] else 0,
            "accessed": stats["accessed"],
        }
        log.info(f"📊 Salience decay applied: {result}")
        return result
        
    except Exception as e:
        log.error(f"Salience decay error: {e}")
        return None


async def boost_on_recall(pg_pool, memory_id: int):
    """
    Boost memory importance on recall.
    Called when a memory is retrieved via search.
    
    Args:
        pg_pool: asyncpg connection pool
        memory_id: The ID of the recalled memory
    """
    try:
        await pg_pool.execute(
            """UPDATE agent_memory 
               SET access_count = access_count + 1,
                   last_accessed_at = NOW(),
                   importance = LEAST($1, importance + $2)
               WHERE id = $3""",
            MAX_IMPORTANCE, ACCESS_BOOST, memory_id
        )
    except Exception as e:
        log.debug(f"Memory boost failed for id={memory_id}: {e}")


async def get_hot_memories(pg_pool, limit: int = 10):
    """
    Get the most important (hottest) memories.
    Used for context injection — only inject hot memories to save tokens.
    
    Args:
        pg_pool: asyncpg connection pool
        limit: Max number of memories to return
    
    Returns:
        List of memory dicts sorted by importance
    """
    try:
        rows = await pg_pool.fetch(
            """SELECT id, title, content, importance, access_count, 
                      category, updated_at, last_accessed_at
               FROM agent_memory
               WHERE importance >= $1
               ORDER BY importance DESC, last_accessed_at DESC NULLS LAST
               LIMIT $2""",
            MIN_IMPORTANCE + 20,  # Only memories above threshold
            limit
        )
        return [dict(r) for r in rows]
    except Exception as e:
        log.error(f"Get hot memories error: {e}")
        return []


def run_decay_sync(pg_dsn: str = "postgresql://nova:nova_agent_2026@192.168.1.30:5432/agent_memory"):
    """
    Synchronous wrapper for running decay from cron or CLI.
    
    Usage:
        python3 -c "from core.salience_decay import run_decay_sync; run_decay_sync()"
    """
    import asyncio
    import asyncpg
    
    async def _run():
        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        try:
            result = await apply_decay(pool, hours=1.0)
            print(f"Decay result: {result}")
        finally:
            await pool.close()
    
    asyncio.run(_run())