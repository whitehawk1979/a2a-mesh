"""
Marveen-inspired DB tables — Kanban comments, events, labels, task runs,
conversation log, daily logs, background tasks.

These extend the A2A Mesh with Marveen's audit trail and organizational features.
"""

import time
import logging
import json
from typing import Optional, List, Dict, Any

log = logging.getLogger("marveen_db")

# Lazy PG pool reference
_pg_pool = None


def _fix_dt(d: dict) -> dict:
    """Convert datetime objects to ISO strings for JSON serialization."""
    for k, v in d.items():
        if hasattr(v, 'isoformat'):
            d[k] = v.isoformat()
    return d


def set_pg_pool(pool):
    global _pg_pool
    _pg_pool = pool


# ── Kanban Comments ──────────────────────────────────────────────

async def add_kanban_comment(card_id: int, author: str, comment: str) -> Optional[int]:
    """Add a comment to a kanban card."""
    if not _pg_pool:
        return None
    try:
        row = await _pg_pool.fetchrow(
            "INSERT INTO kanban_comments (card_id, author, comment) VALUES ($1, $2, $3) RETURNING id",
            card_id, author, comment
        )
        # Log event
        await add_card_event(card_id, "commented", None, comment[:200], author)
        return row["id"] if row else None
    except Exception as e:
        log.warning(f"add_kanban_comment failed: {e}")
        return None


async def get_kanban_comments(card_id: int, limit: int = 50) -> List[Dict]:
    """Get comments for a kanban card."""
    if not _pg_pool:
        return []
    try:
        rows = await _pg_pool.fetch(
            "SELECT id, author, comment, created_at FROM kanban_comments WHERE card_id = $1 ORDER BY created_at ASC LIMIT $2",
            card_id, limit
        )
        return [_fix_dt(dict(r)) for r in rows]
    except Exception as e:
        log.warning(f"get_kanban_comments failed: {e}")
        return []


# ── Kanban Card Events ──────────────────────────────────────────

async def add_card_event(card_id: int, event_type: str, old_value: str = None,
                         new_value: str = None, actor: str = "system") -> Optional[int]:
    """Log a kanban card event (state change, assignment, etc.)."""
    if not _pg_pool:
        return None
    try:
        row = await _pg_pool.fetchrow(
            "INSERT INTO kanban_card_events (card_id, event_type, old_value, new_value, actor) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id",
            card_id, event_type, old_value, new_value, actor
        )
        return row["id"] if row else None
    except Exception as e:
        log.warning(f"add_card_event failed: {e}")
        return None


async def get_card_events(card_id: int, limit: int = 50) -> List[Dict]:
    """Get event history for a kanban card."""
    if not _pg_pool:
        return []
    try:
        rows = await _pg_pool.fetch(
            "SELECT id, event_type, old_value, new_value, actor, created_at "
            "FROM kanban_card_events WHERE card_id = $1 ORDER BY created_at DESC LIMIT $2",
            card_id, limit
        )
        return [_fix_dt(dict(r)) for r in rows]
    except Exception as e:
        log.warning(f"get_card_events failed: {e}")
        return []


# ── Labels ──────────────────────────────────────────────────────

async def create_label(name: str, color: str = "#6366f1") -> Optional[int]:
    """Create a kanban label."""
    if not _pg_pool:
        return None
    try:
        row = await _pg_pool.fetchrow(
            "INSERT INTO kanban_labels (name, color) VALUES ($1, $2) "
            "ON CONFLICT (name) DO UPDATE SET color = $2 RETURNING id",
            name, color
        )
        return row["id"] if row else None
    except Exception as e:
        log.warning(f"create_label failed: {e}")
        return None


async def list_labels() -> List[Dict]:
    """List all kanban labels."""
    if not _pg_pool:
        return []
    try:
        rows = await _pg_pool.fetch("SELECT id, name, color FROM kanban_labels ORDER BY name")
        return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"list_labels failed: {e}")
        return []


async def attach_label(card_id: int, label_id: int) -> bool:
    """Attach a label to a card."""
    if not _pg_pool:
        return False
    try:
        await _pg_pool.execute(
            "INSERT INTO kanban_card_labels (card_id, label_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            card_id, label_id
        )
        return True
    except Exception as e:
        log.warning(f"attach_label failed: {e}")
        return False


# ── Task Runs (execution audit trail) ──────────────────────────

async def start_task_run(task_id: str, agent: str, task_type: str = "delegation") -> Optional[int]:
    """Record the start of a task execution."""
    if not _pg_pool:
        return None
    try:
        row = await _pg_pool.fetchrow(
            "INSERT INTO task_runs (task_id, agent, status, started_at) "
            "VALUES ($1, $2, 'started', NOW()) RETURNING id",
            task_id, agent
        )
        return row["id"] if row else None
    except Exception as e:
        log.warning(f"start_task_run failed: {e}")
        return None


async def complete_task_run(run_id: int, status: str = "completed",
                            result_summary: str = None, error: str = None) -> bool:
    """Record the completion of a task execution."""
    if not _pg_pool:
        return False
    try:
        await _pg_pool.execute(
            "UPDATE task_runs SET status = $1, completed_at = NOW(), "
            "duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000, "
            "result_summary = $2, error = $3 WHERE id = $4",
            status, result_summary, error, run_id
        )
        return True
    except Exception as e:
        log.warning(f"complete_task_run failed: {e}")
        return False


async def get_task_runs(agent: str = None, limit: int = 50) -> List[Dict]:
    """Get task run history, optionally filtered by agent."""
    if not _pg_pool:
        return []
    try:
        if agent:
            rows = await _pg_pool.fetch(
                "SELECT id, task_id, agent, status, started_at, completed_at, "
                "duration_ms, result_summary, error FROM task_runs "
                "WHERE agent = $1 ORDER BY started_at DESC LIMIT $2",
                agent, limit
            )
        else:
            rows = await _pg_pool.fetch(
                "SELECT id, task_id, agent, status, started_at, completed_at, "
                "duration_ms, result_summary, error FROM task_runs "
                "ORDER BY started_at DESC LIMIT $1",
                limit
            )
        result = []
        for r in rows:
            d = dict(r)
            # Ensure JSON-serializable
            if d.get('started_at'):
                d['started_at'] = str(d['started_at'])
            if d.get('completed_at'):
                d['completed_at'] = str(d['completed_at'])
            result.append(_fix_dt(d))
        return result
    except Exception as e:
        log.warning(f"get_task_runs failed: {e}")
        return []


# ── Conversation Log ─────────────────────────────────────────────

async def log_conversation(agent: str, role: str, content: str,
                           turn_id: int = None, tokens: int = None) -> Optional[int]:
    """Log a conversation turn for an agent."""
    if not _pg_pool:
        return None
    try:
        row = await _pg_pool.fetchrow(
            "INSERT INTO conversation_log (agent, role, content, turn_id, tokens_used) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id",
            agent, role, content[:10000], turn_id, tokens
        )
        return row["id"] if row else None
    except Exception as e:
        log.warning(f"log_conversation failed: {e}")
        return None


async def get_conversation_log(agent: str, limit: int = 50) -> List[Dict]:
    """Get conversation history for an agent."""
    if not _pg_pool:
        return []
    try:
        rows = await _pg_pool.fetch(
            "SELECT id, role, content, turn_id, tokens_used, created_at "
            "FROM conversation_log WHERE agent = $1 ORDER BY created_at DESC LIMIT $2",
            agent, limit
        )
        return [_fix_dt(dict(r)) for r in rows]
    except Exception as e:
        log.warning(f"get_conversation_log failed: {e}")
        return []


# ── Daily Logs ──────────────────────────────────────────────────

async def update_daily_log(agent: str, date_str: str = None, **kwargs) -> bool:
    """Update or create a daily log entry for an agent."""
    if not _pg_pool:
        return False
    try:
        from datetime import date
        d = date.fromisoformat(date_str) if date_str else date.today()
        # Upsert
        sets = []
        values = [agent, d]
        idx = 3
        for key, val in kwargs.items():
            if key in ("summary", "tasks_completed", "tasks_failed", "tokens_used",
                       "delegations_sent", "delegations_received"):
                sets.append(f"{key} = ${idx}")
                values.append(val)
                idx += 1
        if not sets:
            return False
        sql = (f"INSERT INTO daily_logs (agent, date, {', '.join(k for k in kwargs if k in ('summary','tasks_completed','tasks_failed','tokens_used','delegations_sent','delegations_received'))}) "
               f"VALUES ($1, $2, {', '.join(f'${i}' for i in range(3, idx))}) "
               f"ON CONFLICT (agent, date) DO UPDATE SET {', '.join(sets)}")
        await _pg_pool.execute(sql, *values)
        return True
    except Exception as e:
        log.warning(f"update_daily_log failed: {e}")
        return False


async def get_daily_logs(agent: str = None, days: int = 7) -> List[Dict]:
    """Get daily logs for an agent or all agents."""
    if not _pg_pool:
        return []
    try:
        if agent:
            rows = await _pg_pool.fetch(
                "SELECT * FROM daily_logs WHERE agent = $1 AND date >= NOW() - make_interval(days => $2) ORDER BY date DESC",
                agent, days
            )
        else:
            rows = await _pg_pool.fetch(
                "SELECT * FROM daily_logs WHERE date >= NOW() - make_interval(days => $1) ORDER BY date DESC, agent",
                days
            )
        return [_fix_dt(dict(r)) for r in rows]
    except Exception as e:
        log.warning(f"get_daily_logs failed: {e}")
        return []


# ── Background Tasks ────────────────────────────────────────────

async def create_background_task(agent: str, task_type: str, payload: dict) -> Optional[int]:
    """Create a background task."""
    if not _pg_pool:
        return None
    try:
        row = await _pg_pool.fetchrow(
            "INSERT INTO background_tasks (agent, task_type, payload) "
            "VALUES ($1, $2, $3) RETURNING id",
            agent, task_type, json.dumps(payload)
        )
        return row["id"] if row else None
    except Exception as e:
        log.warning(f"create_background_task failed: {e}")
        return None


async def get_pending_background_tasks(agent: str = None, limit: int = 50) -> List[Dict]:
    """Get pending background tasks."""
    if not _pg_pool:
        return []
    try:
        if agent:
            rows = await _pg_pool.fetch(
                "SELECT id, agent, task_type, payload, created_at FROM background_tasks "
                "WHERE status = 'pending' AND agent = $1 ORDER BY created_at ASC LIMIT $2",
                agent, limit
            )
        else:
            rows = await _pg_pool.fetch(
                "SELECT id, agent, task_type, payload, created_at FROM background_tasks "
                "WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1",
                limit
            )
        return [_fix_dt(dict(r)) for r in rows]
    except Exception as e:
        log.warning(f"get_pending_background_tasks failed: {e}")
        return []


async def complete_background_task(task_id: int, status: str = "completed",
                                   result: dict = None) -> bool:
    """Mark a background task as completed or failed."""
    if not _pg_pool:
        return False
    try:
        await _pg_pool.execute(
            "UPDATE background_tasks SET status = $1, result = $2, completed_at = NOW() WHERE id = $3",
            status, json.dumps(result) if result else None, task_id
        )
        return True
    except Exception as e:
        log.warning(f"complete_background_task failed: {e}")
        return False


# ── Summary for dashboard ──────────────────────────────────────

async def get_marveen_db_status() -> Dict[str, Any]:
    """Get summary status of all Marveen DB tables."""
    if not _pg_pool:
        return {"error": "PG pool not initialized"}
    try:
        tables = {}
        for t in ["kanban_comments", "kanban_card_events", "kanban_labels",
                   "task_runs", "conversation_log", "daily_logs", "background_tasks"]:
            row = await _pg_pool.fetchrow(f"SELECT count(*) as c FROM {t}")
            tables[t] = row["c"] if row else 0
        return {"tables": tables, "total_tables": len(tables)}
    except Exception as e:
        return {"error": str(e)}


async def generate_daily_summary(pg_pool, agent: str = None) -> Dict:
    """Generate daily summary from task_runs and write to daily_logs.

    Called by a cron job once per day. Aggregates:
    - Total delegations, success rate, avg duration
    - Error count, most common errors
    - Agent ranking by throughput

    Returns the summary dict.
    """
    import json as _json
    import datetime as _dt
    if not pg_pool:
        return {"error": "No PG pool"}

    try:
        rows = await pg_pool.fetch(
            """SELECT agent,
                      COUNT(*) as total,
                      COUNT(CASE WHEN status='completed' THEN 1 END) as completed,
                      COUNT(CASE WHEN status='failed' THEN 1 END) as failed,
                      AVG(duration_ms) as avg_duration_ms,
                      MAX(duration_ms) as max_duration_ms
               FROM task_runs
               WHERE started_at >= CURRENT_DATE
               GROUP BY agent
               ORDER BY total DESC"""
        )

        total_delegations = 0
        total_completed = 0
        total_failed = 0
        agent_stats = []

        for r in rows:
            ag = r["agent"]
            total = r["total"]
            completed = r["completed"]
            failed = r["failed"]
            avg_dur = float(r["avg_duration_ms"]) if r["avg_duration_ms"] else 0
            max_dur = float(r["max_duration_ms"]) if r["max_duration_ms"] else 0
            success_rate = completed / total if total > 0 else 0

            total_delegations += total
            total_completed += completed
            total_failed += failed

            summary = (
                f"📊 {ag}: {total} tasks ({completed}✅ {failed}❌) "
                f"avg={avg_dur:.0f}ms max={max_dur:.0f}ms "
                f"success={success_rate:.0%}"
            )
            agent_stats.append({"agent": ag, "summary": summary,
                               "total": total, "completed": completed,
                               "failed": failed, "avg_ms": avg_dur})

            await pg_pool.execute(
                """INSERT INTO daily_logs (agent, date, summary, task_count, error_count, metadata)
                   VALUES ($1, CURRENT_DATE, $2, $3, $4, $5)
                   ON CONFLICT (agent, date) DO UPDATE SET
                     summary = $2, task_count = $3, error_count = $4, metadata = $5""",
                ag, summary, total, failed,
                _json.dumps({"avg_ms": avg_dur, "max_ms": max_dur,
                             "success_rate": success_rate}),
            )

        overall_rate = total_completed / total_delegations if total_delegations > 0 else 0
        overall_summary = (
            f"📅 Daily Summary: {total_delegations} delegations "
            f"({total_completed}✅ {total_failed}❌) "
            f"success_rate={overall_rate:.0%}"
        )

        log.info(f"Daily summary generated: {overall_summary}")
        return {
            "date": _dt.date.today().isoformat(),
            "total_delegations": total_delegations,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "success_rate": overall_rate,
            "agents": agent_stats,
            "summary": overall_summary,
        }
    except Exception as e:
        log.error(f"generate_daily_summary failed: {e}")
        return {"error": str(e)}