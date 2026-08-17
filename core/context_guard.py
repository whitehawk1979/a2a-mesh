"""
Context Guard — monitors context window saturation and triggers preventive actions.

Inspired by Marveen's Context Guard:
  - Monitors token usage / context size
  - At actPct (80%): suggests writing a handoff summary
  - At hardPct (95%): forces fresh restart with handoff injection
  - Saturation net: if 100% used, refuses dispatch + auto-restarts

For A2A Mesh:
  - Monitors delegations for context overflow signals
  - Tracks agent turn counts via delegation stats
  - If an agent exceeds max_turns threshold → suggest compaction
  - Logs warnings for dashboard visibility

Deterministic: no LLM needed for monitoring logic.
"""

import time
import logging

log = logging.getLogger("context_guard")

# Thresholds
ACT_PCT = 0.80      # Suggest handoff at 80% context
HARD_PCT = 0.95     # Force action at 95%
MAX_TURNS_DEFAULT = 80  # Default max turns before context guard triggers


def check_agent_context(agent_name, turns_used, max_turns=None):
    """Check if an agent is approaching context limits.
    Returns dict with status and recommended action."""
    max_t = max_turns or MAX_TURNS_DEFAULT
    pct = turns_used / max_t if max_t > 0 else 0

    if pct >= 1.0:
        return {
            "agent": agent_name,
            "status": "saturated",
            "pct": pct,
            "turns": turns_used,
            "max_turns": max_t,
            "action": "force_restart",
            "message": f"🔴 {agent_name} context SATURATED ({turns_used}/{max_t} turns) — force restart needed"
        }
    elif pct >= HARD_PCT:
        return {
            "agent": agent_name,
            "status": "critical",
            "pct": pct,
            "turns": turns_used,
            "max_turns": max_t,
            "action": "hard_restart",
            "message": f"🟠 {agent_name} context CRITICAL ({pct:.0%}) — handoff + restart"
        }
    elif pct >= ACT_PCT:
        return {
            "agent": agent_name,
            "status": "warning",
            "pct": pct,
            "turns": turns_used,
            "max_turns": max_t,
            "action": "suggest_handoff",
            "message": f"🟡 {agent_name} context WARNING ({pct:.0%}) — suggest writing handoff"
        }
    else:
        return {
            "agent": agent_name,
            "status": "ok",
            "pct": pct,
            "turns": turns_used,
            "max_turns": max_t,
            "action": "none",
            "message": None
        }


async def context_guard_tick(pg_pool, node_name="unknown"):
    """Periodic check of all agents' context usage.
    Called by self-healing loop."""
    results = []
    if not pg_pool or not pg_pool.is_connected():
        return results
    try:
        async with pg_pool.acquire() as conn:
            # Check active delegations for turn counts
            rows = await conn.fetch(
                """SELECT assigned_agent, 
                          COUNT(*) as active_tasks,
                          MAX(progress) as max_progress
                   FROM shared_delegations
                   WHERE status IN ('pending', 'running')
                   GROUP BY assigned_agent"""
            )
            for r in rows:
                agent = r["assigned_agent"] or "unknown"
                # Estimate context usage from active task count
                active = r["active_tasks"]
                # Use active tasks as proxy for context pressure
                check = check_agent_context(agent, active * 10, MAX_TURNS_DEFAULT)
                if check["action"] != "none":
                    log.warning(f"Context Guard: {check['message']}")
                    results.append(check)
    except Exception as e:
        log.debug(f"Context guard tick error: {e}")
    return results


def generate_handoff_prompt(agent_name, current_task, context_summary):
    """Generate a handoff prompt for context restart.
    This prompt is injected after a fresh restart so the agent can continue."""
    return f"""## Context Handoff — {agent_name}

### Previous Task
{current_task}

### Context Summary
{context_summary}

### Instruction
You are continuing from a context restart. The above summary contains the key
decisions, progress, and next steps from your previous session. Pick up where
you left off. Do not repeat completed work.
"""