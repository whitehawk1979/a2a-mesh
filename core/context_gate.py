"""
Context Restart Gate — proactive context clearing before saturation.

Inspired by Marveen's context-restart-gate:
  - Monitors context token count per agent
  - At thresholdTokens → suggests /clear (soft restart)
  - At hardThreshold → force clear + context replay
  - SessionStart hooks replay context after clear
  - Prevents expensive in-TUI auto-compact
  - Context priority: preserve important context during compaction

For A2A Mesh:
  - Tracks delegation context sizes per agent
  - Estimates token usage from active delegations + memory
  - Suggests compaction at threshold (default 70% of max_turns)
  - Force clear at 85%
  - Dashboard: real-time saturation meters per agent
  - Alert: Telegram notification on critical saturation
"""

import time
import logging
import json

log = logging.getLogger("context_gate")

# Thresholds (percentage of max_turns)
SOFT_THRESHOLD_PCT = 70   # Suggest clear at 70%
HARD_THRESHOLD_PCT = 85   # Force clear at 85%
CRITICAL_PCT = 95         # Critical alert

# Token estimation: average tokens per turn (rough estimate)
TOKENS_PER_TURN = 3500    # ~3500 tokens per turn including tool output
TOKENS_PER_DELEGATION = 5000  # Base context per active delegation

# Context priority levels for preservation during compaction
PRIORITY_CRITICAL = ["system", "identity", "active_task", "user_preference"]
PRIORITY_HIGH = ["recent_memory", "skill_reference", "delegation_context"]
PRIORITY_LOW = ["old_conversation", "tool_output", "search_result"]


def check_gate(node_name, current_turns, max_turns=90):
    """Check if context restart is needed.
    Returns dict with recommendation and token estimate."""
    threshold = int(max_turns * (SOFT_THRESHOLD_PCT / 100))
    hard = int(max_turns * (HARD_THRESHOLD_PCT / 100))
    critical = int(max_turns * (CRITICAL_PCT / 100))
    pct = (current_turns / max_turns) * 100 if max_turns > 0 else 0
    est_tokens = current_turns * TOKENS_PER_TURN

    if current_turns >= critical:
        return {
            "node": node_name,
            "action": "critical_alert",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(pct, 1),
            "est_tokens": est_tokens,
            "severity": "critical",
            "message": "🔴 " + node_name + ": " + str(current_turns) + "/" + str(max_turns) + " turns (" + str(round(pct, 1)) + "%) — CRITICAL context saturation"
        }
    elif current_turns >= hard:
        return {
            "node": node_name,
            "action": "force_clear",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(pct, 1),
            "est_tokens": est_tokens,
            "severity": "critical",
            "message": "🔴 " + node_name + ": " + str(current_turns) + "/" + str(max_turns) + " turns — FORCE context clear"
        }
    elif current_turns >= threshold:
        return {
            "node": node_name,
            "action": "suggest_clear",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(pct, 1),
            "est_tokens": est_tokens,
            "severity": "warning",
            "message": "🟡 " + node_name + ": " + str(current_turns) + "/" + str(max_turns) + " turns — suggest context clear"
        }
    return {
        "node": node_name,
        "action": "none",
        "turns": current_turns,
        "max_turns": max_turns,
        "pct": round(pct, 1),
        "est_tokens": est_tokens,
        "severity": "ok",
        "message": None
    }


def get_priority_context(agent_name, pg_pool_data=None):
    """Determine which context items to preserve during compaction.
    Returns list of priority context items for replay after clear."""
    preserve = []
    # Always preserve system-level context
    preserve.append({"type": "system", "priority": "critical", "content": "agent identity + core instructions"})
    # Active tasks
    if pg_pool_data and pg_pool_data.get("active_delegations"):
        for d in pg_pool_data["active_delegations"][:3]:
            preserve.append({
                "type": "active_task",
                "priority": "critical",
                "content": d.get("task_type", "unknown") + ": " + str(d.get("description", ""))[:100]
            })
    # Recent high-importance memories
    if pg_pool_data and pg_pool_data.get("recent_memories"):
        for m in pg_pool_data["recent_memories"][:5]:
            preserve.append({
                "type": "recent_memory",
                "priority": "high",
                "content": m
            })
    return preserve


async def context_gate_tick(pg_pool):
    """Periodic check for all agents' context gates.
    Uses AsyncDBPool API (fetch/fetchval/execute — no acquire())."""
    results = []
    if not pg_pool:
        return results
    try:
        rows = await pg_pool.fetch(
            """SELECT assigned_agent,
                      COUNT(*) as active,
                      COALESCE(SUM(progress), 0) as total_progress
               FROM shared_delegations
               WHERE status IN ('pending', 'running')
               GROUP BY assigned_agent"""
        )
        for r in rows:
            agent = r["assigned_agent"] or "unknown"
            # Estimate turns from active delegations
            est_turns = r["active"] * 15 + r["total_progress"] // 10
            check = check_gate(agent, est_turns)
            if check["action"] != "none":
                log.warning("Context gate: " + check["message"])
                results.append(check)
    except Exception as e:
        log.debug("Context gate tick error: " + str(e))
    return results


async def get_context_status(pg_pool=None, node_name=None):
    """Get detailed context gate status for dashboard.
    Returns per-agent saturation data."""
    agents_status = []
    if not pg_pool:
        return {"agents": [], "error": "PG unavailable"}

    try:
        # Get active delegation counts per agent
        rows = await pg_pool.fetch(
            """SELECT assigned_agent,
                      COUNT(*) as active,
                      COALESCE(SUM(progress), 0) as total_progress,
                      COUNT(*) FILTER (WHERE status = 'running') as running,
                      COUNT(*) FILTER (WHERE status = 'pending') as pending
               FROM shared_delegations
               WHERE status IN ('pending', 'running')
               GROUP BY assigned_agent"""
        )

        for r in rows:
            agent = r["assigned_agent"] or "unknown"
            est_turns = r["active"] * 15 + r["total_progress"] // 10
            check = check_gate(agent, est_turns, 90)

            agents_status.append({
                "agent": agent,
                "turns": check["turns"],
                "max_turns": check["max_turns"],
                "pct": check["pct"],
                "est_tokens": check["est_tokens"],
                "action": check["action"],
                "severity": check["severity"],
                "message": check["message"],
                "active_delegations": r["active"],
                "running": r["running"],
                "pending": r["pending"],
                "progress": r["total_progress"],
            })

        # Also check agents with 0 active delegations (idle = healthy)
        all_agents_rows = await pg_pool.fetch(
            """SELECT DISTINCT assigned_agent FROM shared_delegations
               WHERE assigned_agent IS NOT NULL"""
        )
        active_agents = set(r["assigned_agent"] for r in rows if r["assigned_agent"])
        for r in all_agents_rows:
            agent = r["assigned_agent"]
            if agent not in active_agents:
                agents_status.append({
                    "agent": agent,
                    "turns": 0,
                    "max_turns": 90,
                    "pct": 0,
                    "est_tokens": 0,
                    "action": "none",
                    "severity": "ok",
                    "message": None,
                    "active_delegations": 0,
                    "running": 0,
                    "pending": 0,
                    "progress": 0,
                })

        # Sort by severity (critical first)
        severity_order = {"critical": 0, "warning": 1, "ok": 2}
        agents_status.sort(key=lambda a: severity_order.get(a["severity"], 3))

        # Summary
        total = len(agents_status)
        critical = sum(1 for a in agents_status if a["severity"] == "critical")
        warning = sum(1 for a in agents_status if a["severity"] == "warning")
        healthy = total - critical - warning

        return {
            "agents": agents_status,
            "summary": {
                "total": total,
                "critical": critical,
                "warning": warning,
                "healthy": healthy,
            },
            "thresholds": {
                "soft": SOFT_THRESHOLD_PCT,
                "hard": HARD_THRESHOLD_PCT,
                "critical": CRITICAL_PCT,
            },
        }
    except Exception as e:
        log.debug("Context status error: " + str(e))
        return {"agents": [], "error": str(e)}