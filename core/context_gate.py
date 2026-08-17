"""
Context Restart Gate — proactive context clearing before saturation.

Inspired by Marveen's context-restart-gate:
  - Monitors context token count
  - At thresholdTokens → suggests /clear (soft restart)
  - SessionStart hooks replay context after clear
  - Prevents expensive in-TUI auto-compact

For A2A Mesh:
  - Tracks delegation context sizes
  - Suggests compaction at threshold (default 80% of max_turns)
  - Logs warnings for dashboard visibility
"""

import time
import logging

log = logging.getLogger("context_gate")

THRESHOLD_TURNS = 70  # Suggest clear at 70 turns
HARD_THRESHOLD = 85   # Force clear at 85 turns


def check_gate(node_name, current_turns, max_turns=90):
    """Check if context restart is needed.
    Returns dict with recommendation."""
    threshold = max_turns * (THRESHOLD_TURNS / 100)
    hard = max_turns * (HARD_THRESHOLD / 100)
    pct = (current_turns / max_turns) * 100 if max_turns > 0 else 0
    
    if current_turns >= hard:
        return {
            "node": node_name,
            "action": "force_clear",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(pct, 1),
            "severity": "critical",
            "message": f"🔴 {node_name}: {current_turns}/{max_turns} turns — FORCE context clear"
        }
    elif current_turns >= threshold:
        return {
            "node": node_name,
            "action": "suggest_clear",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(pct, 1),
            "severity": "warning",
            "message": f"🟡 {node_name}: {current_turns}/{max_turns} turns — suggest context clear"
        }
    return {
        "node": node_name,
        "action": "none",
        "turns": current_turns,
        "max_turns": max_turns,
        "pct": round(pct, 1),
        "severity": "ok",
        "message": None
    }


async def context_gate_tick(pg_pool):
    """Periodic check for all agents' context gates."""
    results = []
    if not pg_pool or not pg_pool.is_connected():
        return results
    try:
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
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
                    log.warning(f"Context gate: {check['message']}")
                    results.append(check)
    except Exception as e:
        log.debug(f"Context gate tick error: {e}")
    return results