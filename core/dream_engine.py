"""
Dream Engine — Nightly analysis loop for A2A Mesh.

Inspired by Marveen's Dream Engine:
  Runs at 02:00 AM (configurable). Analyzes the day's:
  1. Memories — skill suggestions from repeated patterns
  2. Memory health — unvectorized, stale hot-tier → cold
  3. Kanban — stuck tasks, archivable done cards
  4. Errors — recurring patterns → skill/skill-update
  5. Synthesis — 4 prioritized action suggestions for morning

Output: DREAM.md file + morning brief delivered via Telegram.

Deterministic core (no LLM dependency):
  - SQL queries against Brain PG + Kanban JSON
  - Pattern detection via frequency analysis
  - File generation (DREAM.md)
  - Optional: LLM-powered synthesis (if available)
"""

import asyncio
import json
import os
import time
import logging
from datetime import datetime, timezone

log = logging.getLogger("dream_engine")

DREAM_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/DREAM.md")


async def run_dream_cycle(pg_pool, node_name="unknown", kanban_mgr=None):
    """Run one dream cycle. Returns dict with analysis results."""
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node": node_name,
        "buckets": {}
    }

    # Bucket 1: Memory patterns — find repeated operations
    results["buckets"]["memory_patterns"] = await _analyze_memory_patterns(pg_pool)

    # Bucket 2: Memory health — unvectorized + stale hot-tier
    results["buckets"]["memory_health"] = await _check_memory_health(pg_pool)

    # Bucket 3: Kanban — stuck tasks + archivable
    if kanban_mgr:
        results["buckets"]["kanban"] = await _analyze_kanban(kanban_mgr)
    else:
        results["buckets"]["kanban"] = {"stuck": [], "archivable": 0}

    # Bucket 4: Error patterns from PG
    results["buckets"]["errors"] = await _analyze_errors(pg_pool)

    # Bucket 5: Agent performance (Marveen)
    results["buckets"]["agent_performance"] = await _analyze_agent_performance(pg_pool)

    # Bucket 6: Skill usage (Marveen)
    results["buckets"]["skill_usage"] = await _analyze_skill_usage(pg_pool)

    # Bucket 7: Cost analysis (Marveen)
    results["buckets"]["cost"] = await _analyze_cost(pg_pool)

    # Generate DREAM.md
    dream_md = _generate_dream_md(results)
    try:
        os.makedirs(os.path.dirname(DREAM_FILE), exist_ok=True)
        with open(DREAM_FILE, "w", encoding="utf-8") as f:
            f.write(dream_md)
        log.info(f"Dream Engine: DREAM.md written to {DREAM_FILE}")
    except Exception as e:
        log.error(f"Dream Engine: Failed to write DREAM.md: {e}")

    # Save to PG for dashboard status
    try:
        if pg_pool:
            await pg_pool.execute(
                """INSERT INTO shared_a2a_memory (sender_agent, recipient_agent, subject, content, memory_type, priority)
                   VALUES ($1, $1, 'dream_engine', $2, 'observation', 3)""",
                node_name, dream_md[:5000]
            )
            log.info("Dream Engine: results saved to PG")
    except Exception as e:
        log.debug(f"Dream Engine: PG save skipped: {e}")

    return results


async def _analyze_memory_patterns(pg_pool):
    """Find repeated memory patterns that could become skills."""
    if not pg_pool:
        return {"suggestions": [], "total_memories": 0}
    try:
        # Count memories by category in last 24h
        rows = await pg_pool.fetch(
            """SELECT category, COUNT(*) as cnt
               FROM agent_memory
               WHERE created_at > NOW() - INTERVAL '24 hours'
               GROUP BY category ORDER BY cnt DESC"""
        )
        total = sum(r["cnt"] for r in rows)
        cats = {r["category"]: r["cnt"] for r in rows}

        # Find frequently accessed memories (potential skill candidates)
        hot = await pg_pool.fetch(
            """SELECT title, importance, access_count
               FROM agent_memory
               WHERE access_count > 3 AND importance >= 70
               ORDER BY access_count DESC LIMIT 10"""
        )
        suggestions = []
        for m in hot:
            if m["access_count"] >= 5:
                suggestions.append({
                    "title": m["title"] or "(untitled)",
                    "access_count": m["access_count"],
                    "importance": m["importance"],
                    "recommendation": "Consider creating a SKILL.md — accessed 5+ times"
                })
        return {"suggestions": suggestions, "total_memories": total, "categories": cats}
    except Exception as e:
        log.debug(f"Dream memory patterns error: {e}")
        return {"suggestions": [], "total_memories": 0, "error": str(e)}


async def _check_memory_health(pg_pool):
    """Check memory vectorization + stale hot-tier."""
    if not pg_pool:
        return {"unvectorized": 0, "stale_hot": 0}
    try:
        unvec = await pg_pool.fetchval("SELECT COUNT(*) FROM agent_memory WHERE embedding IS NULL")
        stale = await pg_pool.fetchval(
            """SELECT COUNT(*) FROM agent_memory
               WHERE importance >= 70
               AND COALESCE(last_accessed_at, updated_at) < NOW() - INTERVAL '7 days'"""
        )
        return {"unvectorized": unvec or 0, "stale_hot": stale or 0}
    except Exception as e:
        log.debug(f"Dream memory health error: {e}")
        return {"unvectorized": 0, "stale_hot": 0, "error": str(e)}


async def _analyze_kanban(kanban_mgr):
    """Find stuck + archivable kanban cards."""
    try:
        audit = await kanban_mgr.audit_stale_cards()
        return {
            "stuck": audit.get("stale_cards", []),
            "archivable": audit.get("dead_count", 0),
            "total_cards": audit.get("total_cards", 0),
        }
    except Exception as e:
        return {"stuck": [], "archivable": 0, "error": str(e)}


async def _analyze_errors(pg_pool):
    """Find recurring error patterns."""
    if not pg_pool:
        return {"recurring": [], "total_errors": 0}
    try:
        exists = await pg_pool.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='error_log')"
        )
        if not exists:
            return {"recurring": [], "total_errors": 0}
        rows = await pg_pool.fetch(
            """SELECT substring(content from 1 for 80) as err_prefix, COUNT(*) as cnt
               FROM error_log
               WHERE created_at > NOW() - INTERVAL '24 hours'
               GROUP BY err_prefix
               HAVING COUNT(*) > 2
               ORDER BY cnt DESC LIMIT 5"""
        )
        recurring = [{"pattern": r["err_prefix"], "count": r["cnt"]} for r in rows]
        total = sum(r["cnt"] for r in rows)
        return {"recurring": recurring, "total_errors": total}
    except Exception as e:
        log.debug(f"Dream error analysis error: {e}")
        return {"recurring": [], "total_errors": 0, "error": str(e)}


async def _analyze_agent_performance(pg_pool):
    """Analyze per-agent performance metrics from delegations."""
    if not pg_pool:
        return {"agents": [], "best": None, "worst": None}
    try:
        rows = await pg_pool.fetch(
            """SELECT to_agent,
                      COUNT(*) as total,
                      COUNT(*) FILTER (WHERE status = 'completed') as completed,
                      COUNT(*) FILTER (WHERE status = 'failed') as failed,
                      AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - created_at))) as avg_duration_s
               FROM shared_delegations
               WHERE created_at > NOW() - INTERVAL '24 hours'
               GROUP BY to_agent ORDER BY total DESC"""
        )
        agents = []
        for r in rows:
            total = r["total"] or 0
            completed = r["completed"] or 0
            failed = r["failed"] or 0
            sr = (completed / total * 100) if total > 0 else 0
            agents.append({
                "agent": r["to_agent"],
                "total": total,
                "completed": completed,
                "failed": failed,
                "success_rate": round(sr, 1),
                "avg_duration_s": round(r["avg_duration_s"] or 0, 1),
            })
        best = max(agents, key=lambda a: a["success_rate"]) if agents else None
        worst = min(agents, key=lambda a: a["success_rate"]) if agents else None
        return {"agents": agents, "best": best, "worst": worst}
    except Exception as e:
        log.debug(f"Dream agent performance error: {e}")
        return {"agents": [], "error": str(e)}


async def _analyze_skill_usage(pg_pool):
    """Analyze which skills are used vs idle."""
    if not pg_pool:
        return {"total": 0, "active": 0, "idle": [], "auto_generated": 0}
    try:
        total = await pg_pool.fetchval("SELECT COUNT(*) FROM mesh.mesh_skills WHERE status = 'active'") or 0
        auto = await pg_pool.fetchval("SELECT COUNT(*) FROM mesh.mesh_skills WHERE 'auto' = ANY(tags)") or 0
        idle = await pg_pool.fetch(
            """SELECT skill_name, agent_name FROM mesh.mesh_skills
               WHERE status = 'active' AND avg_latency_ms = 0 AND cost = 0
               LIMIT 10"""
        )
        return {
            "total": total,
            "auto_generated": auto,
            "idle": [{"skill": r["skill_name"], "agent": r["agent_name"]} for r in idle],
            "idle_count": len(idle),
        }
    except Exception as e:
        log.debug(f"Dream skill usage error: {e}")
        return {"total": 0, "error": str(e)}


async def _analyze_cost(pg_pool=None):
    """Analyze cost and token usage from CostOps ledger."""
    try:
        from .costops import get_monthly_summary
        from .token_usage import get_summary as get_token_summary
        cost_summary = get_monthly_summary()
        token_summary = get_token_summary()
        lines = []
        if cost_summary["total_requests"] > 0:
            lines.append(f"💰 **Költség (hó):** ${cost_summary['total_cost_usd']:.4f}")
            lines.append(f"   Token: {cost_summary['total_input_tokens']:,} in / {cost_summary['total_output_tokens']:,} out")
            for agent, cost in list(cost_summary["by_agent"].items())[:5]:
                lines.append(f"   {agent}: ${cost:.4f}")
        else:
            lines.append("💰 Nincs költség adat (még)")
        
        if token_summary["total_requests"] > 0:
            lines.append(f"📊 Token használat: {token_summary['total_tokens']:,} total ({token_summary['total_requests']} kérés)")
            for agent, usage in list(token_summary.get("by_agent", {}).items())[:5]:
                lines.append(f"   {agent}: {usage.get('input',0)+usage.get('output',0):,} tokens")
        
        return "\n".join(lines) if lines else "Nincs költség adat"
    except Exception as e:
        return {"error":str(e)}


def _generate_dream_md(results):
    """Generate DREAM.md from analysis results."""
    ts = results["timestamp"]
    node = results["node"]
    b = results["buckets"]

    lines = [
        f"# 🌙 Dream Engine — {ts}",
        f"**Node:** {node}",
        "",
        "## 💡 Bucket 1 — Skill javaslatok",
        "",
    ]

    mp = b.get("memory_patterns", {})
    if mp.get("suggestions"):
        for s in mp["suggestions"]:
            lines.append(f"- **{s['title']}** (access={s['access_count']}, imp={s['importance']}) — {s['recommendation']}")
    else:
        lines.append("*(nincs skill-javaslat)*")
    lines.append(f"\nÖsszes memória (24h): {mp.get('total_memories', 0)}")

    lines.extend([
        "",
        "## 🧹 Bucket 2 — Memória egészség",
        "",
        f"- Vektorizálatlan: {b.get('memory_health', {}).get('unvectorized', 0)}",
        f"- Stale hot-tier (>7nap): {b.get('memory_health', {}).get('stale_hot', 0)}",
    ])

    lines.extend([
        "",
        "## 📋 Bucket 3 — Kanban audit",
        "",
    ])
    kb = b.get("kanban", {})
    if kb.get("stuck"):
        for s in kb["stuck"]:
            lines.append(f"- ⚠️ Beragadt: {s}")
    else:
        lines.append("*(nincs beragadt task)*")
    lines.append(f"\nArchiválható (done >7nap): {kb.get('archivable', 0)}")

    lines.extend([
        "",
        "## 🔴 Bucket 4 — Hibák",
        "",
    ])
    err = b.get("errors", {})
    if err.get("recurring"):
        for e in err["recurring"]:
            lines.append(f"- **{e['count']}x**: {e['pattern']}")
    else:
        lines.append("*(nincs visszatérő hiba)*")

    # Bucket 5: Agent performance
    lines.extend(["", "## 🤖 Bucket 5 — Agent teljesítmény", ""])
    ap = b.get("agent_performance", {})
    if ap.get("agents"):
        for a in ap["agents"]:
            lines.append(f"- **{a['agent']}**: {a['total']} task, {a['success_rate']}% siker, {a['avg_duration_s']}s átlag")
        if ap.get("best"):
            lines.append(f"\n🏆 Legjobb: {ap['best']['agent']} ({ap['best']['success_rate']}%)")
        if ap.get("worst"):
            lines.append(f"⚠️ Legrosszabb: {ap['worst']['agent']} ({ap['worst']['success_rate']}%)")
    else:
        lines.append("*(nincs adat az elmúlt 24órában)*")

    # Bucket 6: Skill usage
    lines.extend(["", "## ⭐ Bucket 6 — Skill használat", ""])
    su = b.get("skill_usage", {})
    lines.append(f"- Összes aktív skill: {su.get('total', 0)}")
    lines.append(f"- Auto-generált: {su.get('auto_generated', 0)}")
    if su.get("idle"):
        lines.append(f"- Tétlen skill-ek ({su.get('idle_count', 0)}):")
        for s in su["idle"][:5]:
            lines.append(f"  - {s['skill']} ({s['agent']})")

    # Bucket 7: Cost
    lines.extend(["", "## 💰 Bucket 7 — Költség", ""])
    cost = b.get("cost", {})
    if not isinstance(cost, dict):
        lines.append(f"- WARNING: cost bucket returned non-dict: {str(cost)[:200]}")
    elif cost.get("error"):
        lines.append(f"- WARNING: {cost["error"]}")
    else:
        lines.append(f"- Monthly total: ${cost.get("month_total", 0):.4f} ({cost.get("total_requests", 0)} reqs)")
        lines.append(f"- Tokens: {cost.get("total_input_tokens", 0):,} in / {cost.get("total_output_tokens", 0):,} out")
        if cost.get("by_agent"):
            lines.append("- Top agents:")
            for agent, c in list(cost["by_agent"].items())[:5]:
                lines.append(f"  - {agent}: ${c:.4f}")
        if cost.get("total_tokens"):
            lines.append(f"- Token usage: {cost["total_tokens"]:,} total ({cost.get("token_requests", 0)} reqs)")

    lines.extend([
        "",
        "## 🎯 Reggeli javaslatok",
        "",
    ])
    # Generate 4 prioritized suggestions
    suggestions = []
    if mp.get("suggestions"):
        suggestions.append(f"1. 🧠 Skill: {mp['suggestions'][0]['title']} — gyakran használt, érdemes skill-be önteni")
    if b.get("memory_health", {}).get("unvectorized", 0) > 0:
        suggestions.append(f"2. 🧹 {b['memory_health']['unvectorized']} vektorizálatlan memória — backfill szükséges")
    if kb.get("stuck"):
        suggestions.append(f"3. 📋 {len(kb['stuck'])} beragadt kanban task — ping el needed")
    if err.get("recurring"):
        suggestions.append(f"4. 🔴 {err['total_errors']} visszatérő hiba — root cause elemzés")
    if not suggestions:
        suggestions.append("✅ Minden rendben — nincs azonnali teendő")
    for s in suggestions[:4]:
        lines.append(f"- {s}")

    lines.append("")
    lines.append("---")
    lines.append("*Dream Engine — A2A Mesh v0.36+*")

    return "\n".join(lines)


async def get_dream_status(pg_pool=None):
    """Get Dream Engine status for dashboard."""
    import time as _time
    status = {
        "enabled": True,
        "interval_hours": 6,
        "last_run": None,
        "next_run": None,
        "recent_results": [],
    }
    if not pg_pool:
        status["enabled"] = False
        status["error"] = "PG unavailable"
        return status
    try:
        rows = await pg_pool.fetch(
            """SELECT created_at, content, subject FROM shared_a2a_memory
               WHERE subject = 'dream_engine' AND created_at > NOW() - INTERVAL '24 hours'
               ORDER BY created_at DESC LIMIT 5"""
        )
        if rows:
            status["last_run"] = rows[0]["created_at"].isoformat() if rows else None
            status["recent_results"] = [
                {"timestamp": r["created_at"].isoformat(), "preview": str(r["content"])[:200]}
                for r in rows
            ]
        mem_count = await pg_pool.fetchval("SELECT COUNT(*) FROM shared_a2a_memory WHERE subject = 'dream_engine'")
        status["total_dreams"] = mem_count or 0
    except Exception as e:
        status["error"] = str(e)
    return status