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
  - Tracks node context sizes per node
  - Estimates token usage from active delegations + model profile
  - Suggests compaction at threshold (default 70% of max_turns)
  - Force clear at 85%
  - Dashboard: real-time saturation meters per node
  - Alert: Telegram notification on critical saturation
"""

import time
import logging
import json
import os
import sqlite3
import yaml
from typing import Optional, Dict, Any, List

log = logging.getLogger("context_gate")

# Thresholds (percentage of max_turns)
SOFT_THRESHOLD_PCT = 70   # Suggest clear at 70%
HARD_THRESHOLD_PCT = 85   # Force clear at 85%
CRITICAL_PCT = 95         # Critical alert

# Context priority levels for preservation during compaction
PRIORITY_CRITICAL = ["system", "identity", "active_task", "user_preference"]
PRIORITY_HIGH = ["recent_memory", "skill_reference", "delegation_context"]
PRIORITY_LOW = ["old_conversation", "tool_output", "search_result"]


def resolve_local_model_info():
    """
    Detect the running model for the local node.
    Source: ~/.hermes/state.db (sessions table) + ~/.hermes/config.yaml
    """
    info = {"model": "unknown", "context_length": None, "max_turns": None}
    home = os.path.expanduser("~")
    # HAOS/container nodes keep Hermes config under /config/.hermes instead of ~/.hermes
    hermes_home = os.environ.get("HERMES_HOME") or os.path.join(home, ".hermes")
    # HAOS/container nodes keep the actual state.db under /config/.hermes
    if (not os.path.exists(os.path.join(hermes_home, "state.db"))
            and os.path.exists("/config/.hermes/state.db")):
        hermes_home = "/config/.hermes"
    db_path = os.path.join(hermes_home, "state.db")
    cfg_path = os.path.join(hermes_home, "config.yaml")

    # 1. Try to get the last used model from state.db
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT model FROM sessions WHERE model IS NOT NULL AND model != '' ORDER BY started_at DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                info["model"] = row[0]
            conn.close()
        except Exception as e:
            log.debug(f"Error reading local state.db: {e}")

    # 2. Try to get context_length and max_turns from config.yaml
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r") as f:
                cfg = yaml.safe_load(f)
                if cfg:
                    # context_length usually under 'model'
                    model_cfg = cfg.get("model", {})
                    if isinstance(model_cfg, dict):
                        info["context_length"] = model_cfg.get("context_length")
                    
                    # max_turns usually under 'agent'
                    agent_cfg = cfg.get("agent", {})
                    if isinstance(agent_cfg, dict):
                        info["max_turns"] = agent_cfg.get("max_turns")
        except Exception as e:
            log.debug(f"Error reading local config.yaml: {e}")

    return info


async def resolve_node_profile(node_name: str, node=None, pg_pool=None) -> Dict[str, Any]:
    """
    Resolve a node's model profile (model, context_length, max_turns, tokens_per_turn).
    Fallback order:
    1. Local detection (if node_name is the local node)
    2. Explicit node_profiles in MeshConfig
    3. PG mesh.mesh_nodes.provider_status (jsonb)
    4. Global defaults
    """
    # Defaults from node config if available
    defaults = {
        "model": "unknown",
        "context_length": 128000,
        "max_turns": 90,
        "tokens_per_turn": 3500
    }
    if node and node.config and hasattr(node.config, "context_gate"):
        defaults["max_turns"] = node.config.context_gate.max_turns_default
        defaults["tokens_per_turn"] = node.config.context_gate.tokens_per_turn_default
        if node.config.context_gate.node_profiles and node_name in node.config.context_gate.node_profiles:
            return {**defaults, **node.config.context_gate.node_profiles[node_name]}

    # Local node detection
    if node and getattr(node, "node_name", None) == node_name:
        local_info = resolve_local_model_info()
        # Merge local_info into defaults
        resolved = {**defaults}
        for k, v in local_info.items():
            if v is not None:
                resolved[k] = v
        return resolved

    # PG provider_status
    if pg_pool:
        try:
            row = await pg_pool.fetchval("SELECT provider_status FROM mesh.mesh_nodes WHERE node_name = $1", node_name)
            if row:
                status = json.loads(row) if isinstance(row, str) else row
                if isinstance(status, dict):
                    # We expect provider_status to possibly contain a 'model' block
                    model_data = status.get("model", {})
                    if isinstance(model_data, dict):
                        return {**defaults, **model_data}
        except Exception as e:
            log.debug(f"Error resolving PG profile for {node_name}: {e}")

    return defaults


def check_gate(node_name, current_turns, max_turns=90, context_length=128000, tokens_per_turn=3500):
    """
    Check if context restart is needed based on turns AND tokens.
    Returns dict with recommendation and token estimate.
    """
    threshold = int(max_turns * (SOFT_THRESHOLD_PCT / 100))
    hard = int(max_turns * (HARD_THRESHOLD_PCT / 100))
    critical = int(max_turns * (CRITICAL_PCT / 100))
    
    pct_turns = (current_turns / max_turns) * 100 if max_turns > 0 else 0
    est_tokens = current_turns * tokens_per_turn
    pct_ctx = (est_tokens / context_length) * 100 if context_length > 0 else 0
    
    # Use the worst of the two saturations
    effective_pct = max(pct_turns, pct_ctx)

    if effective_pct >= CRITICAL_PCT:
        return {
            "node": node_name,
            "action": "critical_alert",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(effective_pct, 1),
            "est_tokens": est_tokens,
            "severity": "critical",
            "message": f"🔴 {node_name}: {current_turns}/{max_turns} turns ({round(effective_pct, 1)}%) — CRITICAL context saturation",
            "pct_turns": round(pct_turns, 1),
            "pct_ctx": round(pct_ctx, 1),
        }
    elif effective_pct >= HARD_THRESHOLD_PCT:
        return {
            "node": node_name,
            "action": "force_clear",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(effective_pct, 1),
            "est_tokens": est_tokens,
            "severity": "critical",
            "message": f"🔴 {node_name}: {current_turns}/{max_turns} turns — FORCE context clear",
            "pct_turns": round(pct_turns, 1),
            "pct_ctx": round(pct_ctx, 1),
        }
    elif effective_pct >= SOFT_THRESHOLD_PCT:
        return {
            "node": node_name,
            "action": "suggest_clear",
            "turns": current_turns,
            "max_turns": max_turns,
            "pct": round(effective_pct, 1),
            "est_tokens": est_tokens,
            "severity": "warning",
            "message": f"🟡 {node_name}: {current_turns}/{max_turns} turns — suggest context clear",
            "pct_turns": round(pct_turns, 1),
            "pct_ctx": round(pct_ctx, 1),
        }
    return {
        "node": node_name,
        "action": "none",
        "turns": current_turns,
        "max_turns": max_turns,
        "pct": round(effective_pct, 1),
        "est_tokens": est_tokens,
        "severity": "ok",
        "message": None,
        "pct_turns": round(pct_turns, 1),
        "pct_ctx": round(pct_ctx, 1),
    }


def get_priority_context(agent_name, pg_pool_data=None):
    """Determine which context items to preserve during compaction."""
    preserve = []
    preserve.append({"type": "system", "priority": "critical", "content": "agent identity + core instructions"})
    if pg_pool_data and pg_pool_data.get("active_delegations"):
        for d in pg_pool_data["active_delegations"][:3]:
            preserve.append({
                "type": "active_task",
                "priority": "critical",
                "content": d.get("task_type", "unknown") + ": " + str(d.get("description", ""))[:100]
            })
    if pg_pool_data and pg_pool_data.get("recent_memories"):
        for m in pg_pool_data["recent_memories"][:5]:
            preserve.append({
                "type": "recent_memory",
                "priority": "high",
                "content": m
            })
    return preserve


async def context_gate_tick(pg_pool, node=None):
    """Periodic check for all nodes' context gates."""
    results = []
    if not pg_pool:
        return results
    try:
        # List all active nodes in the mesh
        nodes = await pg_pool.fetch("SELECT node_name FROM mesh.mesh_nodes WHERE status = 'active'")
        for n in nodes:
            node_name = n["node_name"]
            profile = await resolve_node_profile(node_name, node, pg_pool)
            
            # Estimate turns from active delegations
            row = await pg_pool.fetchrow("""SELECT COUNT(*) as active, COALESCE(SUM(progress), 0) as total_progress 
                                  FROM shared_delegations WHERE assigned_agent = $1 AND status IN ('pending', 'running')""", node_name)
            
            active = row["active"] if row else 0
            progress = row["total_progress"] if row else 0
            est_turns = active * 15 + progress // 10
            
            check = check_gate(
                node_name, 
                est_turns, 
                max_turns=profile["max_turns"], 
                context_length=profile["context_length"], 
                tokens_per_turn=profile["tokens_per_turn"]
            )
            if check["action"] != "none":
                log.warning("Context gate: " + check["message"])
                results.append(check)
    except Exception as e:
        log.debug("Context gate tick error: " + str(e))
    return results


async def get_context_status(pg_pool=None, node=None):
    """Get detailed context gate status for dashboard. Now lists ALL mesh nodes."""
    agents_status = []
    if not pg_pool:
        return {"agents": [], "error": "PG unavailable"}

    try:
        # 1. Get ALL active nodes from mesh schema
        nodes_rows = await pg_pool.fetch("SELECT node_name FROM mesh.mesh_nodes WHERE status = 'active'")
        all_nodes = [r["node_name"] for r in nodes_rows]

        # 2. Get delegation stats for everyone in one go
        delegation_rows = await pg_pool.fetch("""SELECT assigned_agent,
                                              COUNT(*) as active,
                                              COALESCE(SUM(progress), 0) as total_progress,
                                              COUNT(*) FILTER (WHERE status = 'running') as running,
                                              COUNT(*) FILTER (WHERE status = 'pending') as pending
                                       FROM shared_delegations
                                       WHERE status IN ('pending', 'running')
                                       GROUP BY assigned_agent""")
        
        stats_map = {r["assigned_agent"]: r for r in delegation_rows if r["assigned_agent"]}

        for node_name in all_nodes:
            profile = await resolve_node_profile(node_name, node, pg_pool)
            stats = stats_map.get(node_name, {"active": 0, "total_progress": 0, "running": 0, "pending": 0})
            
            est_turns = stats["active"] * 15 + stats["total_progress"] // 10
            check = check_gate(
                node_name, 
                est_turns, 
                max_turns=profile["max_turns"], 
                context_length=profile["context_length"], 
                tokens_per_turn=profile["tokens_per_turn"]
            )

            agents_status.append({
                "agent": node_name,
                "turns": check["turns"],
                "max_turns": check["max_turns"],
                "pct": check["pct"],
                "est_tokens": check["est_tokens"],
                "action": check["action"],
                "severity": check["severity"],
                "message": check["message"],
                "active_delegations": stats["active"],
                "running": stats["running"],
                "pending": stats["pending"],
                "progress": stats["total_progress"],
                "model": profile["model"],
                "context_length": profile["context_length"],
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
