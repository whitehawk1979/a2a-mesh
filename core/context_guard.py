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

# Cooldown cache to prevent repeated handoff generation
# { agent_name: last_timestamp }
HANDOFF_COOLDOWN_CACHE = {}
HANDOFF_COOLDOWN_SEC = 30 * 60


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

                    # --- FEATURE A: Automatic Handoff ---
                    if check["pct"] >= ACT_PCT:
                        now = time.time()
                        last_handoff = HANDOFF_COOLDOWN_CACHE.get(agent, 0)
                        if now - last_handoff < HANDOFF_COOLDOWN_SEC:
                            continue
                            
                        # Simulation of GateInputs for handoff preparation
                        # We allow prep even if busy, as the handoff is for the NEXT session
                        inputs = GateInputs(
                            turns_used=active * 10,
                            is_busy=False, 
                            has_pending_outbound=False,
                            has_open_question=False,
                            has_live_task_state=False,
                            has_stale_outbound=False,
                            hard_guard_active=False
                        )
                        
                        decision = decide_gate(inputs)
                        if decision.action == GateAction.ALLOW:
                            # Deterministic summary from active tasks
                            task_rows = await conn.fetch(
                                "SELECT subject, progress FROM shared_delegations \n                                  WHERE assigned_agent = $1 AND status IN ('pending', 'running')",
                                agent
                            )
                            summary_lines = [f"- {tr['subject']} ({tr['progress']}%)" for tr in task_rows]
                            context_summary = "\n".join(summary_lines) if summary_lines else "No active tasks summary available."
                            current_task = task_rows[0]['subject'] if task_rows else "None"
                            
                            prompt = generate_handoff_prompt(agent, current_task, context_summary)
                            epoch = int(now)
                            key = f"handoff_{agent}_{epoch}"
                            
                            # Store as shared context entry
                            await conn.execute(
                                "INSERT INTO shared_context (agent, context_key, context_value, value_type, expires_at) \n                                 VALUES ($1, $2, $3, 'text', NOW() + INTERVAL '1 hour')",
                                agent, key, prompt
                            )
                            
                            HANDOFF_COOLDOWN_CACHE[agent] = now
                            log.info(f"[Context Guard] Automatic handoff generated for {agent} -> {key} (pct={check['pct']:.2%})")
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


# ── Marveen-inspired: decideGate state machine ──────────────────
# Adapted from Marveen's context-restart-gate.ts (pure logic, no tmux I/O).
# Makes a deterministic "should we compact/restart this agent?" decision
# based on multiple fail-closed gate conditions.

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

class GateAction(Enum):
    ALLOW = "allow"       # Safe to compact/restart
    BLOCK = "block"       # Work in flight, do not touch
    ALERT = "block-alert" # Blocked too long, escalate

@dataclass
class GateConfig:
    enabled: bool = True
    threshold_turns: int = 70           # Trigger at 70 turns (our context proxy)
    stale_cutoff_ms: int = 2 * 60 * 60 * 1000  # 2h — stale tasks don't block
    retry_interval_ms: int = 5 * 60 * 1000    # 5 min between re-checks
    persistent_block_alert_ms: int = 2 * 60 * 60 * 1000  # 2h → alert

@dataclass
class GateInputs:
    turns_used: Optional[int]      # Current turn count (null = unmeasurable)
    is_busy: bool                  # Agent has running tasks
    has_pending_outbound: bool     # Pending P2P messages to deliver
    has_open_question: bool        # Unresolved inbound message
    has_live_task_state: bool      # Structured task in flight
    has_stale_outbound: bool       # Pending messages older than stale_cutoff
    hard_guard_active: bool        # Hard guard already managing this agent

@dataclass
class GateDecision:
    action: GateAction
    reason: str
    note_stale: bool = False

DEFAULT_GATE_CONFIG = GateConfig()

def decide_gate(inputs: GateInputs, cfg: GateConfig = None,
                first_blocked_at: Optional[float] = None,
                now_ms: Optional[float] = None) -> GateDecision:
    """Marveen-inspired gate decision — pure logic, fail-closed.

    Returns ALLOW if safe to compact/restart, BLOCK if work is in flight,
    ALERT if blocked for too long (escalate to owner).

    FAIL-CLOSED: any unmeasurable signal blocks, never allows.
    """
    if cfg is None:
        cfg = DEFAULT_GATE_CONFIG
    if now_ms is None:
        now_ms = time.time() * 1000

    if not cfg.enabled:
        return GateDecision(GateAction.BLOCK, "gate-disabled")

    # Trigger: below threshold → no action needed
    if inputs.turns_used is None:
        return GateDecision(GateAction.BLOCK, "turns-unmeasurable (fail-closed)")
    if inputs.turns_used < cfg.threshold_turns:
        return GateDecision(GateAction.BLOCK, f"below-threshold ({inputs.turns_used} < {cfg.threshold_turns})")

    # Interlock: hard guard is managing → stand aside
    if inputs.hard_guard_active:
        return _block_or_alert(first_blocked_at, now_ms, cfg, "hard-guard-armed")

    # Gate conditions (FAIL-CLOSED)
    if inputs.is_busy:
        return _block_or_alert(first_blocked_at, now_ms, cfg, "agent-busy (running tasks)")

    if inputs.has_pending_outbound and not inputs.has_stale_outbound:
        return _block_or_alert(first_blocked_at, now_ms, cfg, "pending-outbound (live dispatched work)")

    if inputs.has_open_question:
        return _block_or_alert(first_blocked_at, now_ms, cfg, "open-question (unresolved inbound)")

    if inputs.has_live_task_state:
        return _block_or_alert(first_blocked_at, now_ms, cfg, "live-task-state (structured task in flight)")

    # All conditions clear → ALLOW
    decision = GateDecision(GateAction.ALLOW, f"safe-to-compact ({inputs.turns_used} turns)")
    if inputs.has_stale_outbound:
        decision.note_stale = True
    return decision


def _block_or_alert(first_blocked_at: Optional[float], now_ms: float,
                    cfg: GateConfig, reason: str) -> GateDecision:
    """Block, or escalate to ALERT if blocked too long."""
    if first_blocked_at is not None:
        blocked_duration = now_ms - first_blocked_at
        if blocked_duration >= cfg.persistent_block_alert_ms:
            return GateDecision(GateAction.ALERT, f"persistent-block ({reason}, {blocked_duration/1000/60:.0f}min)")
    return GateDecision(GateAction.BLOCK, reason)