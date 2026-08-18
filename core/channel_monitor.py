"""
Channel Monitor Watchdog — adapted from Marveen's channel-monitor.ts + agent-restart-policy.ts.

Marveen eredeti: tmux pane state detection + channel plugin liveness + auto-restart policy.
A2A Mesh adaptáció: P2P heartbeat + delegation health + auto-recovery actions.

Key Marveen concepts ported:
1. Multi-level stalled detection (30s/60s/5min/10min/30min) — replaces our simple 10min/30min
2. Exponential backoff for recovery attempts — doubles grace per failure
3. Down confirmation window — one bad sample is not enough
4. Busy deferral — don't kill running work; escalate if too long
5. Max restart attempts → escalate to operator (stop churning)
6. Startup grace — young processes get time to come up

Architecture mapping:
  Marveen tmux pane capture → A2A P2P heartbeat + PG delegation status
  Marveen channel plugin → A2A mesh node process
  Marveen agent restart → A2A node restart (launchctl/systemctl)
  Marveen alert to owner → A2A Telegram alert to Zsolt
"""

import time
import logging
import asyncio
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from enum import Enum

log = logging.getLogger("channel_monitor")

# ── Stalled detection thresholds (multi-level, Marveen-inspired) ──
STALL_WARNING_S = 30        # 30s: likely starting
STALL_BUSY_S = 60           # 60s: actively working
STALL_STUCK_S = 300         # 5min: probably stuck
STALL_FROZEN_S = 600        # 10min: definitely stuck
STALL_DEAD_S = 1800         # 30min: unresponsive

# ── Recovery policy defaults ──
STARTUP_GRACE_S = 30        # Young process gets 30s before any action
RESTART_GRACE_S = 60        # After restart, 60s before next attempt
MAX_RESTART_ATTEMPTS = 5    # After 5 failed restarts → escalate
DOWN_CONFIRM_S = 10         # 10s down before acting (not 1 sample)
BUSY_DEFER_MAX_S = 600      # 10min busy before escalating


class WatchdogAction(Enum):
    """What the watchdog decided to do."""
    NONE = "none"              # All good
    WATCH = "watch"            # Noticed stall, monitoring
    RECONNECT = "reconnect"   # Try P2P reconnect
    RESTART = "restart"        # Restart the node process
    ALERT = "alert"            # Escalate to operator (max restarts hit)
    ALERT_BUSY = "alert-busy"  # Escalate (busy too long + down)
    SKIP = "skip"              # Within grace/backoff window


class AgentState(Enum):
    """Agent state machine — adapted from Marveen pane-state.ts."""
    IDLE = "idle"              # No active tasks, heartbeat OK
    BUSY = "busy"              # Active delegation(s), heartbeat OK
    TYPING = "typing"          # Recently produced output (progress update)
    STUCK = "stuck"            # No progress for >5min, task still running
    FROZEN = "frozen"          # No progress for >10min
    DEAD = "dead"              # No heartbeat for >30min
    UNKNOWN = "unknown"        # Can't determine state


@dataclass
class WatchdogInput:
    """Inputs for the watchdog decision — gathered from P2P + PG."""
    node_name: str
    # Heartbeat
    last_heartbeat_s: Optional[float]    # Seconds since last heartbeat (None = never)
    # Delegation activity
    active_delegations: int             # Count of running delegations
    last_progress_s: Optional[float]     # Seconds since last progress update
    # Process state
    process_age_s: float                 # How long the node process has been running
    is_busy: bool                        # Has running delegations
    # Recovery state
    consecutive_failures: int = 0        # Failed recovery attempts
    ms_since_last_restart: Optional[float] = None  # Seconds since last restart
    down_since_s: Optional[float] = None  # When did we first notice it down


@dataclass
class WatchdogDecision:
    """Output of the watchdog decision."""
    action: WatchdogAction
    agent_state: AgentState
    reason: str
    stall_level: str = "ok"   # ok/warning/busy/stuck/frozen/dead
    elapsed_s: float = 0.0


def detect_agent_state(inp: WatchdogInput) -> AgentState:
    """Detect agent state from P2P heartbeat + delegation activity.
    
    Adapted from Marveen's pane-state.ts regex detection → A2A P2P heartbeat + PG queries.
    """
    now = time.time()
    
    # No heartbeat at all
    if inp.last_heartbeat_s is None or inp.last_heartbeat_s > STALL_DEAD_S:
        return AgentState.DEAD
    
    # No progress on active tasks
    if inp.active_delegations > 0:
        if inp.last_progress_s is None:
            return AgentState.BUSY  # Just started, no progress yet
        if inp.last_progress_s > STALL_FROZEN_S:
            return AgentState.FROZEN
        if inp.last_progress_s > STALL_STUCK_S:
            return AgentState.STUCK
        if inp.last_progress_s < STALL_WARNING_S:
            return AgentState.TYPING  # Recent progress
        return AgentState.BUSY
    
    # No active tasks, heartbeat OK
    return AgentState.IDLE


def effective_restart_grace_s(
    restart_grace_s: float,
    consecutive_failures: int,
    max_grace_s: float = 3600  # 1h cap
) -> float:
    """Exponential backoff for restart grace.
    
    Each consecutive failure doubles the grace period, capped at max_grace_s.
    Adapted from Marveen's effectiveRestartGraceMs().
    """
    failures = max(0, int(consecutive_failures))
    exp = min(failures, 20)  # Cap exponent to prevent overflow
    grace = restart_grace_s * (2 ** exp)
    return min(grace, max_grace_s)


def should_auto_restart(inp: WatchdogInput,
                        startup_grace_s: float = STARTUP_GRACE_S,
                        restart_grace_s: float = RESTART_GRACE_S) -> bool:
    """Determine if a down agent should be restarted.
    
    Adapted from Marveen's shouldAutoRestartDownAgent().
    Fail-closed: unknown age → don't restart.
    """
    # Unknown process age → conservative, don't restart
    if inp.process_age_s < 0:
        return False
    # Freshly started → give it time
    if inp.process_age_s < startup_grace_s:
        return False
    # Recently restarted → backoff
    grace = effective_restart_grace_s(restart_grace_s, inp.consecutive_failures)
    if inp.ms_since_last_restart is not None and inp.ms_since_last_restart < grace:
        return False
    return True


def decide_watchdog_action(
    inp: WatchdogInput,
    max_restart_attempts: int = MAX_RESTART_ATTEMPTS,
    down_confirm_s: float = DOWN_CONFIRM_S,
    busy_defer_max_s: float = BUSY_DEFER_MAX_S,
) -> WatchdogDecision:
    """Main watchdog decision — adapted from Marveen's decideDownAgentAction().
    
    Returns the action to take + the detected agent state + reason.
    
    Decision tree:
    1. Detect agent state (idle/busy/typing/stuck/frozen/dead)
    2. If dead: check confirmation window → restart/alert
    3. If stuck/frozen: try reconnect first, then restart
    4. If busy with heartbeat: just monitor
    5. If idle: all good
    """
    state = detect_agent_state(inp)
    
    # All good states
    if state in (AgentState.IDLE, AgentState.BUSY, AgentState.TYPING):
        stall = "ok"
        if state == AgentState.BUSY:
            stall = "busy"
        elif state == AgentState.TYPING:
            stall = "typing"
        return WatchdogDecision(
            action=WatchdogAction.NONE,
            agent_state=state,
            reason=f"healthy ({state.value})",
            stall_level=stall,
            elapsed_s=inp.last_progress_s or 0,
        )
    
    # STUCK: no progress >5min, try reconnect
    if state == AgentState.STUCK:
        return WatchdogDecision(
            action=WatchdogAction.RECONNECT,
            agent_state=state,
            reason=f"stuck ({inp.last_progress_s:.0f}s no progress on {inp.active_delegations} tasks)",
            stall_level="stuck",
            elapsed_s=inp.last_progress_s or 0,
        )
    
    # FROZEN: no progress >10min, restart candidate
    if state == AgentState.FROZEN:
        # Check max restart attempts
        if inp.consecutive_failures >= max_restart_attempts:
            if inp.consecutive_failures == max_restart_attempts:
                return WatchdogDecision(
                    action=WatchdogAction.ALERT,
                    agent_state=state,
                    reason=f"frozen + {inp.consecutive_failures} failed restarts → escalate",
                    stall_level="frozen",
                    elapsed_s=inp.last_progress_s or 0,
                )
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason=f"already alerted ({inp.consecutive_failures} failures)",
                stall_level="frozen",
                elapsed_s=inp.last_progress_s or 0,
            )
        
        # Confirmation window
        down_time = inp.down_since_s or 0
        if down_time < down_confirm_s:
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason=f"confirming down ({down_time:.0f}s < {down_confirm_s}s)",
                stall_level="frozen",
                elapsed_s=inp.last_progress_s or 0,
            )
        
        # Should we restart?
        if not should_auto_restart(inp):
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason="within grace/backoff window",
                stall_level="frozen",
                elapsed_s=inp.last_progress_s or 0,
            )
        
        # Busy guard: don't kill running work
        if inp.is_busy:
            if down_time >= busy_defer_max_s:
                return WatchdogDecision(
                    action=WatchdogAction.ALERT_BUSY,
                    agent_state=state,
                    reason=f"busy + frozen >{busy_defer_max_s}s → escalate",
                    stall_level="frozen",
                    elapsed_s=inp.last_progress_s or 0,
                )
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason=f"busy, deferring (down {down_time:.0f}s)",
                stall_level="frozen",
                elapsed_s=inp.last_progress_s or 0,
            )
        
        return WatchdogDecision(
            action=WatchdogAction.RESTART,
            agent_state=state,
            reason=f"frozen ({inp.last_progress_s:.0f}s), restarting (attempt {inp.consecutive_failures + 1})",
            stall_level="frozen",
            elapsed_s=inp.last_progress_s or 0,
        )
    
    # DEAD: no heartbeat >30min
    if state == AgentState.DEAD:
        heartbeat_age = inp.last_heartbeat_s or 999999
        
        # Max restarts → alert
        if inp.consecutive_failures >= max_restart_attempts:
            if inp.consecutive_failures == max_restart_attempts:
                return WatchdogDecision(
                    action=WatchdogAction.ALERT,
                    agent_state=state,
                    reason=f"dead ({heartbeat_age:.0f}s no heartbeat) + {inp.consecutive_failures} failed restarts",
                    stall_level="dead",
                    elapsed_s=heartbeat_age,
                )
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason=f"already alerted",
                stall_level="dead",
                elapsed_s=heartbeat_age,
            )
        
        # Confirmation window
        down_time = inp.down_since_s or 0
        if down_time < down_confirm_s:
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason=f"confirming dead ({down_time:.0f}s < {down_confirm_s}s)",
                stall_level="dead",
                elapsed_s=heartbeat_age,
            )
        
        if not should_auto_restart(inp):
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason="within grace/backoff window",
                stall_level="dead",
                elapsed_s=heartbeat_age,
            )
        
        # Busy guard
        if inp.is_busy:
            if down_time >= busy_defer_max_s:
                return WatchdogDecision(
                    action=WatchdogAction.ALERT_BUSY,
                    agent_state=state,
                    reason=f"busy + dead >{busy_defer_max_s}s → escalate",
                    stall_level="dead",
                    elapsed_s=heartbeat_age,
                )
            return WatchdogDecision(
                action=WatchdogAction.SKIP,
                agent_state=state,
                reason=f"busy, deferring (down {down_time:.0f}s)",
                stall_level="dead",
                elapsed_s=heartbeat_age,
            )
        
        return WatchdogDecision(
            action=WatchdogAction.RESTART,
            agent_state=state,
            reason=f"dead ({heartbeat_age:.0f}s no heartbeat), restarting (attempt {inp.consecutive_failures + 1})",
            stall_level="dead",
            elapsed_s=heartbeat_age,
        )
    
    # UNKNOWN
    return WatchdogDecision(
        action=WatchdogAction.SKIP,
        agent_state=state,
        reason="unknown state (fail-closed)",
        stall_level="unknown",
    )


# ── Async watchdog loop ──────────────────────────────────────────

# In-memory state tracking (per node)
_down_since: Dict[str, float] = {}       # node → first-down timestamp
_consecutive_failures: Dict[str, int] = {}  # node → failure count
_last_restart: Dict[str, float] = {}      # node → last restart time
_last_action: Dict[str, WatchdogAction] = {}  # node → last action taken


async def watchdog_tick(pg_pool, node_name: str = "unknown",
                        get_heartbeat_age=None, get_active_delegations=None,
                        get_last_progress=None, get_process_age=None,
                        restart_callback=None, alert_callback=None) -> WatchdogDecision:
    """One watchdog tick for one node.
    
    Gathers inputs from P2P + PG, runs decide_watchdog_action, and executes the action.
    
    Callbacks are injected for testability:
    - get_heartbeat_age(node) → seconds since last heartbeat
    - get_active_delegations(node) → count of running delegations
    - get_last_progress(node) → seconds since last progress update
    - get_process_age(node) → seconds the node process has been running
    - restart_callback(node) → restart the node process
    - alert_callback(node, message) → send alert to operator
    """
    # Gather inputs
    hb_age = await get_heartbeat_age(node_name) if get_heartbeat_age else None
    active = await get_active_delegations(node_name) if get_active_delegations else 0
    progress = await get_last_progress(node_name) if get_last_progress else None
    proc_age = await get_process_age(node_name) if get_process_age else 0
    
    # Track down state
    is_down = hb_age is None or hb_age > STALL_DEAD_S
    if is_down and node_name not in _down_since:
        _down_since[node_name] = time.time()
    elif not is_down and node_name in _down_since:
        # Recovered!
        del _down_since[node_name]
        _consecutive_failures.pop(node_name, None)
        log.info(f"Watchdog: {node_name} recovered, clearing failure state")
    
    down_since = _down_since.get(node_name)
    down_time = (time.time() - down_since) if down_since else 0
    
    inp = WatchdogInput(
        node_name=node_name,
        last_heartbeat_s=hb_age,
        active_delegations=active,
        last_progress_s=progress,
        process_age_s=proc_age,
        is_busy=active > 0,
        consecutive_failures=_consecutive_failures.get(node_name, 0),
        ms_since_last_restart=(time.time() - _last_restart[node_name]) if node_name in _last_restart else None,
        down_since_s=down_time,
    )
    
    decision = decide_watchdog_action(inp)
    _last_action[node_name] = decision.action
    
    # Execute action
    if decision.action == WatchdogAction.RESTART:
        log.warning(f"Watchdog: RESTART {node_name} — {decision.reason}")
        _consecutive_failures[node_name] = _consecutive_failures.get(node_name, 0) + 1
        _last_restart[node_name] = time.time()
        if restart_callback:
            await restart_callback(node_name)
    elif decision.action == WatchdogAction.RECONNECT:
        log.info(f"Watchdog: RECONNECT {node_name} — {decision.reason}")
        # P2P reconnect is handled by the transport layer automatically
    elif decision.action in (WatchdogAction.ALERT, WatchdogAction.ALERT_BUSY):
        log.error(f"Watchdog: ALERT {node_name} — {decision.reason}")
        if alert_callback:
            await alert_callback(node_name, decision.reason)
    elif decision.action == WatchdogAction.WATCH:
        log.info(f"Watchdog: WATCH {node_name} — {decision.reason}")
    
    return decision


def get_watchdog_status() -> List[Dict]:
    """Get current watchdog state for all monitored nodes."""
    results = []
    for node in set(list(_down_since.keys()) + list(_consecutive_failures.keys()) + list(_last_action.keys())):
        results.append({
            "node": node,
            "down_since": _down_since.get(node),
            "consecutive_failures": _consecutive_failures.get(node, 0),
            "last_restart": _last_restart.get(node),
            "last_action": _last_action.get(node, WatchdogAction.NONE).value if node in _last_action else "none",
        })
    return results