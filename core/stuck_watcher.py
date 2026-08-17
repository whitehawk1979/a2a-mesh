"""
Stuck Tool-Call Watcher — detect wedged tool calls.

Inspired by Marveen's stuck-tool-call-watcher:
  - Agent gets stuck at "Worked for Ns" indefinitely
  - Tool call hangs server-side, TUI blocks
  - Detection: same progress line across multiple polls
  - Recovery: respawn

For A2A Mesh:
  - Monitors delegation durations
  - If a delegation exceeds expected time → stuck
  - Logs + alerts
  - Suggests cancellation
"""

import time
import json
import os
import logging

log = logging.getLogger("stuck_watcher")

STUCK_THRESHOLD = 600  # 10 min: likely stuck
FREEZE_THRESHOLD = 1800  # 30 min: definitely frozen


def load_delegations():
    """Load current delegations from PG or local state."""
    # This will be called with pg_pool in production
    return []


def check_stuck(active_delegations):
    """Check delegations for stuck tool calls.
    active_delegations: list of dicts with id, started_at, last_update, status."""
    now = time.time()
    issues = []
    
    for d in active_delegations:
        started = d.get("started_at", now)
        last_update = d.get("last_update", started)
        elapsed = now - started
        since_update = now - last_update
        
        if elapsed >= FREEZE_THRESHOLD:
            issues.append({
                "delegation_id": d.get("id", ""),
                "status": "frozen",
                "elapsed_s": int(elapsed),
                "idle_s": int(since_update),
                "severity": "critical",
                "action": "cancel",
                "message": f"🔴 Delegation {d.get('id','')} FROZEN ({elapsed:.0f}s elapsed, {since_update:.0f}s idle)"
            })
        elif elapsed >= STUCK_THRESHOLD and since_update > 120:
            issues.append({
                "delegation_id": d.get("id", ""),
                "status": "stuck",
                "elapsed_s": int(elapsed),
                "idle_s": int(since_update),
                "severity": "warning",
                "action": "suggest_cancel",
                "message": f"🟡 Delegation {d.get('id','')} STUCK ({elapsed:.0f}s elapsed, {since_update:.0f}s idle)"
            })
    
    for issue in issues:
        log.warning(f"Stuck watcher: {issue['message']}")
    
    return issues