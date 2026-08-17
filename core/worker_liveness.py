"""
Worker Liveness — detect agent worker deaths.

Inspired by Marveen's worker-liveness:
  - Pre-started worker sessions die silently
  - Nothing notices → operator only finds out via tmux ls
  - This module: poll worker health, log death time + last pane content

For A2A Mesh:
  - Monitors active delegation workers
  - Detects stalled/dead workers (no heartbeat for N seconds)
  - Logs death event for forensics
  - Alerts via dashboard
"""

import time
import json
import os
import logging

log = logging.getLogger("worker_liveness")

STATE_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/worker_liveness.json")
STALE_THRESHOLD = 300  # 5 min without heartbeat = stale
DEAD_THRESHOLD = 900   # 15 min = dead


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def record_heartbeat(worker_id, task_id=""):
    """Record a heartbeat from a worker."""
    state = load_state()
    state[worker_id] = {
        "last_heartbeat": time.time(),
        "task_id": task_id,
        "status": "alive",
    }
    save_state(state)


def check_liveness():
    """Check all workers for liveness. Returns list of issues."""
    state = load_state()
    now = time.time()
    issues = []
    
    for worker_id, data in state.items():
        last_hb = data.get("last_heartbeat", 0)
        elapsed = now - last_hb
        
        if elapsed >= DEAD_THRESHOLD:
            if data.get("status") != "dead":
                data["status"] = "dead"
                data["died_at"] = now
                data["age_before_death"] = elapsed
                log.warning(f"Worker liveness: {worker_id} DEAD (no heartbeat for {elapsed:.0f}s)")
                issues.append({
                    "worker": worker_id,
                    "status": "dead",
                    "elapsed_s": int(elapsed),
                    "task": data.get("task_id", ""),
                    "severity": "critical",
                })
        elif elapsed >= STALE_THRESHOLD:
            if data.get("status") != "stale":
                data["status"] = "stale"
                log.warning(f"Worker liveness: {worker_id} STALE (no heartbeat for {elapsed:.0f}s)")
                issues.append({
                    "worker": worker_id,
                    "status": "stale",
                    "elapsed_s": int(elapsed),
                    "task": data.get("task_id", ""),
                    "severity": "warning",
                })
    
    save_state(state)
    return issues


def get_all_workers():
    """Get all worker statuses."""
    state = load_state()
    now = time.time()
    result = []
    for worker_id, data in state.items():
        elapsed = now - data.get("last_heartbeat", 0)
        result.append({
            "worker": worker_id,
            "status": data.get("status", "unknown"),
            "last_heartbeat_ago_s": int(elapsed),
            "task": data.get("task_id", ""),
        })
    return result


def clear_worker(worker_id):
    """Remove a worker from tracking (after restart)."""
    state = load_state()
    if worker_id in state:
        del state[worker_id]
        save_state(state)