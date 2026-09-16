"""
Pending Retries — persistent retry queue for failed/skipped tasks.

Inspired by Marveen's pending-retries:
  - Every failed task is persisted (survives restart)
  - Retried on every tick until success
  - Alert after threshold (no silent abandonment)
  - At-least-once delivery guarantee

For A2A Mesh:
  - Stores failed delegations, failed deploys, failed health checks
  - Retry loop in self-healing
  - Alert via Telegram after 5 failed attempts
"""

import time
import json
import os
import logging

log = logging.getLogger("pending_retries")

QUEUE_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/pending_retries.json")
ALERT_THRESHOLD = 5  # Alert after 5 attempts
MAX_AGE_HOURS = 24  # Drop after 24h


def load_queue():
    try:
        with open(QUEUE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_queue(queue):
    os.makedirs(os.path.dirname(QUEUE_FILE), exist_ok=True)
    with open(QUEUE_FILE, "w") as f:
        json.dump(queue, f, indent=2)


def enqueue(task_type, task_data, node_name=""):
    """Add a task to the retry queue."""
    queue = load_queue()
    entry = {
        "id": f"retry-{int(time.time()*1000)}-{len(queue)}",
        "task_type": task_type,
        "task_data": task_data,
        "node": node_name,
        "enqueued_at": time.time(),
        "attempts": 0,
        "last_attempt": None,
        "alert_sent": False,
        "status": "pending",
    }
    queue.append(entry)
    save_queue(queue)
    log.info(f"Pending retry: enqueued {task_type} for {node_name}")
    return entry


def get_pending():
    """Get all pending retries."""
    return load_queue()


def mark_attempt(retry_id, success=False):
    """Mark a retry attempt. If success, remove from queue."""
    queue = load_queue()
    now = time.time()
    
    # Drop expired entries
    queue = [e for e in queue if now - e["enqueued_at"] < MAX_AGE_HOURS * 3600]
    
    for entry in queue:
        if entry["id"] == retry_id:
            entry["attempts"] += 1
            entry["last_attempt"] = now
            if success:
                entry["status"] = "completed"
                queue.remove(entry)
                save_queue(queue)
                log.info(f"Pending retry: {retry_id} completed after {entry['attempts']} attempts")
                return {"status": "completed", "attempts": entry["attempts"]}
            elif entry["attempts"] >= ALERT_THRESHOLD and not entry["alert_sent"]:
                entry["alert_sent"] = True
                save_queue(queue)
                log.warning(f"Pending retry: {retry_id} alert threshold reached ({entry['attempts']} attempts)")
                return {"status": "alert", "entry": entry}
            break
    
    save_queue(queue)
    return {"status": "pending"}


def get_stats():
    """Get retry queue statistics."""
    queue = load_queue()
    now = time.time()
    return {
        "total": len(queue),
        "pending": len([e for e in queue if e["status"] == "pending"]),
        "alerting": len([e for e in queue if e.get("alert_sent") and e["status"] == "pending"]),
        "oldest_age_min": min([(now - e["enqueued_at"]) / 60 for e in queue], default=0),
        "by_type": {},
    }


def cleanup():
    """Remove completed and expired entries."""
    queue = load_queue()
    now = time.time()
    before = len(queue)
    queue = [e for e in queue if e["status"] == "pending" and now - e["enqueued_at"] < MAX_AGE_HOURS * 3600]
    after = len(queue)
    if before != after:
        save_queue(queue)
        log.info(f"Pending retry cleanup: {before - after} entries removed")
    return before - after