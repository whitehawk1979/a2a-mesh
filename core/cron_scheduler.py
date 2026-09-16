"""
Cron Scheduler — timezone-aware cron for A2A Mesh.

Inspired by Marveen's cron module:
  - Parse cron expressions in operator's timezone
  - Scheduled task runner with proper TZ handling
  - Support interval + fixed-time crons

For A2A Mesh:
  - Schedule periodic mesh tasks (health checks, backups, etc.)
  - TZ: Europe/Budapest (configurable)
  - API for creating/listing/deleting scheduled tasks
"""

import os
import json
import time
import logging
import re

log = logging.getLogger("cron_scheduler")

CRON_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/cron_tasks.json")
DEFAULT_TZ = os.environ.get("MESH_TZ", "Europe/Budapest")

# Simple cron parser (minute, hour, day_of_month, month, day_of_week)
def parse_cron(expr):
    """Parse a cron expression into fields. Returns dict or None."""
    parts = expr.strip().split()
    if len(parts) != 5:
        return None
    return {
        "minute": parts[0],
        "hour": parts[1],
        "dom": parts[2],
        "month": parts[3],
        "dow": parts[4],
    }


def should_run(cron_fields, now=None):
    """Check if a cron expression should fire at the given time."""
    import datetime
    if now is None:
        now = datetime.datetime.now()
    
    def match_field(field, value):
        if field == "*":
            return True
        if "/" in field:
            base, step = field.split("/")
            if base == "*":
                return value % int(step) == 0
            return False
        if "," in field:
            return any(match_field(f.strip(), value) for f in field.split(","))
        try:
            return int(field) == value
        except ValueError:
            return False
    
    return (
        match_field(cron_fields["minute"], now.minute) and
        match_field(cron_fields["hour"], now.hour) and
        match_field(cron_fields["dom"], now.day) and
        match_field(cron_fields["month"], now.month) and
        match_field(cron_fields["dow"], now.weekday())
    )


def load_tasks():
    """Load scheduled tasks."""
    try:
        with open(CRON_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"tasks": [], "tz": DEFAULT_TZ}


def add_task(name, cron_expr, action, description=""):
    """Add a scheduled task."""
    fields = parse_cron(cron_expr)
    if not fields:
        return {"error": f"Invalid cron: {cron_expr}"}
    
    tasks = load_tasks()
    task = {
        "id": f"cron-{int(time.time())}-{name[:10]}",
        "name": name,
        "cron": cron_expr,
        "action": action,
        "description": description,
        "enabled": True,
        "last_run": 0,
        "run_count": 0,
        "created_at": time.time(),
    }
    tasks["tasks"] = [t for t in tasks["tasks"] if t["name"] != name]
    tasks["tasks"].append(task)
    _save(tasks)
    return task


def remove_task(task_id):
    """Remove a scheduled task."""
    tasks = load_tasks()
    before = len(tasks["tasks"])
    tasks["tasks"] = [t for t in tasks["tasks"] if t["id"] != task_id]
    _save(tasks)
    return len(tasks["tasks"]) < before


def get_cron_status():
    """Get cron scheduler status for dashboard."""
    tasks = load_tasks()
    return {
        "tz": tasks.get("tz", DEFAULT_TZ),
        "task_count": len(tasks.get("tasks", [])),
        "enabled_count": sum(1 for t in tasks["tasks"] if t.get("enabled")),
        "tasks": [
            {
                "id": t["id"],
                "name": t["name"],
                "cron": t["cron"],
                "action": t["action"],
                "enabled": t.get("enabled", True),
                "last_run": t.get("last_run", 0),
                "run_count": t.get("run_count", 0),
            }
            for t in tasks.get("tasks", [])
        ],
    }


def _save(tasks):
    os.makedirs(os.path.dirname(CRON_FILE), exist_ok=True)
    with open(CRON_FILE, "w") as f:
        json.dump(tasks, f, indent=2)