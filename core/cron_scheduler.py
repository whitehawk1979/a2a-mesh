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
import subprocess
import datetime

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
        "last_result": None,   # None | "success" | "fail"
        "last_output": None,   # captured stdio on last run
        "last_duration": None, # last run duration in ms
        "last_exit": None,     # last run exit code
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


def next_run(cron_expr, from_ts=None):
    """Compute the next fire time (epoch secs) for a cron expression.

    Scans forward minute-by-minute. Returns None if invalid or no match
    found in the ~31 day horizon.
    """
    fields = parse_cron(cron_expr)
    if not fields or from_ts is None:
        return None
    base = datetime.datetime.fromtimestamp(from_ts) + datetime.timedelta(minutes=1)
    base = base.replace(second=0, microsecond=0)
    for i in range(60 * 24 * 31):
        t = base + datetime.timedelta(minutes=i)
        if should_run(fields, t):
            return t.timestamp()
    return None


def run_task(task_id, timeout=None):
    """Execute a task's action (shell command) and record the outcome.

    Returns a dict {task, ok, result, output, exit_code, duration_ms} or an
    error dict if the task is missing or has no action.
    """
    task = _find_task(task_id)
    if not task:
        return {"error": f"Task not found: {task_id}"}
    if not task.get("action"):
        return {"error": "Task has no action to run", "task": task}

    timeout = timeout or 300
    start = time.time()
    try:
        proc = subprocess.run(
            task["action"], shell=True, capture_output=True, text=True,
            timeout=timeout,
            cwd=os.path.expanduser("~/.hermes/scripts/a2a_mesh"),
        )
        ok = proc.returncode == 0
        output = (proc.stdout or "") + (("\n--- stderr ---\n" + proc.stderr) if proc.stderr else "")
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        ok = False
        output = f"TIMEOUT after {timeout}s"
        exit_code = -1
    except Exception as e:
        ok = False
        output = f"Run error: {e}"
        exit_code = -2

    duration_ms = int((time.time() - start) * 1000)
    updated = _record_run(task_id, "success" if ok else "fail", output, exit_code, duration_ms)
    return {
        "task": updated,
        "ok": ok,
        "result": "success" if ok else "fail",
        "output": output,
        "exit_code": exit_code,
        "duration_ms": duration_ms,
    }


def toggle_task(task_id):
    """Enable/disable a task. Returns updated task or None."""
    tasks = load_tasks()
    for t in tasks["tasks"]:
        if t["id"] == task_id:
            t["enabled"] = not t.get("enabled", True)
            _save(tasks)
            return t
    return None


def _find_task(task_id):
    tasks = load_tasks()
    for t in tasks["tasks"]:
        if t["id"] == task_id:
            return t
    return None


def _record_run(task_id, result, output, exit_code, duration_ms):
    """Persist the outcome of a run onto the task."""
    tasks = load_tasks()
    for t in tasks["tasks"]:
        if t["id"] == task_id:
            t["last_run"] = time.time()
            t["run_count"] = int(t.get("run_count", 0)) + 1
            t["last_result"] = result
            t["last_output"] = (output or "")[-4000:] if output else ""
            t["last_exit"] = exit_code
            t["last_duration"] = duration_ms
            _save(tasks)
            return t
    return None


def get_cron_status():
    """Get cron scheduler status for dashboard."""
    tasks = load_tasks()
    out_tasks = []
    for t in tasks.get("tasks", []):
        nr = next_run(t.get("cron", ""), t.get("last_run") or time.time())
        out_tasks.append({
            "id": t.get("id"),
            "name": t.get("name"),
            "cron": t.get("cron"),
            "action": t.get("action"),
            "description": t.get("description", ""),
            "enabled": t.get("enabled", True),
            "last_run": t.get("last_run", 0),
            "run_count": t.get("run_count", 0),
            "last_result": t.get("last_result"),
            "last_output": t.get("last_output"),
            "last_duration": t.get("last_duration"),
            "last_exit": t.get("last_exit"),
            "next_run": nr,
        })
    return {
        "tz": tasks.get("tz", DEFAULT_TZ),
        "task_count": len(out_tasks),
        "enabled_count": sum(1 for t in out_tasks if t.get("enabled")),
        "success_count": sum(1 for t in out_tasks if t.get("last_result") == "success"),
        "fail_count": sum(1 for t in out_tasks if t.get("last_result") == "fail"),
        "tasks": out_tasks,
    }


def _save(tasks):
    os.makedirs(os.path.dirname(CRON_FILE), exist_ok=True)
    with open(CRON_FILE, "w") as f:
        json.dump(tasks, f, indent=2)