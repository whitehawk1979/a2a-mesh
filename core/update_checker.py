"""
Update Checker — track git commits and pending updates.

Inspired by Marveen's update-checker:
  - Compare local HEAD vs remote
  - List pending commits
  - Show release versions with summaries
  - Preflight before update

For A2A Mesh:
  - Check Gitea for new commits
  - Show what's pending to deploy
  - Release tracking
"""

import os
import subprocess
import logging
from pathlib import Path

log = logging.getLogger("update_checker")

MESH_DIR = os.path.expanduser("~/.hermes/scripts/a2a_mesh")


def get_local_head():
    """Get local git HEAD."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=MESH_DIR,
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None


def get_remote_head():
    """Get remote git HEAD."""
    try:
        result = subprocess.run(
            ["git", "ls-remote", "origin", "main"],
            cwd=MESH_DIR,
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            return result.stdout.split()[0]
    except Exception:
        pass
    return None


def get_pending_commits():
    """Get list of commits not yet pulled."""
    try:
        result = subprocess.run(
            ["git", "log", "--oneline", "HEAD..origin/main"],
            cwd=MESH_DIR,
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            return [l for l in lines if l]
    except Exception:
        pass
    return []


def get_recent_commits(count=10):
    """Get recent local commits."""
    try:
        result = subprocess.run(
            ["git", "log", f"--oneline", f"-{count}"],
            cwd=MESH_DIR,
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip().split("\n")
    except Exception:
        pass
    return []


def get_branch():
    """Get current git branch."""
    try:
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=MESH_DIR,
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def get_update_status():
    """Get update status for dashboard."""
    local = get_local_head()
    remote = get_remote_head()
    pending = get_pending_commits()
    recent = get_recent_commits(5)
    branch = get_branch()
    
    return {
        "branch": branch,
        "local_head": (local or "")[:7],
        "remote_head": (remote or "")[:7],
        "up_to_date": local == remote if local and remote else None,
        "pending_count": len(pending),
        "pending_commits": pending[:10],
        "recent_commits": recent,
        "update_available": local != remote if local and remote else False,
    }