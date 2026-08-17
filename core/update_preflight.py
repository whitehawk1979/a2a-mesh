"""
Update Preflight — check before git pull/deploy.

Inspired by Marveen's update-preflight:
  - Before updating: check git state (detached HEAD, local mods, branch)
  - Prevents silent update failures
  - Returns can_update + blocking issues

For A2A Mesh:
  - Checks local git state before auto-deploy
  - Verifies branch, no local changes, no detached HEAD
  - Returns preflight result
"""

import subprocess
import os
import logging

log = logging.getLogger("update_preflight")

MESH_DIR = os.path.expanduser("~/.hermes/scripts/a2a_mesh")


def run_preflight(repo_dir=None):
    """Run preflight checks before updating.
    Returns dict with can_update, issues, warnings."""
    repo = repo_dir or MESH_DIR
    issues = []
    warnings = []
    
    # 1. Check if directory exists
    if not os.path.isdir(os.path.join(repo, ".git")):
        issues.append(f"Not a git repo: {repo}")
        return {"can_update": False, "issues": issues, "warnings": warnings}
    
    # 2. Check current branch
    try:
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=repo, capture_output=True, text=True, timeout=5
        )
        branch = result.stdout.strip()
        if not branch:
            issues.append("Detached HEAD — cannot fast-forward")
        elif branch != "main":
            warnings.append(f"On branch '{branch}', not 'main'")
    except Exception as e:
        issues.append(f"Cannot determine branch: {e}")
    
    # 3. Check for local modifications
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo, capture_output=True, text=True, timeout=5
        )
        if result.stdout.strip():
            modified = result.stdout.strip().split("\n")
            for m in modified[:5]:
                warnings.append(f"Local change: {m.strip()}")
            if len(modified) > 5:
                warnings.append(f"... and {len(modified)-5} more")
    except Exception as e:
        warnings.append(f"Cannot check status: {e}")
    
    # 4. Check remote connectivity
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=repo, capture_output=True, text=True, timeout=5
        )
        remote = result.stdout.strip()
        if not remote:
            issues.append("No 'origin' remote configured")
    except Exception:
        warnings.append("Cannot check remote")
    
    can_update = len(issues) == 0
    return {
        "can_update": can_update,
        "branch": branch if 'branch' in dir() else "unknown",
        "issues": issues,
        "warnings": warnings,
        "repo": repo,
    }