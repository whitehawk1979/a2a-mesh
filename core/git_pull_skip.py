"""
Git pull diff-alapú skip (idea_639b591b0ffe, tor beküldése).

Cél: csak akkor fusson `git pull`, ha a remote HEAD tényleg eltér a local
HEAD-től — így a webhook/cron/deploy útvonalak a legtöbb alkalommal
fetch+checkout nélkül, ~0 hálózati költséggel tudnak skip-pelni.

A `git ls-remote` egyetlen egyszerű hálózathívás (fetch NÉLKÜL — nem tölt
le object-eket, nem ír refs-eket), ezért ideális a "van-e értelme pull-ozni"
gyorsellenőrzésre.

Használat:
    from core.git_pull_skip import pull_if_needed
    ok, skipped, msg = pull_if_needed(repo_dir, remote="origin", branch="main")
    if skipped: ...  # local == remote, semmi teendő
"""

import os
import subprocess

__all__ = ["remote_head", "local_head", "pull_needed", "pull_if_needed"]


def _run(cmd, cwd, timeout=15):
    """Run command, return (returncode, stdout+stderr)."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return r.returncode, (r.stdout + r.stderr).strip()
    except Exception as e:
        return 1, str(e)


def local_head(repo_dir, branch="main"):
    """Local HEAD commit hash (None ha nem kérdezhető le)."""
    code, out = _run(["git", "rev-parse", f"refs/heads/{branch}"], repo_dir, timeout=5)
    return out.split()[0] if code == 0 and out else None


def remote_head(repo_dir, remote="origin", branch="main"):
    """Remote branch HEAD `git ls-remote`-dal — fetch nélkül (None ha elérhetetlen)."""
    code, out = _run(["git", "ls-remote", remote, f"refs/heads/{branch}"], repo_dir, timeout=15)
    if code == 0 and out:
        first = out.splitlines()[0].split()
        if first:
            return first[0]
    return None


def pull_needed(repo_dir, remote="origin", branch="main"):
    """True ha a remote HEAD != local HEAD (vagy bármelyik lekérdezés sikertelen
    — ekkor biztonsági okból legyen pull, ne skip-peljünk hibára)."""
    local = local_head(repo_dir, branch)
    if local is None:
        return True, "local HEAD unavailable → pull"
    remote = remote_head(repo_dir, remote, branch)
    if remote is None:
        return True, "remote HEAD unavailable → pull"
    if remote == local:
        return False, f"up to date ({local[:7]})"
    return True, f"remote {remote[:7]} != local {local[:7]} → pull"


def pull_if_needed(repo_dir, remote="origin", branch="main", dry_run=False):
    """Diff-alapú pull: fetch+checkout CSAK ha a remote HEAD eltér a localtól.

    Returns (pulled, skipped, message):
      pulled  — True ha pull futott (és sikerült), False ha nem futott / hibás
      skipped — True ha a pull el lett skip-pelve (local == remote)
    """
    need, why = pull_needed(repo_dir, remote=remote, branch=branch)
    if not need:
        return False, True, why
    if dry_run:
        return False, False, f"pull would run: {why}"
    code, out = _run(["git", "pull", remote, branch], repo_dir, timeout=60)
    if code == 0:
        return True, False, out.strip() or "pull OK"
    return False, False, f"pull failed: {out[:300]}"


if __name__ == "__main__":
    import sys
    repo = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    rem = sys.argv[2] if len(sys.argv) > 2 else "origin"
    br = sys.argv[3] if len(sys.argv) > 3 else "main"
    p, s, msg = pull_if_needed(repo, rem, br)
    print(f"pulled={p} skipped={s}: {msg}")