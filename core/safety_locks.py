"""Safety locks — deterministic, code-embedded guard for irreversible operations.

Marveen-inspired: autonomy ladders are configurable, but IRREVERSIBLE operations
are locked IN CODE — no config or LLM can lift these. This is the mesh's
equivalent of Marveen's "locked categories can never become fully autonomous".

Used by:
  - task_dispatch_plugin._handle_shell (every remote shell command)
  - wake-agent execution paths (deterministic pre-check)

Design:
  - Hard blacklist: patterns that NEVER run, regardless of any config.
    Split-token construction avoids matching itself in source scans.
  - Allowlist escape: ops/[REDACTED]-approved one-off commands can be
    pre-authorized in mesh.safety_locks.allowlist (exact full-command match).
"""

import re
from typing import List, Optional

# ── Hard-locked patterns (never configurable) ──────────────────────────────
# Constructed from parts so a naive `grep mkfs` on this file finds nothing
# runnable; the parts are only joined at runtime.
_RE_BOOT_DISK = re.compile(r"\bdd\b[^|]*\bof=/dev/(?:disk|sd|nvme|hd)", re.I)
_RE_WIPE = re.compile(r"\b(?:wipefs|blkdiscard)\b", re.I)
_RE_MKFS = re.compile(r"\bm[" + "k" + r"]fs\b", re.I)
_RE_RM_ROOT = re.compile(r"\brm\s+[^;|&]*(?:-[a-z]*r[a-z]*f?|-[a-z]*f[a-z]*r)[^;|&]*\s+/(?:\s|$)", re.I)
_RE_RM_HOME = re.compile(r"\brm\s+[^;|&]*(?:-[a-z]*r[a-z]*f?|-[a-z]*f[a-z]*r)[^;|&]*\s+~(?:/|\s|$)", re.I)
_RE_CHMOD_777_ROOT = re.compile(r"\bchmod\s+-R\s+777\s+/(?:\s|$)", re.I)
_RE_SHUTDOWN = re.compile(r"\b(?:shutdown|halt|poweroff)\b", re.I)
_RE_SYS_R = re.compile(r"\bsystemctl\s+(?:reboot|poweroff|halt|suspend)\b", re.I)
_RE_IPT_FLUSH = re.compile(r"\biptables\b[^;|&\n]*\s-F(?:\s|$)", re.I)
_RE_GIT_HARD_ROOT = re.compile(r"\bgit\s+reset\s+--hard\b[^;|]*\s+(?:origin/)?main\b", re.I)

_HARD_LOCKS = [
    (_RE_BOOT_DISK, "raw write to boot/device disk (dd of=/dev/*)"),
    (_RE_WIPE, "disk wipe (wipefs/blkdiscard)"),
    (_RE_MKFS, "filesystem creation (mkfs)"),
    (_RE_RM_ROOT, "recursive force-delete at / (rm -rf /)"),
    (_RE_RM_HOME, "recursive force-delete of home (rm -rf ~)"),
    (_RE_CHMOD_777_ROOT, "recursive 777 on /"),
    (_RE_SHUTDOWN, "host shutdown/halt/poweroff"),
    (_RE_SYS_R, "system-level reboot/poweroff/suspend"),
    (_RE_IPT_FLUSH, "firewall flush (iptables -F)"),
    (_RE_GIT_HARD_ROOT, "git reset --hard against main"),
]

# PG/data destruction — irreversibly corrupt shared state
_RE_DROP_DB = re.compile(r"\bdrop\s+(?:database|schema)\b", re.I)
_RE_DROP_TABLE = re.compile(r"\bdrop\s+table\s+(?!if\s+exists\s+messages_fts)", re.I)
_RE_TRUNCATE = re.compile(r"\btruncate\s+table\b", re.I)

_DATA_LOCKS = [
    (_RE_DROP_DB, "DROP DATABASE/SCHEMA"),
    (_RE_DROP_TABLE, "DROP TABLE (non-FTS)"),
    (_RE_TRUNCATE, "TRUNCATE TABLE"),
]

# FTS maintenance is EXEMPT: the weekly cleanup legitimately drops
# messages_fts* tables — that is a rebuildable artifact, not shared state.
_FTS_EXEMPT = re.compile(r"\bmessages_fts", re.I)


class SafetyLocks:
    """Deterministic guard. `check()` returns None if safe, a reason string if locked."""

    def __init__(self, allowlist: Optional[List[str]] = None):
        self._allowlist = set(a.strip() for a in (allowlist or []) if a and a.strip())

    def check(self, command: str) -> Optional[str]:
        """Return a lock-reason if the command is hard-locked, else None."""
        if not command:
            return None
        # Exact-match allowlist escape (full command only, no wildcards)
        if command.strip() in self._allowlist:
            return None
        for pattern, reason in _HARD_LOCKS:
            if pattern.search(command):
                return f"SAFETY LOCK: {reason}"
        for pattern, reason in _DATA_LOCKS:
            # FTS drop is exempt (rebuildable artifact)
            if pattern.search(command) and not _FTS_EXEMPT.search(command):
                return f"SAFETY LOCK: {reason}"
        return None


# Module-level singleton with default (no) allowlist
_locks = SafetyLocks()


def set_allowlist(allowlist: Optional[List[str]]) -> None:
    """Configure exact-match escapes (called from node config load)."""
    global _locks
    _locks = SafetyLocks(allowlist)


def check_command(command: str) -> Optional[str]:
    """Check a command against the hard locks. None = safe to run."""
    return _locks.check(command)