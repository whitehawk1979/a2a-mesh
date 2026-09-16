"""
Login Throttle — brute-force protection for dashboard login.

Inspired by Marveen's login-throttle:
  - 5 consecutive failures → lock 30s, doubling to 15min cap
  - Global: 50 failures/hour → all logins 429
  - Unknown username indistinguishable from wrong password

For A2A Mesh:
  - In-memory throttle (single process)
  - Per-username + global counters
  - Integrates with dashboard auth
"""

import time
import logging

log = logging.getLogger("login_throttle")

MAX_FAILURES = 5
INITIAL_LOCK = 30  # seconds
MAX_LOCK = 900     # 15 min
GLOBAL_WINDOW = 3600  # 1 hour
GLOBAL_THRESHOLD = 50

# In-memory state
_user_failures = {}  # username → {count, locked_until, last_failure}
_global_failures = []  # list of timestamps


def check_login_allowed(username):
    """Check if login is allowed for this username.
    Returns (allowed, reason)."""
    username = (username or "").lower()
    now = time.time()
    
    # Global check
    global_recent = [t for t in _global_failures if now - t < GLOBAL_WINDOW]
    _global_failures[:] = global_recent
    if len(global_recent) >= GLOBAL_THRESHOLD:
        return False, "Too many global failures. Try again later."
    
    # Per-user check
    state = _user_failures.get(username)
    if state and state.get("locked_until", 0) > now:
        remaining = int(state["locked_until"] - now)
        return False, f"Locked. Try again in {remaining}s."
    
    return True, None


def record_failure(username):
    """Record a login failure."""
    username = (username or "").lower()
    now = time.time()
    
    _global_failures.append(now)
    
    state = _user_failures.setdefault(username, {"count": 0, "locked_until": 0})
    state["count"] += 1
    state["last_failure"] = now
    
    if state["count"] >= MAX_FAILURES:
        lock_time = min(INITIAL_LOCK * (2 ** (state["count"] - MAX_FAILURES)), MAX_LOCK)
        state["locked_until"] = now + lock_time
        log.warning(f"Login throttle: {username} locked for {lock_time}s ({state['count']} failures)")


def record_success(username):
    """Record a successful login — clears counter."""
    username = (username or "").lower()
    if username in _user_failures:
        del _user_failures[username]


def get_throttle_status():
    """Get current throttle state for dashboard."""
    now = time.time()
    locked_users = [
        {"username": u, "locked_until": s["locked_until"], "failures": s["count"]}
        for u, s in _user_failures.items()
        if s.get("locked_until", 0) > now
    ]
    return {
        "locked_users": locked_users,
        "global_failures_hour": len([t for t in _global_failures if now - t < GLOBAL_WINDOW]),
        "global_threshold": GLOBAL_THRESHOLD,
        "max_failures_per_user": MAX_FAILURES,
    }