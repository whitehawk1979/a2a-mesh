#!/usr/bin/env python3
"""
Session Cleanup Script — removes expired sessions from the auth DB.

Designed to be run as a cron-scheduler task (cron_scheduler.py).
Usage:
    python3 scripts/cleanup_sessions.py

Can also be imported and called programmatically:
    from core.auth import AuthManager
    auth = AuthManager()
    auth.cleanup_sessions()
"""
import os
import sys
import time
import logging

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
log = logging.getLogger("session_cleanup")


def run():
    """Run the cleanup and return a human-readable summary."""
    try:
        from core.auth import AuthManager
        
        auth = AuthManager()
        before_count = len(auth.list_active_sessions())
        
        start = time.time()
        auth.cleanup_sessions()
        elapsed = time.time() - start
        
        after_count = len(auth.list_active_sessions())
        removed = before_count - after_count
        
        log.info(
            f"Session cleanup: {removed} removed, {after_count} remaining "
            f"(took {elapsed*1000:.0f}ms)"
        )
        return {
            "ok": True,
            "before": before_count,
            "after": after_count,
            "removed": removed,
            "duration_ms": int(elapsed * 1000),
        }
    except Exception as e:
        log.error(f"Session cleanup failed: {e}")
        return {"ok": False, "error": str(e)}


if __name__ == "__main__":
    result = run()
    if not result.get("ok"):
        sys.exit(1)
    print(f"Removed {result.get('removed', 0)} expired sessions. "
          f"Active: {result.get('after', 0)}")