"""
Process Lock — exclusive lock on the mesh node's TCP port.

Inspired by Marveen's process-lock:
  - On startup, find zombie processes holding the port
  - SIGTERM them, wait grace period, SIGKILL survivors
  - Prevents parallel instances with conflicting state

For A2A Mesh:
  - Lock the dashboard port (8650) on startup
  - Kill stale node processes
  - Platform-aware (macOS launchd vs Linux systemd)
"""

import os
import sys
import time
import socket
import logging
import subprocess

log = logging.getLogger("process_lock")

DEFAULT_PORT = 8650
GRACE_PERIOD = 3  # seconds before SIGKILL


def acquire_port_lock(port=DEFAULT_PORT):
    """Try to bind to the port. If it's already in use, find and kill the old process.
    Returns True if port is now available, False otherwise."""
    # First try to bind
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", port))
        sock.close()
        log.info(f"Process lock: port {port} is free")
        return True
    except OSError:
        log.warning(f"Process lock: port {port} is in use, attempting cleanup")
    
    # Find processes holding the port
    pids = _find_pids_on_port(port)
    if not pids:
        log.error(f"Process lock: cannot find process on port {port}, but it's in use")
        return False
    
    # SIGTERM
    for pid in pids:
        try:
            os.kill(pid, 15)  # SIGTERM
            log.info(f"Process lock: SIGTERM sent to PID {pid}")
        except ProcessLookupError:
            pass
        except PermissionError:
            log.error(f"Process lock: no permission to kill PID {pid}")
            return False
    
    # Wait grace period
    time.sleep(GRACE_PERIOD)
    
    # SIGKILL survivors
    for pid in pids:
        try:
            os.kill(pid, 9)  # SIGKILL
            log.warning(f"Process lock: SIGKILL sent to PID {pid}")
        except ProcessLookupError:
            pass  # Already dead — good
    
    time.sleep(1)
    
    # Try to bind again
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", port))
        sock.close()
        log.info(f"Process lock: port {port} acquired after cleanup")
        return True
    except OSError:
        log.error(f"Process lock: port {port} still in use after cleanup")
        return False


def _find_pids_on_port(port):
    """Find PIDs of processes listening on the given port."""
    pids = []
    try:
        if sys.platform == "darwin":
            # macOS: lsof
            result = subprocess.run(
                ["lsof", "-ti", f":{port}"],
                capture_output=True, text=True, timeout=5
            )
            pids = [int(p) for p in result.stdout.strip().split("\n") if p.strip().isdigit()]
        else:
            # Linux: ss or fuser
            result = subprocess.run(
                ["ss", "-tlnp", f"sport = :{port}"],
                capture_output=True, text=True, timeout=5
            )
            # Parse PID from output
            import re
            pids = [int(m) for m in re.findall(r'pid=(\d+)', result.stdout)]
            if not pids:
                result = subprocess.run(
                    ["fuser", f"{port}/tcp"],
                    capture_output=True, text=True, timeout=5
                )
                pids = [int(p) for p in result.stdout.strip().split() if p.strip().isdigit()]
    except Exception as e:
        log.debug(f"Process lock: failed to find PIDs: {e}")
    
    # Don't kill ourselves
    my_pid = os.getpid()
    pids = [p for p in pids if p != my_pid]
    return pids


def get_lock_status(port=DEFAULT_PORT):
    """Get current lock status for dashboard display."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(("127.0.0.1", port))
        sock.close()
        in_use = result == 0
        pids = _find_pids_on_port(port) if in_use else []
        return {
            "port": port,
            "in_use": in_use,
            "pids": pids,
            "our_pid": os.getpid(),
            "platform": sys.platform,
        }
    except Exception as e:
        return {"port": port, "error": str(e), "platform": sys.platform}