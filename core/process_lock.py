"""
Process-Lock Takeover — adapted from Marveen's process-lock.ts.

Marveen eredeti: port-lock takeover — ha a dashboard restartol, megöli a zombie
elődöt ami még tartja a portot. SIGTERM → grace → SIGKILL.

A2A Mesh adaptáció:
  - node.py indításakor ellenőrzi hogy a port (8650) foglalt-e
  - Ha igen, megkeresi a zombie folyamatot (same UID, same port)
  - SIGTERM → grace period (5s) → SIGKILL ha még él
  - Így biztos hogy csak egy instance fut egyszerre
  - Bármennyi node-ra működik (mindegyik a saját portját védi)

Nem tmux-specifikus — tiszta process management.
"""

import asyncio
import logging
import os
import signal
import time
from typing import List, Optional, Tuple

log = logging.getLogger("process_lock")


async def acquire_port_lock(port: int, grace_s: float = 5.0) -> bool:
    """Ensure exclusive ownership of a TCP port.
    
    If another process holds the port, attempt graceful takeover:
    1. Find the process (same UID, holding the port)
    2. SIGTERM it
    3. Wait grace_s seconds
    4. SIGKILL if still alive
    5. Verify port is free
    
    Returns True if port is now ours, False if we couldn't acquire it.
    """
    # Check if port is free
    holders = _find_port_holders(port)
    # Exclude our own PID — we might already be binding the port
    my_pid = os.getpid()
    holders = [p for p in holders if p != my_pid]
    if not holders:
        return True  # Port is free

    log.warning(f"Port {port} is held by PIDs: {holders} — attempting takeover")

    our_uid = os.getuid()
    our_pid = os.getpid()

    # Filter to same-UID processes (don't kill other users' processes)
    target_pids = []
    for pid in holders:
        if pid == our_pid:
            continue  # Don't kill ourselves
        uid = _get_process_uid(pid)
        if uid == our_uid:
            target_pids.append(pid)
        else:
            log.warning(f"Port {port} also held by PID {pid} (UID {uid}) — not ours, skipping")

    if not target_pids:
        log.error(f"Port {port} held by other users' processes — cannot acquire")
        return False

    # Phase 1: SIGTERM
    log.info(f"SIGTERM → PIDs {target_pids}")
    for pid in target_pids:
        _signal_process(pid, signal.SIGTERM)

    # Wait grace period
    await asyncio.sleep(grace_s)

    # Check which survived
    survivors = [pid for pid in target_pids if _is_process_alive(pid)]
    if survivors:
        # Phase 2: SIGKILL
        log.warning(f"SIGKILL → survivors PIDs {survivors}")
        for pid in survivors:
            _signal_process(pid, signal.SIGKILL)

        # Brief wait for kernel cleanup
        await asyncio.sleep(1.0)

    # Verify port is free
    remaining = _find_port_holders(port)
    remaining_ours = [p for p in remaining if p != our_pid and _get_process_uid(p) == our_uid]
    if remaining_ours:
        log.error(f"Port {port} still held after takeover: {remaining_ours}")
        return False

    log.info(f"Port {port} acquired successfully")
    return True


def _find_port_holders(port: int) -> List[int]:
    """Find PIDs holding a TCP port."""
    import subprocess
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return [int(p.strip()) for p in result.stdout.strip().split("\n") if p.strip().isdigit()]
        return []  # FIX: lsof ran fine but port is free (returncode 1) — was implicit None -> 'NoneType is not iterable'
    except FileNotFoundError:
        # lsof not available — try ss (Linux)
        try:
            result = subprocess.run(
                ["ss", "-tlnp", f"sport = :{port}"],
                capture_output=True, text=True, timeout=5
            )
            pids = []
            for line in result.stdout.split("\n"):
                if "pid=" in line:
                    # Extract pid=N
                    import re
                    match = re.search(r'pid=(\d+)', line)
                    if match:
                        pids.append(int(match.group(1)))
            return pids
        except FileNotFoundError:
            log.warning("Neither lsof nor ss available — cannot find port holders")
            return []
    except Exception as e:
        log.warning(f"Port holder detection failed: {e}")
        return []


def _get_process_uid(pid: int) -> Optional[int]:
    """Get the UID of a process."""
    try:
        return os.stat(f"/proc/{pid}")[4]  # st_uid
    except (OSError, IndexError):
        pass
    # macOS / fallback: ps
    import subprocess
    try:
        result = subprocess.run(
            ["ps", "-o", "uid=", "-p", str(pid)],
            capture_output=True, text=True, timeout=3
        )
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip())
    except Exception:
        pass
    return None


def _is_process_alive(pid: int) -> bool:
    """Check if a process is still alive."""
    try:
        os.kill(pid, 0)  # Signal 0 = check existence
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # Exists but we can't signal it


def _signal_process(pid: int, sig: int):
    """Send a signal to a process, logging errors."""
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        log.debug(f"PID {pid} already gone")
    except PermissionError:
        log.warning(f"Cannot signal PID {pid} — permission denied")
    except Exception as e:
        log.warning(f"Signal {sig} to PID {pid} failed: {e}")


# ── Singleton lock check ─────────────────────────────────────

_lock_acquired = False
_lock_port: Optional[int] = None


async def ensure_single_instance(port: int, grace_s: float = 5.0) -> bool:
    """Ensure only one instance of this process is running on the given port.
    
    Call this at startup, before binding the HTTP server.
    Returns True if we successfully acquired the port.
    """
    global _lock_acquired, _lock_port
    
    if _lock_acquired and _lock_port == port:
        return True
    
    success = await acquire_port_lock(port, grace_s)
    if success:
        _lock_acquired = True
        _lock_port = port
        log.info(f"Singleton lock acquired on port {port}")
    else:
        log.error(f"Failed to acquire singleton lock on port {port}")
    
    return success


def get_lock_status() -> dict:
    """Get lock status for dashboard."""
    return {
        "acquired": _lock_acquired,
        "port": _lock_port,
        "pid": os.getpid(),
        "uid": os.getuid(),
    }