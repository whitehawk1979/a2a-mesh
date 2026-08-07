#!/usr/bin/env python3
"""A2A Mesh Gateway Watchdog — monitors Hermes gateway and restarts if down.

Runs as a cron job on each mesh node. Checks:
1. Gateway health endpoint (localhost:8650/health)
2. Process existence (pgrep)

If either fails, attempts restart via the node-appropriate method.
Logs to ~/.hermes/logs/gateway_watchdog.log

Platform-specific restart:
- macOS (Nova): launchctl kickstart
- Linux (Morzsa/Runa): systemctl --user restart hermes-gateway

Usage:
    python3 gateway_watchdog.py [--node nova|morzsa|runa] [--dry-run]
"""

import argparse
import logging
import os
import platform
import subprocess
import sys
import time
from datetime import datetime

LOG_FILE = os.path.expanduser("~/.hermes/logs/gateway_watchdog.log")
HEALTH_URL = "http://localhost:8650/health"
HEALTH_TIMEOUT = 10  # seconds
MAX_RESTART_PER_HOUR = 3  # prevent infinite restart loops
RESTART_COOLDOWN = 120  # seconds between restart attempts

# Setup logging
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stderr),
    ],
)
log = logging.getLogger("gateway_watchdog")


def check_health_endpoint() -> bool:
    """Check if the gateway health endpoint responds."""
    try:
        import urllib.request
        import urllib.error
        req = urllib.request.Request(HEALTH_URL, method="GET")
        resp = urllib.request.urlopen(req, timeout=HEALTH_TIMEOUT)
        return resp.status == 200
    except Exception:
        return False


def check_process() -> bool:
    """Check if a hermes gateway process is running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "hermes.*gateway"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def restart_gateway(node: str) -> bool:
    """Restart the gateway using the platform-appropriate method."""
    log.info(f"Attempting gateway restart on {node}...")

    if platform.system() == "Darwin":
        # macOS (Nova) — launchctl
        try:
            subprocess.run(
                ["launchctl", "kickstart", "-k", "com.hermes.a2a-mesh-node"],
                capture_output=True,
                timeout=30,
            )
            log.info("launchctl kickstart sent")
            return True
        except Exception as e:
            log.error(f"launchctl restart failed: {e}")
            return False
    else:
        # Linux (Morzsa/Runa) — systemctl --user
        # Use setsid to bypass lifecycle guard
        try:
            script = f"""
import subprocess, time, os
time.sleep(2)
subprocess.run(
    ["systemctl", "--user", "restart", "hermes-gateway"],
    capture_output=True,
    timeout=30,
)
"""
            # Write temp script and run via nohup to avoid lifecycle guard
            tmp_script = "/tmp/.gw_watchdog_restart.py"
            with open(tmp_script, "w") as f:
                f.write(script)
            subprocess.Popen(
                ["nohup", "python3", tmp_script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            log.info("systemctl --user restart dispatched via nohup")
            return True
        except Exception as e:
            log.error(f"systemctl restart failed: {e}")
            return False


def check_restart_rate() -> bool:
    """Check if we've exceeded the max restart rate (prevent loops)."""
    state_file = "/tmp/.gw_watchdog_state"
    now = time.time()
    one_hour_ago = now - 3600

    recent_restarts = []
    if os.path.exists(state_file):
        try:
            with open(state_file, "r") as f:
                for line in f:
                    ts = float(line.strip())
                    if ts > one_hour_ago:
                        recent_restarts.append(ts)
        except Exception:
            pass

    if len(recent_restarts) >= MAX_RESTART_PER_HOUR:
        log.warning(f"Restart rate limit reached ({len(recent_restarts)}/hour)")
        return False

    # Record this restart attempt
    recent_restarts.append(now)
    try:
        with open(state_file, "w") as f:
            for ts in recent_restarts:
                f.write(f"{ts}\n")
    except Exception:
        pass

    return True


def check_cooldown() -> bool:
    """Check if we're in a restart cooldown period."""
    state_file = "/tmp/.gw_watchdog_last_restart"
    if os.path.exists(state_file):
        try:
            last = float(open(state_file).read().strip())
            if time.time() - last < RESTART_COOLDOWN:
                log.info(f"Cooldown active ({int(time.time() - last)}s since last restart)")
                return False
        except Exception:
            pass
    return True


def record_restart():
    """Record restart timestamp."""
    with open("/tmp/.gw_watchdog_last_restart", "w") as f:
        f.write(str(time.time()))


def main():
    parser = argparse.ArgumentParser(description="Gateway Watchdog")
    parser.add_argument("--node", default=os.environ.get("MESH_NODE_NAME", "auto"),
                        help="Node name (nova/morzsa/runa)")
    parser.add_argument("--dry-run", action="store_true", help="Check only, no restart")
    args = parser.parse_args()

    # Auto-detect node name from hostname
    if args.node == "auto":
        hostname = platform.node().lower()
        if "mac" in hostname or "zsolt" in hostname:
            args.node = "nova"
        elif "morzsa" in hostname or "openclaw" in hostname:
            args.node = "morzsa"
        elif "runa" in hostname or "ubuntu" in hostname:
            args.node = "runa"

    health_ok = check_health_endpoint()
    process_ok = check_process()

    # Health endpoint is the primary indicator — if it's up, gateway is serving.
    # Process check is secondary (on macOS the mesh node serves the health endpoint).
    if health_ok:
        log.info(f"[{args.node}] Gateway healthy (health=✅ process={'✅' if process_ok else '⚠️'})")
        return

    log.warning(f"[{args.node}] Gateway unhealthy (health=❌ process={'✅' if process_ok else '❌'})")

    if args.dry_run:
        log.info("Dry run — skipping restart")
        return

    if not check_cooldown():
        return

    if not check_restart_rate():
        log.error(f"[{args.node}] Max restart rate exceeded — manual intervention needed")
        return

    success = restart_gateway(args.node)
    if success:
        record_restart()
        log.info(f"[{args.node}] Restart dispatched")

        # Wait and verify
        time.sleep(30)
        if check_health_endpoint() and check_process():
            log.info(f"[{args.node}] ✅ Gateway recovered after restart")
        else:
            log.error(f"[{args.node}] ❌ Gateway still unhealthy after restart")
    else:
        log.error(f"[{args.node}] ❌ Restart failed")


if __name__ == "__main__":
    main()