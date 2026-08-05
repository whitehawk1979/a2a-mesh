#!/usr/bin/env python3
"""A2A Mesh Silent Watchdog — only outputs when something is wrong.

Cron usage (no_agent=True):
- If all nodes are healthy: no stdout → no message sent
- If any node is down or degraded: stdout alert → delivered to chat
"""
import json
import sys
import urllib.request
import urllib.error
from datetime import datetime

NODES = [
    ("nova", "192.168.1.8", 8650),
    ("morzsa", "192.168.1.30", 8650),
    ("runa", "192.168.1.100", 8650),
]

ALERTS = []

for name, host, port in NODES:
    try:
        url = f"http://{host}:{port}/api/health"
        req = urllib.request.Request(url, headers={"User-Agent": "mesh-watchdog"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())

        status = data.get("status", "unknown")
        if status != "healthy":
            ALERTS.append(f"🔴 {name}: unhealthy (status={status})")
            continue

        uptime = data.get("uptime", 0)
        peers = data.get("peers", {})
        connected = peers.get("connected", 0)
        known = peers.get("known", 0)

        # Peer connectivity check
        if known > 0 and connected < known:
            ALERTS.append(f"🟠 {name}: peers {connected}/{known} — disconnected peers")

        # Uptime check — just restarted
        if uptime < 60:
            ALERTS.append(f"🟡 {name}: just restarted (uptime={uptime}s)")

    except urllib.error.URLError:
        ALERTS.append(f"🔴 {name}: UNREACHABLE ({host}:{port})")
    except Exception as e:
        ALERTS.append(f"🔴 {name}: error — {e}")

# Output only if there are alerts
if ALERTS:
    print("🚨 A2A Mesh — probléma észlelve\n")
    for alert in ALERTS:
        print(f"  {alert}")
    print(f"\nIdő: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# else: silent — no output means no delivery