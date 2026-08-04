#!/usr/bin/env python3
"""A2A Mesh — Telegram bot command handler.

Handles /mesh commands from Telegram via the Hermes gateway.
Run as a cron job or webhook endpoint that receives /mesh commands
and responds with mesh status/info.

Commands:
  /mesh status    — Show all node statuses
  /mesh restart <node> — Restart a node (nova/morzsa/runa/all)
  /mesh skills    — List marketplace skills
  /mesh health    — Detailed health for all nodes
  /mesh alerts    — Show active Prometheus alerts
  /mesh help      — Show available commands

Usage:
  Called by Hermes when a /mesh command is received.
  Reads the command from $1 or stdin, outputs response to stdout.
"""
import json
import os
import subprocess
import sys
import urllib.request

NODES = {
    "nova":   "192.168.1.8",
    "morzsa": "192.168.1.30",
    "runa":   "192.168.1.100",
}

PROMETHEUS_URL = "http://192.168.1.100:9090"

_NODE_CONFIGS = {
    "nova":   {"host": "192.168.1.8",   "user": "zsolt",     "method": "launchctl", "service": "com.hermes.a2a-mesh-node"},
    "morzsa": {"host": "192.168.1.30",  "user": "openclaw",  "method": "systemd",   "service": "a2a-mesh"},
    "runa":   {"host": "192.168.1.100", "user": "zsolt",     "method": "systemd",   "service": "a2a-mesh.service"},
}


def api_get(host: str, path: str) -> dict:
    """GET request to mesh node API (no auth for public endpoints)."""
    try:
        url = f"http://{host}:8650{path}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def cmd_status():
    """Show all node statuses."""
    lines = ["📊 **A2A Mesh Status**\n"]
    for name, host in sorted(NODES.items()):
        d = api_get(host, "/api/health")
        if "error" in d:
            lines.append(f"🔴 **{name}**: unreachable")
        else:
            ver = d.get("version", "?")
            status = d.get("status", "?")
            uptime = d.get("uptime_seconds", 0)
            peers = d.get("peers_connected", "?")
            icon = "🟢" if status == "running" else "🟠"
            up_str = f"{uptime:.0f}s" if isinstance(uptime, (int, float)) else "?"
            lines.append(f"{icon} **{name}**: v{ver} {status} | peers={peers} | uptime={up_str}")
    return "\n".join(lines)


def cmd_health():
    """Detailed health for all nodes."""
    lines = ["🩺 **A2A Mesh Health**\n"]
    for name, host in sorted(NODES.items()):
        d = api_get(host, "/api/health")
        if "error" in d:
            lines.append(f"🔴 **{name}**: {d['error'][:50]}")
            continue
        lines.append(f"**{name}** (v{d.get('version','?')}):")
        lines.append(f"  status: {d.get('status','?')}")
        lines.append(f"  uptime: {d.get('uptime_seconds',0):.0f}s")
        lines.append(f"  peers: {d.get('peers_connected','?')}")
        transports = d.get("transports", {})
        t_str = ", ".join(f"{k}={v}" for k, v in transports.items() if isinstance(v, bool))
        lines.append(f"  transports: {t_str}")
        election = d.get("election", {})
        if election:
            lines.append(f"  coordinator: {election.get('coordinator','?')}")
        lines.append("")
    return "\n".join(lines)


def cmd_restart(node: str):
    """Restart a node via SSH."""
    if node not in _NODE_CONFIGS and node != "all":
        return f"❌ Unknown node: {node}\nValid: {', '.join(_NODE_CONFIGS.keys())}, all"

    nodes = list(_NODE_CONFIGS.keys()) if node == "all" else [node]
    lines = [f"🔄 Restarting {', '.join(nodes)}...\n"]

    for n in nodes:
        cfg = _NODE_CONFIGS[n]
        host = cfg["host"]
        m = cfg["method"]
        svc = cfg["service"]

        if m == "launchctl":
            cmd = f"launchctl kickstart -k gui/$(id -u)/{svc}"
        elif m == "systemd":
            cmd = f"systemctl --user restart {svc}"
        else:
            cmd = f"kill $(lsof -i :8650 -t | head -1) 2>/dev/null"

        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", f"{cfg['user']}@{host}", cmd]
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                lines.append(f"✅ {n}: restart command sent")
            else:
                lines.append(f"❌ {n}: {result.stderr.strip()[:60]}")
        except Exception as e:
            lines.append(f"❌ {n}: {e}")

    lines.append("\nWait ~15s then run /mesh status to verify.")
    return "\n".join(lines)


def cmd_skills():
    """List marketplace skills."""
    d = api_get("192.168.1.8", "/api/skills")
    if "error" in d:
        return f"❌ Cannot reach Nova API: {d['error']}"
    skills = d.get("skills", [])
    if not skills:
        return "📭 No skills in marketplace"
    lines = ["🧠 **Skills Marketplace**\n"]
    for s in skills:
        name = s.get("skill_name", "?")
        agent = s.get("agent", "?")
        status = s.get("status", "?")
        desc = (s.get("description") or "")[:50]
        lines.append(f"• **{name}** (agent: {agent}, {status})")
        if desc:
            lines.append(f"  {desc}")
    return "\n".join(lines)


def cmd_alerts():
    """Show active Prometheus alerts."""
    try:
        url = f"{PROMETHEUS_URL}/api/v1/alerts"
        with urllib.request.urlopen(url, timeout=5) as resp:
            d = json.loads(resp.read())
        alerts = d.get("data", {}).get("alerts", [])
        if not alerts:
            return "✅ No active alerts"
        lines = [f"🚨 **{len(alerts)} Active Alert(s)**\n"]
        for a in alerts:
            labels = a.get("labels", {})
            ann = a.get("annotations", {})
            sev = labels.get("severity", "info")
            icon = {"critical": "🔴", "warning": "🟠", "info": "🔵"}.get(sev, "⚪")
            lines.append(f"{icon} **{labels.get('alertname','?')}** ({sev}, {a.get('state','?')})")
            if labels.get("node"):
                lines.append(f"  node: {labels['node']}")
            if ann.get("summary"):
                lines.append(f"  {ann['summary']}")
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Cannot reach Prometheus: {e}"


def cmd_help():
    """Show available commands."""
    return """🤖 **A2A Mesh Commands**

/mesh status — All node statuses
/mesh health — Detailed health info
/mesh restart <node> — Restart node (nova/morzsa/runa/all)
/mesh skills — List marketplace skills
/mesh alerts — Show active alerts
/mesh help — This message"""


COMMANDS = {
    "status": cmd_status,
    "health": cmd_health,
    "restart": cmd_restart,
    "skills": cmd_skills,
    "alerts": cmd_alerts,
    "help": cmd_help,
}


def main():
    # Read command from argument or stdin
    if len(sys.argv) > 1:
        cmd_str = " ".join(sys.argv[1:]).strip()
    else:
        cmd_str = sys.stdin.read().strip()

    if not cmd_str:
        print(cmd_help())
        return

    # Parse: /mesh <subcommand> [args]
    parts = cmd_str.split()
    if parts[0] == "/mesh":
        parts = parts[1:]

    if not parts:
        print(cmd_help())
        return

    subcmd = parts[0].lower()
    handler = COMMANDS.get(subcmd, cmd_help)

    if subcmd == "restart" and len(parts) > 1:
        print(handler(parts[1]))
    else:
        print(handler())


if __name__ == "__main__":
    main()