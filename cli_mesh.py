#!/usr/bin/env python3
"""A2A Mesh CLI — Command-line tool for mesh management.

Usage:
    a2a status          Show all node statuses
    a2a peers           Show peer connectivity matrix
    a2a health [node]   Detailed health for a node (default: all)
    a2a send <agent> <msg>   Send message to an agent
    a2a broadcast <msg>      Broadcast to all agents
    a2a delegate <agent> <task>  Delegate a task to an agent
    a2a delegations     List active delegations
    a2a agents          List registered agents
    a2a logs [node]     Show recent debug logs

Configuration:
    Reads node addresses from ~/.a2a-mesh.yaml or env vars:
    A2A_NODES="nova:localhost:8650,morzsa:192.168.1.30:8650,runa:192.168.1.100:8650"
    A2A_AUTH_TOKEN="your-token" (optional, for authenticated endpoints)
"""

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# ─── Config ───

DEFAULT_CONFIG_PATH = os.path.expanduser("~/.a2a-mesh.yaml")
DEFAULT_NODES = {
    "nova": "localhost:8650",
    "morzsa": "192.168.1.30:8650",
    "runa": "192.168.1.100:8650",
}


def load_config() -> Dict[str, str]:
    """Load node addresses from config file or env vars."""
    nodes = {}

    # Try env var first
    env_nodes = os.environ.get("A2A_NODES", "")
    if env_nodes:
        for entry in env_nodes.split(","):
            parts = entry.strip().split(":")
            if len(parts) >= 3:
                nodes[parts[0]] = f"{parts[1]}:{parts[2]}"
            elif len(parts) == 2:
                nodes[parts[0]] = parts[1]

    # Try YAML config file
    if not nodes and os.path.exists(DEFAULT_CONFIG_PATH):
        try:
            import yaml
            with open(DEFAULT_CONFIG_PATH) as f:
                cfg = yaml.safe_load(f)
                for node in cfg.get("nodes", []):
                    nodes[node["name"]] = f"{node['host']}:{node.get('port', 8650)}"
        except ImportError:
            pass
        except Exception:
            pass

    return nodes if nodes else DEFAULT_NODES


def get_auth_token() -> Optional[str]:
    return os.environ.get("A2A_AUTH_TOKEN")


# ─── HTTP ───

def api_get(host: str, path: str, token: Optional[str] = None) -> dict:
    """GET request to a mesh node API."""
    url = f"http://{host}{path}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(url, headers=headers)
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def api_post(host: str, path: str, data: dict, token: Optional[str] = None) -> dict:
    """POST request to a mesh node API."""
    url = f"http://{host}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = json.dumps(data).encode()
    req = Request(url, data=body, headers=headers, method="POST")
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


# ─── Commands ───

def cmd_status(args):
    """Show all node statuses in a table."""
    nodes = load_config()
    token = get_auth_token()
    print(f"{'Node':<10} {'Status':<12} {'Uptime':<10} {'Peers':<10} {'Version':<12}")
    print("─" * 56)
    for name, host in sorted(nodes.items()):
        try:
            d = api_get(host, "/api/health")
            status = "🟢 healthy" if d.get("running") else "🔴 down"
            uptime = f"{d.get('uptime', 0)}s"
            peers = f"{d.get('peers', {}).get('connected', 0)}/{d.get('peers', {}).get('known', 0)}"
            version = d.get("version", "?")
            print(f"{name:<10} {status:<12} {uptime:<10} {peers:<10} {version:<12}")
        except Exception as e:
            print(f"{name:<10} {'🔴 unreachable':<12} {'-':<10} {'-':<10} {'-':<12}")


def cmd_peers(args):
    """Show peer connectivity matrix."""
    nodes = load_config()
    token = get_auth_token()
    for name, host in sorted(nodes.items()):
        try:
            d = api_get(host, "/api/status")
            pd = d.get("peer_discovery", {})
            peers = pd.get("peers", {})
            print(f"\n{'='*40}")
            print(f"  {name} ({host})")
            print(f"{'='*40}")
            print(f"  Known: {pd.get('known_peers', 0)}  Connected: {pd.get('connected_peers', 0)}  Available: {pd.get('available_peers', 0)}")
            for pname, pinfo in sorted(peers.items()):
                p2p = "✅" if pinfo.get("p2p_available") or pinfo.get("p2p_connected") else "❌"
                last_seen = pinfo.get("last_seen", 0)
                if isinstance(last_seen, (int, float)):
                    ago = f"{int(time.time() - last_seen)}s ago"
                else:
                    ago = "?"
                print(f"  {p2p} {pname:<12} last_seen: {ago}")
        except Exception as e:
            print(f"\n{'='*40}")
            print(f"  {name} ({host}) — 🔴 unreachable: {e}")
            print(f"{'='*40}")


def cmd_health(args):
    """Detailed health for a node."""
    nodes = load_config()
    token = get_auth_token()
    target = args.node if args.node else None
    targets = {target: nodes[target]} if target and target in nodes else nodes
    for name, host in sorted(targets.items()):
        try:
            d = api_get(host, "/api/health")
            print(f"\n{'='*50}")
            print(f"  {name} ({host})")
            print(f"{'='*50}")
            print(f"  Status:    {'🟢 healthy' if d.get('running') else '🔴 unhealthy'}")
            print(f"  Running:   {d.get('running', False)}")
            print(f"  Uptime:    {d.get('uptime', 0)}s")
            print(f"  Version:   {d.get('version', '?')}")
            peers = d.get("peers", {})
            print(f"  Peers:     {peers.get('connected', 0)}/{peers.get('known', 0)} connected ({peers.get('available', 0)} available)")
            print(f"  Timestamp: {d.get('timestamp', '?')}")
        except Exception as e:
            print(f"\n  {name} ({host}) — 🔴 unreachable: {e}")


def cmd_send(args):
    """Send a message to an agent."""
    nodes = load_config()
    token = get_auth_token()
    # Use nova as the relay node
    host = nodes.get("nova", "localhost:8650")
    try:
        result = api_post(host, "/api/send", {
            "recipient": args.agent,
            "content": args.message,
            "msg_type": "chat",
        }, token)
        print(f"✅ Sent to {args.agent}: {args.message[:50]}...")
        if result.get("message_id"):
            print(f"   Message ID: {result['message_id']}")
    except Exception as e:
        print(f"❌ Failed to send: {e}")


def cmd_broadcast(args):
    """Broadcast a message to all agents."""
    nodes = load_config()
    token = get_auth_token()
    host = nodes.get("nova", "localhost:8650")
    try:
        result = api_post(host, "/api/send", {
            "recipient": "broadcast",
            "content": args.message,
            "msg_type": "chat",
        }, token)
        print(f"✅ Broadcast: {args.message[:50]}...")
        if result.get("message_id"):
            print(f"   Message ID: {result['message_id']}")
    except Exception as e:
        print(f"❌ Failed to broadcast: {e}")


def cmd_delegate(args):
    """Delegate a task to an agent."""
    nodes = load_config()
    token = get_auth_token()
    host = nodes.get("nova", "localhost:8650")
    try:
        result = api_post(host, "/api/delegations", {
            "to_agent": args.agent,
            "task_description": args.task,
            "priority": args.priority,
        }, token)
        print(f"✅ Delegated to {args.agent}: {args.task[:50]}...")
        if result.get("task_id"):
            print(f"   Task ID: {result['task_id']}")
            print(f"   Status:  {result.get('status', 'pending')}")
    except Exception as e:
        print(f"❌ Failed to delegate: {e}")


def cmd_delegations(args):
    """List active delegations."""
    nodes = load_config()
    token = get_auth_token()
    host = nodes.get("nova", "localhost:8650")
    try:
        result = api_get(host, "/api/delegations", token)
        delegations = result if isinstance(result, list) else result.get("delegations", [])
        if not delegations:
            print("No active delegations.")
            return
        print(f"{'Task ID':<20} {'From':<10} {'To':<10} {'Status':<12} {'Description':<40}")
        print("─" * 92)
        for d in delegations:
            print(f"{str(d.get('task_id', '?')):<20} {d.get('from_agent', '?'):<10} {d.get('to_agent', '?'):<10} {d.get('status', '?'):<12} {str(d.get('description', ''))[:40]:<40}")
    except Exception as e:
        print(f"❌ Failed to list delegations: {e}")


def cmd_agents(args):
    """List registered agents."""
    nodes = load_config()
    token = get_auth_token()
    host = nodes.get("nova", "localhost:8650")
    try:
        result = api_get(host, "/api/agents", token)
        agents = result if isinstance(result, list) else result.get("agents", [])
        if not agents:
            print("No agents registered.")
            return
        print(f"{'Name':<12} {'Status':<10} {'Capabilities':<50}")
        print("─" * 72)
        for a in agents:
            name = a.get("name", "?")
            status = "🟢 online" if a.get("online") else "🔴 offline"
            caps = ", ".join(a.get("capabilities", []))[:50]
            print(f"{name:<12} {status:<10} {caps:<50}")
    except Exception as e:
        print(f"❌ Failed to list agents: {e}")


def cmd_logs(args):
    """Show recent debug logs."""
    nodes = load_config()
    token = get_auth_token()
    target = args.node if args.node else "nova"
    host = nodes.get(target, "localhost:8650")
    try:
        result = api_get(host, f"/api/debug/logs?limit={args.limit}", token)
        logs = result if isinstance(result, list) else result.get("logs", [])
        if not logs:
            print(f"No logs from {target}.")
            return
        for log in logs:
            level = log.get("level", "?")
            ts = log.get("timestamp", "?")[:19]
            msg = log.get("message", "")[:80]
            icon = "⚠️" if level == "WARNING" else "ℹ️" if level == "INFO" else "❌"
            print(f"{icon} [{ts}] {msg}")
    except Exception as e:
        print(f"❌ Failed to get logs: {e}")


# ─── Node restart via SSH ────────────────────────────────────
_NODE_CONFIGS = {
    "nova":   {"host": "192.168.1.8",   "user": "zsolt",     "method": "launchctl", "service": "com.hermes.a2a-mesh-node"},
    "morzsa": {"host": "192.168.1.30",  "user": "openclaw",  "method": "systemd",   "service": "a2a-mesh"},
    "runa":   {"host": "192.168.1.100", "user": "zsolt",     "method": "systemd",   "service": "a2a-mesh.service"},
}


def cmd_restart(node: str):
    """Restart a remote mesh node via SSH."""
    import subprocess as _sp
    import time as _t
    import json as _json

    nodes = list(_NODE_CONFIGS.keys()) if node == "all" else [node]
    print(f"🔄 Restarting {', '.join(nodes)}...\n")

    for n in nodes:
        cfg = _NODE_CONFIGS.get(n)
        if not cfg:
            print(f"  {n}: unknown node")
            continue

        host = cfg["host"]
        m = cfg["method"]
        svc = cfg["service"]

        print(f"  {n} ({host}): restarting via {m}...", end=" ")

        if m == "launchctl":
            cmd = f"launchctl kickstart -k gui/$(id -u)/{svc}"
        elif m == "systemd":
            cmd = f"systemctl --user restart {svc}"
        else:
            cmd = "kill $(lsof -i :8650 -t | head -1) 2>/dev/null"

        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", f"{cfg['user']}@{host}", cmd]
        try:
            result = _sp.run(ssh_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                print("✅ sent")
                _t.sleep(10)
                hr = _sp.run(["curl", "-s", "--connect-timeout", "5", f"http://{host}:8650/health"],
                             capture_output=True, text=True, timeout=10)
                if hr.returncode == 0 and hr.stdout:
                    hd = _json.loads(hr.stdout)
                    print(f"  {n}: ✅ v{hd.get('version','?')} {hd.get('status','?')}")
                else:
                    print(f"  {n}: ⚠️ still starting?")
            else:
                print(f"❌ {result.stderr.strip()[:80]}")
        except Exception as e:
            print(f"❌ {e}")


# ─── Main ───

def main():
    parser = argparse.ArgumentParser(
        prog="a2a",
        description="A2A Mesh CLI — manage mesh nodes, send messages, delegate tasks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", help="Command to run")

    # status
    sub.add_parser("status", help="Show all node statuses")

    # peers
    sub.add_parser("peers", help="Show peer connectivity matrix")

    # health
    p_health = sub.add_parser("health", help="Detailed health for a node")
    p_health.add_argument("node", nargs="?", help="Node name (default: all)")

    # send
    p_send = sub.add_parser("send", help="Send a message to an agent")
    p_send.add_argument("agent", help="Recipient agent name")
    p_send.add_argument("message", help="Message content")

    # broadcast
    p_broadcast = sub.add_parser("broadcast", help="Broadcast to all agents")
    p_broadcast.add_argument("message", help="Message content")

    # delegate
    p_delegate = sub.add_parser("delegate", help="Delegate a task to an agent")
    p_delegate.add_argument("agent", help="Target agent name")
    p_delegate.add_argument("task", help="Task description")
    p_delegate.add_argument("--priority", default=5, type=int, help="Priority (1=highest, 10=lowest)")

    # delegations
    sub.add_parser("delegations", help="List active delegations")

    # agents
    sub.add_parser("agents", help="List registered agents")

    # logs
    p_logs = sub.add_parser("logs", help="Show recent debug logs")
    p_logs.add_argument("node", nargs="?", help="Node name (default: nova)")
    p_logs.add_argument("--limit", type=int, default=20, help="Number of log entries")

    # restart
    p_restart = sub.add_parser("restart", help="Restart a remote mesh node via SSH")
    p_restart.add_argument("node", choices=["nova", "morzsa", "runa", "all"], help="Node to restart")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "status": cmd_status,
        "peers": cmd_peers,
        "health": cmd_health,
        "send": cmd_send,
        "broadcast": cmd_broadcast,
        "delegate": cmd_delegate,
        "delegations": cmd_delegations,
        "agents": cmd_agents,
        "logs": cmd_logs,
        "restart": lambda a: cmd_restart(a.node),
    }

    cmd_func = commands.get(args.command)
    if cmd_func:
        cmd_func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()