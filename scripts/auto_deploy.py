#!/usr/bin/env python3
"""A2A Mesh auto-deploy script — triggered by Gitea webhook or manual run.

Pulls latest code from Gitea, deploys to all nodes, restarts services.
"""
import subprocess
import sys
import os
import time
import json

SSH_KEY = os.path.expanduser("~/.ssh/id_ed25519_openclaw")
SSH_OPTS = ["-i", SSH_KEY, "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]

NODES = {
    "morzsa": {"host": "openclaw@192.168.1.30", "restart": "systemctl --user restart a2a-mesh"},
    "runa": {"host": "zsolt@192.168.1.100", "restart": "nohup bash -c 'systemctl --user restart a2a-mesh' > /tmp/runa_restart.log 2>&1 &"},
}

# Files to deploy (relative to a2a_mesh/)
CORE_FILES = [
    "core/dashboard.html",
    "core/dashboard.py",
    "core/dashboard_admin.py",
    "core/workflow.py",
    "core/alert_manager.py",
    "core/lab.html",
    "core/auto_skill.py",
    "core/salience_decay.py",
    "core/precompact_hook.py",
    "core/kanban.py",
    "core/kanban.html",
    "core/marveen.html",
    "core/dream_engine.py",
    "core/context_guard.py",
    "core/prompt_safety.py",
    "core/costops.py",
    "core/team_trust.py",
    "core/model_fallback.py",
    "core/pending_retries.py",
    "core/tool_timeouts.py",
    "core/process_lock.py",
    "core/remote_enroll.py",
    "core/auto_restart.py",
    "core/context_gate.py",
    "core/llm_breakdown.py",
    "core/worker_liveness.py",
    "core/stuck_watcher.py",
    "core/token_usage.py",
    "core/update_preflight.py",
    "core/store_watcher.py",
    "core/vault.py",
    "core/login_throttle.py",
    "core/csrf_gate.py",
    "core/channel_health.py",
    "core/federation.py",
    "core/model_suggest.py",
    "core/voice_directive.py",
    "core/inbox_nudge.py",
    "core/memory_boundary.py",
    "core/message_router.py",
    "core/agent_team.py",
    "core/cron_scheduler.py",
    "core/update_checker.py",
    "core/network_info.py",
    "core/password_hash.py",
    "core/sanitize.py",
    "core/smart_router.py",
    "core/config.py",
    "core/dashboard_chat.py",
    "core/dashboard_agents.py",
    "core/dashboard_delegations.py",
    "core/dashboard_diagnostics.py",
    "core/delegation.py",
    "core/plugins/__init__.py",
    "core/plugins/gateway_plugin.py",
    "core/plugins/health_monitor_plugin.py",
    "core/plugins/mcp_advertiser_plugin.py",
    "core/plugins/notification_plugin.py",
    "core/plugins/skill_advertiser_plugin.py",
    "core/plugins/task_dispatch_plugin.py",
    "node.py",
    "discovery/mdns.py",
    "discovery/udp_broadcast.py",
    "transports/p2p_transport.py",
    "data/projects.json",
]

MESH_DIR = os.path.expanduser("~/.hermes/scripts/a2a_mesh")

def run(cmd, timeout=30):
    """Run a command and return (exit_code, output)."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout + r.stderr
    except Exception as e:
        return 1, str(e)

def git_pull():
    """Pull latest from Gitea."""
    print("📥 Git pull...")
    code, out = run(["git", "pull", "origin", "main"], timeout=30)
    # git pull needs to run in mesh dir
    code, out = run(["git", "-C", MESH_DIR, "pull", "origin", "main"], timeout=30)
    if code == 0:
        print("  ✅ Git pull OK")
    else:
        print(f"  ❌ Git pull failed: {out[:200]}")
    return code == 0

def deploy_to_node(name, host_info):
    """Deploy files to a remote node."""
    host = host_info["host"]
    print(f"🚀 Deploying to {name} ({host})...")
    
    # SCP core files
    for f in CORE_FILES:
        local = os.path.join(MESH_DIR, f)
        if not os.path.exists(local):
            continue
        remote_dir = f"~/a2a_mesh/{os.path.dirname(f)}"
        scp_cmd = ["scp"] + SSH_OPTS + [local, f"{host}:{remote_dir}/"]
        code, out = run(scp_cmd, timeout=15)
        if code != 0:
            print(f"  ⚠️ {f}: {out[:100]}")
    
    # Clear pycache + restart
    ssh_cmd = ["ssh"] + SSH_OPTS + [host, 
        f"find ~/a2a_mesh -name __pycache__ -exec rm -rf {{}} + 2>/dev/null; {host_info['restart']} 2>/dev/null && echo OK"
    ]
    code, out = run(ssh_cmd, timeout=20)
    if "OK" in out:
        print(f"  ✅ {name} restarted")
    else:
        print(f"  ⚠️ {name} restart: {out[:100]}")

def restart_nova():
    """Restart Nova (local)."""
    print("🚀 Restarting Nova...")
    run(["find", MESH_DIR, "-name", "__pycache__", "-exec", "rm", "-rf", "{}", "+"])
    run(["launchctl", "stop", "com.hermes.a2a-mesh-node"])
    time.sleep(3)
    run(["launchctl", "start", "com.hermes.a2a-mesh-node"])
    print("  ✅ Nova restarted")

def main():
    print("=" * 50)
    print("A2A Mesh Auto-Deploy")
    print("=" * 50)
    
    if not git_pull():
        print("❌ Git pull failed, aborting")
        sys.exit(1)
    
    # Deploy to remote nodes
    for name, info in NODES.items():
        deploy_to_node(name, info)
    
    # Restart Nova
    restart_nova()
    
    # Wait for nodes to come up
    print("\n⏳ Waiting 35s for nodes to boot...")
    time.sleep(35)
    
    # Health check
    print("\n🏥 Health check:")
    for url, label in [
        ("http://localhost:8650/health", "Nova"),
        ("http://192.168.1.30:8650/health", "Morzsa"),
        ("http://192.168.1.100:8650/health", "Runa"),
    ]:
        code, out = run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", url], timeout=10)
        status = "✅" if out == "200" else "❌"
        print(f"  {status} {label}: {out}")
    
    print("\n✅ Deploy complete!")

if __name__ == "__main__":
    main()