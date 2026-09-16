"""
Desired State Reconciler — adapted from Marveen's agent-desired-state.ts.

Marveen eredeti: agents-desired.json fájl tartalmazza mely agent-eknek kell futni,
a channel monitor reconcile-álja a valóságot ehhez (restart after nuke/reboot).

A2A Mesh adaptáció:
  - PG tábla: desired_nodes (node_name, enabled, ssh_target, ssh_key, restart_cmd, ...)
  - Dinamikus: bármennyi node regisztrálható (nem csak 3 hardcoded)
  - Minden node regisztrálja magát a desired listába induláskor (auto-enroll)
  - A health monitor loop reconcile-ál: ha egy desired node nem látható,
    heartbeat timeout után auto-restart kísérlet (SSH-n keresztül)
  - Explicit stop kiveszi a desired listából (nem resurrected)
  - Új node-ok auto-enroll: ha a PG-ben még nincs, de a registry-ben megjelenik,
    automatikusan desired lesz
"""

import asyncio
import logging
import json
import time
import os
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field

log = logging.getLogger("desired_state")

# ── Desired node state ────────────────────────────────────────

@dataclass
class DesiredNode:
    """Desired state for a single node."""
    node_name: str
    enabled: bool = True
    last_seen: float = 0.0  # unix timestamp
    restart_attempts: int = 0
    last_restart: float = 0.0
    ssh_target: str = ""  # user@host
    ssh_key: str = ""     # path to SSH key
    restart_cmd: str = "" # command to restart the node


# Known SSH targets for auto-enroll (expanded as nodes are added)
# New nodes can also register themselves via API
SSH_TARGETS = {
    "nova": {
        "ssh_target": "",  # local
        "restart_cmd": "launchctl stop com.hermes.a2a-mesh-node && launchctl start com.hermes.a2a-mesh-node",
    },
    "morzsa": {
        "ssh_target": "openclaw@192.168.1.30",
        "ssh_key": "~/.ssh/id_ed25519_openclaw",
        "restart_cmd": "systemctl --user restart a2a-mesh.service",
    },
    "runa": {
        "ssh_target": "zsolt@192.168.1.100",
        "ssh_key": "~/.ssh/id_ed25519_openclaw",
        "restart_cmd": "systemctl --user restart a2a-mesh.service",
    },
    # HAOS nodes: embedded sshd on port 2230 (root@192.168.1.43), mesh process
    # parent is openclaw-gateway — restart via 'cd /config/a2a_mesh && cli.py start'
    # is NOT safe here (kills parent's child). Track-only until a safe remote
    # restart path exists.
    "tor": {
        "ssh_target": "root@192.168.1.43",
        "ssh_key": "~/.ssh/id_ed25519_openclaw",
        "restart_cmd": "",
    },
    "mano": {
        "ssh_target": "root@192.168.1.43",
        "ssh_key": "~/.ssh/id_ed25519_openclaw",
        "restart_cmd": "",
    },
}

# In-memory desired state — dynamically populated
_desired_nodes: Dict[str, DesiredNode] = {}
_pg_pool = None
_initialized = False


def set_pg_pool(pool):
    global _pg_pool
    _pg_pool = pool


async def ensure_initialized():
    """Initialize desired state from PG or seed defaults."""
    global _initialized
    if _initialized:
        return

    if _pg_pool:
        # FIX (v0.43.2): a táblát korábban sosem hozta létre senki — minden
        # node-start "PG load failed" logot adott, és az auto_enroll INSERT is
        # csendesen elbukott. Most idempotens CREATE TABLE IF NOT EXISTS.
        try:
            await _pg_pool.execute(
                "CREATE TABLE IF NOT EXISTS desired_nodes ("
                " node_name TEXT PRIMARY KEY,"
                " enabled BOOLEAN NOT NULL DEFAULT true,"
                " ssh_target TEXT NOT NULL DEFAULT '',"
                " ssh_key TEXT NOT NULL DEFAULT '',"
                " restart_cmd TEXT NOT NULL DEFAULT '',"
                " created_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
                " updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"
                ")"
            )
        except Exception as e:
            log.warning(f"desired_nodes CREATE TABLE failed (non-fatal): {e}")

        try:
            # Try to load from PG
            rows = await _pg_pool.fetch(
                "SELECT node_name, enabled, ssh_target, ssh_key, restart_cmd "
                "FROM desired_nodes"
            )
            for row in rows:
                _desired_nodes[row["node_name"]] = DesiredNode(
                    node_name=row["node_name"],
                    enabled=row["enabled"],
                    ssh_target=row["ssh_target"] or "",
                    ssh_key=row["ssh_key"] or "",
                    restart_cmd=row["restart_cmd"] or "",
                )
            log.info(f"Loaded {len(_desired_nodes)} desired nodes from PG")
            if not _desired_nodes:
                # Table exists but is empty — seed defaults so reconcile
                # has nodes to track (otherwise zero desired nodes forever)
                log.info("desired_nodes table empty — seeding defaults")
                _seed_defaults()
                # FIX (v0.43.2): persist seeded defaults so the next start loads from PG
                for name, dn in _desired_nodes.items():
                    try:
                        await _pg_pool.execute(
                            "INSERT INTO desired_nodes (node_name, enabled, ssh_target, ssh_key, restart_cmd) "
                            "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (node_name) DO NOTHING",
                            name, dn.enabled, dn.ssh_target, dn.ssh_key, dn.restart_cmd,
                        )
                    except Exception as e:
                        log.debug(f"Seed persist for {name} failed (non-fatal): {e}")
        except Exception as e:
            # Unexpected load failure — seed defaults in memory
            log.info(f"PG load failed ({e}), seeding defaults")
            _seed_defaults()
    else:
        _seed_defaults()

    _initialized = True


def _seed_defaults():
    """Seed default known nodes."""
    for name, cfg in SSH_TARGETS.items():
        _desired_nodes[name] = DesiredNode(
            node_name=name,
            ssh_target=cfg.get("ssh_target", ""),
            ssh_key=cfg.get("ssh_key", ""),
            restart_cmd=cfg.get("restart_cmd", ""),
        )


async def auto_enroll_from_registry(known_nodes: Dict[str, Dict]):
    """Auto-enroll new nodes that appear in registry but not in desired state.
    
    New nodes that join the mesh are automatically added to desired state.
    """
    for name, info in known_nodes.items():
        if name not in _desired_nodes:
            # Try to find SSH target from known configs or registry info
            ssh_target = info.get("ssh_target", "")
            ssh_key = info.get("ssh_key", "~/.ssh/id_ed25519_openclaw")
            restart_cmd = info.get("restart_cmd", "")

            # Check if we have a known SSH target
            if name in SSH_TARGETS:
                cfg = SSH_TARGETS[name]
                ssh_target = cfg.get("ssh_target", "")
                ssh_key = cfg.get("ssh_key", "")
                restart_cmd = cfg.get("restart_cmd", "")

            # If no SSH target known, try to derive from registry info
            if not ssh_target and info.get("address"):
                # Can't auto-restart without SSH credentials — just track
                log.info(f"Node {name} auto-enrolled (no SSH restart configured)")
            else:
                log.info(f"Node {name} auto-enrolled with SSH restart")

            _desired_nodes[name] = DesiredNode(
                node_name=name,
                ssh_target=ssh_target,
                ssh_key=ssh_key,
                restart_cmd=restart_cmd,
            )

            # Persist to PG if available
            if _pg_pool:
                try:
                    await _pg_pool.execute(
                        "INSERT INTO desired_nodes (node_name, enabled, ssh_target, ssh_key, restart_cmd) "
                        "VALUES ($1, true, $2, $3, $4) ON CONFLICT (node_name) DO NOTHING",
                        name, ssh_target, ssh_key, restart_cmd,
                    )
                except Exception as e:
                    log.debug(f"PG persist for {name} failed (non-fatal): {e}")


def add_desired_node(name: str, ssh_target: str = "", ssh_key: str = "",
                     restart_cmd: str = ""):
    """Add a node to the desired run-state (manual or API)."""
    _desired_nodes[name] = DesiredNode(
        node_name=name,
        ssh_target=ssh_target,
        ssh_key=ssh_key,
        restart_cmd=restart_cmd,
    )
    log.info(f"Node {name} added to desired state")


def remove_desired_node(name: str):
    """Remove a node from desired state (explicit stop — not resurrected)."""
    if name in _desired_nodes:
        _desired_nodes[name].enabled = False
        log.info(f"Node {name} removed from desired state (disabled)")


def get_desired_nodes() -> Dict[str, DesiredNode]:
    """Get all desired nodes."""
    return _desired_nodes


def get_enabled_nodes() -> Set[str]:
    """Get names of enabled desired nodes."""
    return {n for n, d in _desired_nodes.items() if d.enabled}


# ── Reconciler ────────────────────────────────────────────────

@dataclass
class ReconcileResult:
    """Result of a reconcile pass."""
    healthy: List[str] = field(default_factory=list)
    missing: List[str] = field(default_factory=list)
    restarted: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)


async def reconcile_desired_state(
    known_nodes: Dict[str, Dict],
    heartbeat_timeout_s: float = 90.0,
    max_restart_attempts: int = 5,
    restart_cooldown_s: float = 120.0,
) -> ReconcileResult:
    """Reconcile desired state with actual state.
    
    Args:
        known_nodes: Dict of node_name → {last_heartbeat, status, ...} from registry
        heartbeat_timeout_s: Max seconds since last heartbeat before node is "missing"
        max_restart_attempts: Max auto-restart attempts before giving up
        restart_cooldown_s: Min seconds between restart attempts for same node
    
    Returns:
        ReconcileResult with healthy/missing/restarted/failed/skipped lists
    """
    # Auto-enroll new nodes from registry
    await auto_enroll_from_registry(known_nodes)

    result = ReconcileResult()
    now = time.time()

    for name, desired in _desired_nodes.items():
        if not desired.enabled:
            result.skipped.append(name)
            continue

        # Check if node is visible and healthy
        node_info = known_nodes.get(name, {})
        last_hb = node_info.get("last_heartbeat", 0)
        status = node_info.get("status", "unknown")

        if last_hb and (now - last_hb) < heartbeat_timeout_s:
            # Node is healthy — reset restart attempts
            if desired.restart_attempts > 0:
                desired.restart_attempts = 0
                log.info(f"Node {name} recovered — reset restart attempts")
            desired.last_seen = last_hb
            result.healthy.append(name)
            continue

        # Node is missing — check restart cooldown
        if desired.restart_attempts >= max_restart_attempts:
            log.warning(f"Node {name} missing but max_restart_attempts "
                       f"({max_restart_attempts}) reached — giving up")
            result.failed.append(name)
            continue

        if desired.last_restart and (now - desired.last_restart) < restart_cooldown_s:
            log.debug(f"Node {name} missing but in restart cooldown "
                     f"({restart_cooldown_s}s since last attempt)")
            result.skipped.append(name)
            continue

        # Can we restart this node?
        if not desired.ssh_target and not desired.restart_cmd:
            # No restart configuration — just track as missing
            result.missing.append(name)
            continue

        # Attempt restart
        log.warning(f"Node {name} missing (last_heartbeat={last_hb}, "
                   f"status={status}) — attempting restart "
                   f"(attempt {desired.restart_attempts + 1}/{max_restart_attempts})")

        success = await _try_restart_node(desired)
        desired.restart_attempts += 1
        desired.last_restart = now

        if success:
            result.restarted.append(name)
            log.info(f"Node {name} restart command sent")
        else:
            result.failed.append(name)
            log.error(f"Node {name} restart failed")

    return result


async def _try_restart_node(node: DesiredNode) -> bool:
    """Try to restart a node via SSH (or locally for Nova)."""
    import subprocess

    if not node.restart_cmd:
        log.info(f"No restart_cmd configured for {node.node_name} — skip (track-only)")
        return False

    if not node.ssh_target:
        # Local restart (Nova)
        try:
            parts = node.restart_cmd.split("&&")
            for part in parts:
                part = part.strip()
                if part:
                    subprocess.run(part, shell=True, timeout=10,
                                 capture_output=True)
            log.info(f"Local restart sent for {node.node_name}")
            return True
        except Exception as e:
            log.error(f"Local restart failed for {node.node_name}: {e}")
            return False

    # SSH restart
    try:
        key_path = os.path.expanduser(node.ssh_key)
        cmd = [
            "ssh", "-i", key_path,
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5",
            node.ssh_target,
            node.restart_cmd,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=15
        )
        if proc.returncode == 0:
            return True
        else:
            log.error(f"SSH restart failed for {node.node_name}: "
                     f"{stderr.decode()[:200]}")
            return False
    except asyncio.TimeoutError:
        log.error(f"SSH restart timeout for {node.node_name}")
        return False
    except Exception as e:
        log.error(f"SSH restart error for {node.node_name}: {e}")
        return False


# ── Status for dashboard ──────────────────────────────────────

def get_status() -> Dict:
    """Get desired state status for dashboard."""
    return {
        "desired_nodes": {
            name: {
                "enabled": d.enabled,
                "last_seen": d.last_seen,
                "restart_attempts": d.restart_attempts,
                "last_restart": d.last_restart,
                "ssh_target": d.ssh_target or "(local)",
                "has_restart": bool(d.restart_cmd),  # restart only possible with a configured command
            }
            for name, d in _desired_nodes.items()
        },
        "enabled_count": sum(1 for d in _desired_nodes.values() if d.enabled),
        "total_count": len(_desired_nodes),
    }