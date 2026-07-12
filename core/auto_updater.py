#!/usr/bin/env python3
"""A2A Mesh Auto-Updater — Safe, atomic self-update with rollback.

Architecture:
  1. Check Gitea release API for latest version
  2. If newer version available → announce update to mesh peers
  3. Drain: wait for in-flight messages to complete (grace period)
  4. Backup current code to backups/ directory
  5. Git pull + checkout new tag
  6. Restart service (systemd / launchctl)
  7. Post-restart health check → rollback if unhealthy

Usage:
  # From CLI:
  python3 cli.py update check
  python3 cli.py update apply [--version v0.13.0]
  python3 cli.py update rollback

  # Programmatic (from node.py health monitor):
  from core.auto_updater import AutoUpdater
  updater = AutoUpdater(node)
  await updater.check_and_update()
"""

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import aiohttp

logger = logging.getLogger("a2a.mesh.updater")

# ─── Constants ───

GITEA_BASE = os.environ.get("A2A_GITEA_URL", "http://192.168.1.100:3001")
GITEA_REPO = os.environ.get("A2A_GITEA_REPO", "nova/a2a-mesh")
GITEA_USER = os.environ.get("A2A_GITEA_USER", "zsolt")
GITEA_PASS = os.environ.get("A2A_GITEA_PASS", "admin1234")

HEALTH_TIMEOUT = 30       # seconds to wait for health check after restart
DRAIN_TIMEOUT = 60        # seconds to wait for in-flight messages
ROLLBACK_RETENTION = 3    # number of backups to keep
RESTART_COOLDOWN = 300    # seconds between auto-updates (5 min)


class UpdateState(Enum):
    IDLE = "idle"
    CHECKING = "checking"
    DOWNLOADING = "downloading"
    BACKING_UP = "backing_up"
    UPDATING = "updating"
    RESTARTING = "restarting"
    VERIFYING = "verifying"
    ROLLING_BACK = "rolling_back"
    DONE = "done"
    FAILED = "failed"


@dataclass
class UpdateResult:
    success: bool
    previous_version: str
    new_version: str = ""
    error: str = ""
    rollback_performed: bool = False
    state: UpdateState = UpdateState.IDLE
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class AutoUpdater:
    """Safe, atomic self-updater for A2A mesh nodes."""

    def __init__(self, node=None, mesh_dir: Optional[str] = None):
        self.node = node
        self.mesh_dir = Path(mesh_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.backup_dir = self.mesh_dir / "backups"
        self.state = UpdateState.IDLE
        self.last_check: Optional[datetime] = None
        self.last_update: Optional[datetime] = None
        self._http_session: Optional[aiohttp.ClientSession] = None

    @property
    def current_version(self) -> str:
        """Read current version from pyproject.toml."""
        toml_path = self.mesh_dir / "pyproject.toml"
        if toml_path.exists():
            for line in toml_path.read_text().splitlines():
                if line.strip().startswith("version"):
                    # version = "0.12.0"
                    return line.split("=")[1].strip().strip('"').strip("'")
        return "0.0.0"

    @property
    def gitea_releases_url(self) -> str:
        return f"{GITEA_BASE}/api/v1/repos/{GITEA_REPO}/releases"

    @property
    def gitea_tags_url(self) -> str:
        return f"{GITEA_BASE}/api/v1/repos/{GITEA_REPO}/tags"

    # ─── HTTP Session Management ───

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                auth=aiohttp.BasicAuth(GITEA_USER, GITEA_PASS),
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._http_session

    async def close(self):
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    # ─── Version Checking ───

    async def get_latest_release(self) -> Optional[dict]:
        """Query Gitea API for the latest release."""
        session = await self._get_session()
        try:
            async with session.get(self.gitea_releases_url) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to fetch releases: HTTP {resp.status}")
                    return None
                releases = await resp.json()
                if not releases:
                    return None
                # Filter out pre-releases, sort by published_at
                stable = [r for r in releases if not r.get("prerelease", False)]
                if not stable:
                    stable = releases
                stable.sort(key=lambda r: r.get("published_at", ""), reverse=True)
                return stable[0]
        except Exception as e:
            logger.error(f"Error fetching releases: {e}")
            return None

    async def get_latest_tag(self) -> Optional[str]:
        """Get the latest git tag from Gitea."""
        session = await self._get_session()
        try:
            async with session.get(f"{self.gitea_tags_url}?limit=20") as resp:
                if resp.status != 200:
                    return None
                tags = await resp.json()
                if not tags:
                    return None
                # Sort by version semantic
                version_tags = []
                for t in tags:
                    name = t.get("name", "")
                    if name.startswith("v"):
                        version_tags.append(name)
                version_tags.sort(key=self._version_key, reverse=True)
                return version_tags[0] if version_tags else None
        except Exception as e:
            logger.error(f"Error fetching tags: {e}")
            return None

    async def check_for_update(self) -> Optional[str]:
        """Check if a newer version is available. Returns tag name or None."""
        self.state = UpdateState.CHECKING
        self.last_check = datetime.now(timezone.utc)

        current = self.current_version
        latest_tag = await self.get_latest_tag()

        if not latest_tag:
            logger.warning("Could not determine latest version")
            self.state = UpdateState.IDLE
            return None

        latest_ver = latest_tag.lstrip("v")
        if self._version_key(latest_ver) > self._version_key(current):
            logger.info(f"Update available: {current} → {latest_ver} ({latest_tag})")
            self.state = UpdateState.IDLE
            return latest_tag

        logger.info(f"Already up to date: {current}")
        self.state = UpdateState.IDLE
        return None

    # ─── Backup ───

    async def _create_backup(self) -> Path:
        """Create a timestamped backup of current code."""
        self.state = UpdateState.BACKING_UP
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        current_ver = self.current_version
        backup_name = f"a2a_mesh_v{current_ver}_{timestamp}"
        backup_path = self.backup_dir / backup_name

        logger.info(f"Creating backup: {backup_path}")

        # Copy everything except .git, __pycache__, .venv, backups, local_store
        shutil.copytree(
            self.mesh_dir,
            backup_path,
            ignore=shutil.ignore_patterns(
                ".git", "__pycache__", ".venv", "backups",
                "local_store*", "mesh.db", "mesh_store.db",
                "*.pyc", ".pytest_cache", "incoming_files"
            ),
        )

        # Save version info for rollback
        version_info = {
            "version": current_ver,
            "commit": self._get_current_commit(),
            "backup_time": timestamp,
            "tag": f"v{current_ver}",
        }
        (backup_path / ".update_info.json").write_text(json.dumps(version_info, indent=2))

        # Clean old backups (keep last N)
        self._cleanup_old_backups()

        logger.info(f"✅ Backup created: {backup_path}")
        return backup_path

    def _cleanup_old_backups(self):
        """Remove old backups, keeping only the last ROLLBACK_RETENTION."""
        if not self.backup_dir.exists():
            return

        backups = sorted(
            [d for d in self.backup_dir.iterdir() if d.is_dir() and d.name.startswith("a2a_mesh_v")],
            key=lambda p: p.name,
        )

        while len(backups) > ROLLBACK_RETENTION:
            old = backups.pop(0)
            logger.info(f"Removing old backup: {old}")
            shutil.rmtree(old, ignore_errors=True)

    # ─── Update Process ───

    async def apply_update(self, target_tag: Optional[str] = None) -> UpdateResult:
        """Full update flow: check → backup → pull → checkout → restart → verify → rollback if needed."""
        current_ver = self.current_version
        result = UpdateResult(success=False, previous_version=current_ver)

        # Cooldown check
        if self.last_update:
            elapsed = (datetime.now(timezone.utc) - self.last_update).total_seconds()
            if elapsed < RESTART_COOLDOWN:
                result.error = f"Cooldown: {RESTART_COOLDOWN - elapsed:.0f}s remaining"
                result.state = UpdateState.IDLE
                return result

        # Step 1: Determine target version
        if not target_tag:
            target_tag = await self.check_for_update()
            if not target_tag:
                result.error = "No update available"
                result.state = UpdateState.IDLE
                return result

        result.new_version = target_tag.lstrip("v")

        # Step 2: Announce update to mesh peers (if node is running)
        if self.node:
            try:
                await self.node.send_message(
                    recipient="broadcast",
                    msg_type="mesh_update",
                    payload={
                        "action": "updating",
                        "from_version": current_ver,
                        "to_version": result.new_version,
                        "node": self.node.config.node_name,
                    },
                )
            except Exception:
                logger.warning("Failed to announce update to peers (continuing)")

        # Step 3: Drain in-flight messages
        self.state = UpdateState.UPDATING
        if self.node:
            logger.info("Draining in-flight messages...")
            drain_start = time.time()
            while time.time() - drain_start < DRAIN_TIMEOUT:
                # Check if there are pending messages
                if hasattr(self.node, 'offline_queue'):
                    stats = await self.node.offline_queue.get_stats() if hasattr(self.node.offline_queue, 'get_stats') else {}
                    if stats.get('pending', 0) == 0:
                        break
                await asyncio.sleep(2)
            logger.info("Drain complete")

        # Step 4: Backup
        try:
            backup_path = await self._create_backup()
        except Exception as e:
            result.error = f"Backup failed: {e}"
            result.state = UpdateState.FAILED
            logger.error(f"❌ Backup failed: {e}")
            return result

        # Step 5: Git pull + checkout
        try:
            self.state = UpdateState.DOWNLOADING
            await self._git_update(target_tag)
        except Exception as e:
            result.error = f"Git update failed: {e}"
            result.state = UpdateState.FAILED
            logger.error(f"❌ Git update failed: {e}")
            # Attempt rollback
            await self._rollback(backup_path)
            result.rollback_performed = True
            return result

        # Step 6: Restart service
        self.state = UpdateState.RESTARTING
        logger.info("🔄 Restarting service...")
        restart_ok = await self._restart_service()

        if not restart_ok:
            logger.error("❌ Restart failed, rolling back")
            await self._rollback(backup_path)
            result.rollback_performed = True
            result.error = "Service restart failed"
            result.state = UpdateState.FAILED
            return result

        # Step 7: Verify health
        self.state = UpdateState.VERIFYING
        logger.info("🔍 Verifying new version health...")
        healthy = await self._verify_health()

        if not healthy:
            logger.error("❌ Health check failed, rolling back")
            await self._rollback(backup_path)
            result.rollback_performed = True
            result.error = "Health check failed after update"
            result.state = UpdateState.FAILED
            return result

        # Success!
        self.state = UpdateState.DONE
        result.success = True
        self.last_update = datetime.now(timezone.utc)
        logger.info(f"✅ Update complete: {current_ver} → {result.new_version}")

        # Announce success to mesh
        if self.node:
            try:
                await self.node.send_message(
                    recipient="broadcast",
                    msg_type="mesh_update",
                    payload={
                        "action": "updated",
                        "from_version": current_ver,
                        "to_version": result.new_version,
                        "node": self.node.config.node_name,
                    },
                )
            except Exception:
                pass

        return result

    # ─── Git Operations ───

    async def _git_update(self, target_tag: str):
        """Pull latest code and checkout target tag."""
        loop = asyncio.get_event_loop()

        # Fetch all tags
        logger.info(f"📥 Fetching latest code...")
        result = await loop.run_in_executor(
            None,
            lambda: subprocess.run(
                ["git", "fetch", "--tags", "--all"],
                cwd=str(self.mesh_dir),
                capture_output=True, text=True, timeout=60,
            ),
        )
        if result.returncode != 0:
            raise RuntimeError(f"git fetch failed: {result.stderr}")

        # Checkout the tag
        logger.info(f"📦 Checking out {target_tag}...")
        result = await loop.run_in_executor(
            None,
            lambda: subprocess.run(
                ["git", "checkout", target_tag],
                cwd=str(self.mesh_dir),
                capture_output=True, text=True, timeout=30,
            ),
        )
        if result.returncode != 0:
            raise RuntimeError(f"git checkout {target_tag} failed: {result.stderr}")

        logger.info(f"✅ Checked out {target_tag}")

    def _get_current_commit(self) -> str:
        """Get current git commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(self.mesh_dir),
                capture_output=True, text=True, timeout=10,
            )
            return result.stdout.strip() if result.returncode == 0 else "unknown"
        except Exception:
            return "unknown"

    # ─── Service Restart ───

    async def _restart_service(self) -> bool:
        """Restart the mesh service based on the platform."""
        import platform

        system = platform.system()
        node_name = self.node.config.node_name if self.node else "unknown"

        try:
            if system == "Darwin":
                # macOS LaunchAgent
                label = "com.hermes.a2a-mesh-node"
                logger.info(f"Restarting LaunchAgent: {label}")
                subprocess.run(["launchctl", "unload", f"~/Library/LaunchAgents/{label}.plist"],
                              capture_output=True, timeout=10)
                await asyncio.sleep(2)
                subprocess.run(["launchctl", "load", f"~/Library/LaunchAgents/{label}.plist"],
                              capture_output=True, timeout=10)
                return True

            elif system == "Linux":
                # Linux systemd
                # Determine service name based on node
                if node_name == "morzsa":
                    service = "a2a-mesh"
                elif node_name == "runa":
                    service = "a2a-mesh-runa"
                else:
                    service = "a2a-mesh"

                logger.info(f"Restarting systemd service: {service}")
                result = subprocess.run(
                    ["systemctl", "--user", "restart", service],
                    capture_output=True, text=True, timeout=15,
                )
                if result.returncode != 0:
                    # Try with sudo
                    result = subprocess.run(
                        ["sudo", "systemctl", "restart", service],
                        capture_output=True, text=True, timeout=15,
                    )
                return result.returncode == 0

            else:
                logger.error(f"Unsupported platform: {system}")
                return False

        except Exception as e:
            logger.error(f"Restart failed: {e}")
            return False

    # ─── Health Verification ───

    async def _verify_health(self) -> bool:
        """Wait for service to come back healthy after restart."""
        import aiohttp

        # Determine health endpoint
        health_url = "http://localhost:8650/health"
        if self.node:
            port = getattr(self.node.config, 'health_port', 8650)
            health_url = f"http://localhost:{port}/health"

        logger.info(f"Waiting for health check at {health_url}...")
        start = time.time()

        while time.time() - start < HEALTH_TIMEOUT:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                    async with session.get(health_url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("status") == "running":
                                logger.info("✅ Health check passed")
                                return True
            except Exception:
                pass
            await asyncio.sleep(3)

        logger.error(f"Health check timed out after {HEALTH_TIMEOUT}s")
        return False

    # ─── Rollback ───

    async def _rollback(self, backup_path: Path) -> bool:
        """Rollback to a previous backup."""
        self.state = UpdateState.ROLLING_BACK
        logger.warning(f"🔄 Rolling back to {backup_path.name}...")

        try:
            # Read version info from backup
            info_path = backup_path / ".update_info.json"
            if info_path.exists():
                info = json.loads(info_path.read_text())
                tag = info.get("tag", "")
            else:
                # Try to find the tag from directory name
                # a2a_mesh_v0.12.0_20260712_150610
                parts = backup_path.name.split("_")
                if len(parts) >= 3:
                    tag = f"v{parts[2]}"
                else:
                    tag = ""

            # Git checkout the previous tag
            if tag:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    lambda: subprocess.run(
                        ["git", "checkout", tag],
                        cwd=str(self.mesh_dir),
                        capture_output=True, text=True, timeout=30,
                    ),
                )
                if result.returncode != 0:
                    logger.error(f"Git checkout rollback failed: {result.stderr}")
                    # Try restoring from backup directory
                    self._restore_from_backup(backup_path)
            else:
                self._restore_from_backup(backup_path)

            # Restart service after rollback
            await self._restart_service()
            logger.info("✅ Rollback complete")
            return True

        except Exception as e:
            logger.error(f"❌ Rollback failed: {e}")
            return False

    def _restore_from_backup(self, backup_path: Path):
        """Restore Python files from a backup directory."""
        logger.info(f"Restoring files from {backup_path.name}...")
        for item in backup_path.iterdir():
            if item.is_file() and item.suffix == ".py":
                dest = self.mesh_dir / item.name
                shutil.copy2(item, dest)
                logger.debug(f"Restored: {item.name}")

        # Also restore core/ and transports/ directories
        for subdir in ["core", "transports", "discovery", "plugins"]:
            src_dir = backup_path / subdir
            dst_dir = self.mesh_dir / subdir
            if src_dir.exists():
                for item in src_dir.iterdir():
                    if item.is_file() and item.suffix == ".py":
                        dst_dir.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(item, dst_dir / item.name)

    # ─── Utility ───

    @staticmethod
    def _version_key(version_str: str) -> tuple:
        """Parse version string into comparable tuple."""
        # Handle both "v0.12.1" and "0.12.1"
        v = version_str.lstrip("v")
        parts = []
        for part in v.split("."):
            try:
                parts.append(int(part))
            except ValueError:
                # Handle pre-release suffixes like "0.12.1rc1"
                subparts = part.split("rc")
                try:
                    parts.append(int(subparts[0]))
                    if len(subparts) > 1:
                        parts.append(-1)  # rc < release
                        parts.append(int(subparts[1]))
                except ValueError:
                    parts.append(0)
        return tuple(parts)

    def get_status(self) -> dict:
        """Return current updater status."""
        return {
            "state": self.state.value,
            "current_version": self.current_version,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "mesh_dir": str(self.mesh_dir),
            "backup_dir": str(self.backup_dir),
        }


# ─── CLI Interface ───

async def cli_check():
    """CLI: check for updates."""
    updater = AutoUpdater()
    latest_tag = await updater.check_for_update()
    if latest_tag:
        print(f"🆕 Update available: {updater.current_version} → {latest_tag.lstrip('v')}")
        print(f"   Run: python3 cli.py update apply")
    else:
        print(f"✅ Already up to date: v{updater.current_version}")
    await updater.close()


async def cli_apply(version: Optional[str] = None):
    """CLI: apply an update."""
    target_tag = f"v{version}" if version and not version.startswith("v") else version
    updater = AutoUpdater()
    result = await updater.apply_update(target_tag)
    if result.success:
        print(f"✅ Update successful: {result.previous_version} → {result.new_version}")
    else:
        print(f"❌ Update failed: {result.error}")
        if result.rollback_performed:
            print(f"   Rollback to {result.previous_version} was performed")
    await updater.close()


async def cli_rollback():
    """CLI: rollback to the most recent backup."""
    updater = AutoUpdater()
    if not updater.backup_dir.exists():
        print("❌ No backups found")
        return

    backups = sorted(
        [d for d in updater.backup_dir.iterdir() if d.is_dir() and d.name.startswith("a2a_mesh_v")],
        key=lambda p: p.name, reverse=True,
    )

    if not backups:
        print("❌ No backups found")
        return

    latest = backups[0]
    print(f"🔄 Rolling back to: {latest.name}")
    result = await updater._rollback(latest)
    print(f"{'✅' if result else '❌'} Rollback {'successful' if result else 'failed'}")


async def cli_status():
    """CLI: show updater status."""
    updater = AutoUpdater()
    status = updater.get_status()
    print(f"Current version: v{status['current_version']}")
    print(f"State: {status['state']}")
    print(f"Last check: {status['last_check'] or 'never'}")
    print(f"Last update: {status['last_update'] or 'never'}")
    print(f"Mesh dir: {status['mesh_dir']}")

    # List backups
    if updater.backup_dir.exists():
        backups = sorted(
            [d for d in updater.backup_dir.iterdir() if d.is_dir() and d.name.startswith("a2a_mesh_v")],
            key=lambda p: p.name, reverse=True,
        )
        print(f"\nBackups ({len(backups)}):")
        for b in backups[:5]:
            info_path = b / ".update_info.json"
            if info_path.exists():
                info = json.loads(info_path.read_text())
                print(f"  {b.name} → v{info.get('version', '?')} ({info.get('backup_time', '?')})")
            else:
                print(f"  {b.name}")
    else:
        print("\nNo backups directory")


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"

    if cmd == "check":
        asyncio.run(cli_check())
    elif cmd == "apply":
        ver = sys.argv[2] if len(sys.argv) > 2 else None
        asyncio.run(cli_apply(ver))
    elif cmd == "rollback":
        asyncio.run(cli_rollback())
    elif cmd == "status":
        asyncio.run(cli_status())
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python3 auto_updater.py [check|apply|rollback|status]")