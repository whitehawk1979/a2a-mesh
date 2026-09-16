"""Embedded SSH server (sshd) manager for A2A Mesh nodes.

Every mesh node needs a reachable sshd for INBOUND tunnels (peers dial in
to build -L forwards). On bare-metal/VM hosts the system sshd usually
exists; inside containers (Docker, HAOS add-ons) it does NOT — this module
guarantees one is installed, configured, and kept alive:

  - platform detection: macos / bare Linux / docker container / HAOS add-on
  - installs openssh-server if missing (apt/apk/brew, best-effort)
  - persistent host keys + authorized_keys under a config dir (survives
    container restarts where /root/.ssh is wiped by gateway sandboxes)
  - self-heal loop: if sshd dies, restart it (container restarts kill it)
  - the advertised port is announced via ssh_key_sync v2 (bundle), peers
    dial it via forward_host when the container shares the host network

Config (mesh.transports.ssh_tunnel):
  embedded_sshd: true          # enable manager (auto-on in containers)
  sshd_port: 2230              # port our sshd listens on (default 2230)
  sshd_bind: 0.0.0.0           # bind address
  sshd_config_dir: /config/.ssh # persistent dir for keys/authorized_keys
"""

import asyncio
import logging
import os
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger("a2a_mesh.ssh_server")

_DEFAULT_PORT = 2230


def detect_environment() -> str:
    """Return one of: macos, linux, docker, haos."""
    uname = os.uname().sysname if hasattr(os, "uname") else ""
    if uname == "Darwin" or shutil.which("sw_vers"):
        return "macos"
    # HAOS add-on convention: /config holds HA configuration
    if Path("/config/a2a_mesh").exists() or (
        Path("/config").exists() and Path("/config/configuration.yaml").is_file()
    ):
        return "haos"
    if Path("/.dockerenv").exists() or Path("/run/.containerenv").exists():
        return "docker"
    return "linux"


class EmbeddedSSHD:
    """Install, configure, and keep-alive a dedicated sshd for the mesh."""

    def __init__(self, node_name: str, config=None):
        self._node = node_name
        self._cfg = config or {}
        self._port = int(self._cfg.get("sshd_port", _DEFAULT_PORT) or _DEFAULT_PORT)
        self._bind = str(self._cfg.get("sshd_bind", "0.0.0.0") or "0.0.0.0")
        d = self._cfg.get("sshd_config_dir", "") or ""
        self._env = detect_environment()
        if not d:
            d = (
                "/config/.ssh"
                if self._env in ("haos", "docker")
                else str(Path.home() / ".ssh")
            )
        self._dir = Path(d).expanduser()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._sshd_binary = None
        self._proc = None  # our managed sshd subprocess
        self._authorized_keys = self._dir / "authorized_keys"
        self._host_key = self._dir / "ssh_host_ed25519_key"
        self._running = False

    # ── install ──────────────────────────────────────────────────
    def _find_sshd(self) -> Optional[str]:
        for cand in (
            shutil.which("sshd"),
            "/usr/sbin/sshd",
            "/usr/lib/ssh/sshd",
            "/config/.linuxbrew/sbin/sshd",
            "/config/.linuxbrew/bin/sshd",
        ):
            if cand and Path(cand).exists():
                return cand
        return None

    def ensure_installed(self) -> bool:
        """Make sure sshd binary exists; install openssh if missing."""
        self._sshd_binary = self._find_sshd()
        if self._sshd_binary:
            return True
        try:
            if shutil.which("apt-get"):
                subprocess.run(
                    ["apt-get", "update", "-qq"], timeout=120, capture_output=True
                )
                subprocess.run(
                    ["apt-get", "install", "-y", "-qq", "openssh-server"],
                    timeout=300,
                    capture_output=True,
                )
            elif shutil.which("apk"):
                subprocess.run(
                    ["apk", "add", "--no-cache", "openssh"],
                    timeout=180, capture_output=True,
                )
            elif shutil.which("brew"):
                subprocess.run(
                    ["brew", "install", "openssh"], timeout=300, capture_output=True
                )
        except Exception as e:
            log.warning(f"[{self._node}] openssh install failed: {e}")
        self._sshd_binary = self._find_sshd()
        return self._sshd_binary is not None

    # ── configure & start ────────────────────────────────────────
    def _gen_host_key(self):
        if not self._host_key.exists():
            try:
                subprocess.run(
                    ["ssh-keygen", "-t", "ed25519", "-N", "", "-f", str(self._host_key)],
                    capture_output=True, timeout=30,
                )
                log.info(f"[{self._node}] Generated sshd host key {self._host_key}")
            except Exception as e:
                log.warning(f"[{self._node}] Host key generation failed: {e}")

    def _write_sshd_config(self) -> str:
        cfg_path = self._dir / "sshd_config"
        cfg = f"""# A2A Mesh embedded sshd — managed by ssh_server.py
Port {self._port}
ListenAddress {self._bind}
HostKey {self._host_key}
PermitRootLogin prohibit-password
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
AuthorizedKeysFile {self._authorized_keys}
AllowTcpForwarding yes
PermitOpen any
PermitListen any
GatewayPorts no
UsePAM no
StrictModes no
Subsystem sftp internal-sftp
LogLevel INFO
"""
        cfg_path.write_text(cfg)
        return str(cfg_path)

    def _port_open(self) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.5)
                s.connect(("127.0.0.1", self._port))
                return True
        except Exception:
            return False

    def start(self) -> bool:
        """Ensure sshd is up. Returns True if listening on our port."""
        self.ensure_installed()
        self._gen_host_key()
        if not self._sshd_binary:
            log.error(f"[{self._node}] No sshd binary available — inbound tunnels disabled")
            return False
        if self._port_open():
            self._running = True
            log.info(f"[{self._node}] sshd already listening on :{self._port}")
            return True
        cfg = self._write_sshd_config()
        # Debian sshd requires the privilege-separation directory
        try:
            Path("/run/sshd").mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        try:
            proc = subprocess.Popen(
                [self._sshd_binary, "-D", "-e", "-f", cfg],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            self._proc = proc
            for _ in range(20):
                if self._port_open():
                    self._running = True
                    log.info(
                        f"[{self._node}] Embedded sshd started on "
                        f"{self._bind}:{self._port} (pid {proc.pid})"
                    )
                    return True
                if proc.poll() is not None:
                    log.error(f"[{self._node}] sshd exited rc={proc.returncode}")
                    break
                time.sleep(0.5)
            return False
        except Exception as e:
            log.error(f"[{self._node}] sshd start failed: {e}")
            return False

    def stop(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
        self._running = False

    @property
    def port(self) -> int:
        return self._port

    @property
    def running(self) -> bool:
        return self._running or self._port_open()

    # ── self-heal ────────────────────────────────────────────────
    async def self_heal_loop(self, interval: int = 60):
        """Keep sshd alive: container restarts / OOM kills are healed."""
        while True:
            try:
                if not self._port_open():
                    log.warning(f"[{self._node}] sshd down — self-heal restart")
                    self.start()
            except Exception as e:
                log.warning(f"[{self._node}] sshd self-heal error: {e}")
            await asyncio.sleep(interval)

    def add_authorized_key(self, key_line: str) -> bool:
        """Dedup-append a public key to authorized_keys."""
        key_line = key_line.strip()
        if not re.match(r"^(ssh|ecdsa)-[A-Za-z0-9+/]+ ?.*", key_line):
            return False
        self._dir.mkdir(parents=True, exist_ok=True)
        try:
            existing = (
                self._authorized_keys.read_text()
                if self._authorized_keys.exists()
                else ""
            )
        except Exception:
            existing = ""
        if key_line in existing:
            return False
        with open(self._authorized_keys, "a") as f:
            f.write(key_line + "\n")
        os.chmod(self._authorized_keys, 0o600)
        return True


# module-level singleton handle (node.py wires lifecycle)
_INSTANCE: Optional[EmbeddedSSHD] = None


def get_embedded_sshd() -> Optional[EmbeddedSSHD]:
    return _INSTANCE


def start_embedded_sshd(node_name: str, config: dict) -> Optional[EmbeddedSSHD]:
    """Start (or attach to) the node's dedicated sshd. None if impossible."""
    global _INSTANCE
    try:
        inst = EmbeddedSSHD(node_name, config)
        if inst.start():
            _INSTANCE = inst
            return inst
    except Exception as e:
        log.error(f"embedded sshd bootstrap failed: {e}")
    return None