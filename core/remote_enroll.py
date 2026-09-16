"""
Remote Enrollment — SSH key enrollment + connection bundle for new mesh nodes.

Inspired by Marveen's remote-enroll:
  - Generate/retrieve SSH ed25519 key
  - Add to remote authorized_keys with restricted permitopen
  - Build connection bundle for the new node
  - Validate key format + host address

For A2A Mesh:
  - Enroll new nodes into the mesh via SSH
  - Connection bundle: host, port, key, node config
  - Restricted SSH: only port-forwarding, no shell
"""

import os
import subprocess
import logging

log = logging.getLogger("remote_enroll")

KEY_PATH = os.path.expanduser("~/.ssh/id_ed25519_openclaw")
ACCEPTED_KEY_TYPE = "ssh-ed25519"


def get_public_key():
    """Get or generate the mesh SSH public key."""
    pub_path = KEY_PATH + ".pub"
    if not os.path.exists(pub_path):
        log.info("Remote enroll: generating new ed25519 key")
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-f", KEY_PATH, "-N", "", "-C", "a2a-mesh"],
            check=True, capture_output=True
        )
    with open(pub_path, "r") as f:
        return f.read().strip()


def validate_public_key(key_str):
    """Validate that a public key is ed25519 format."""
    if not key_str:
        return False, "Empty key"
    parts = key_str.strip().split()
    if len(parts) < 2:
        return False, "Invalid format"
    key_type = parts[0]
    if key_type != ACCEPTED_KEY_TYPE:
        return False, f"Wrong key type: {key_type} (expected {ACCEPTED_KEY_TYPE})"
    return True, "OK"


def build_authorized_keys_line(pub_key, permitopen_port=8650):
    """Build a restricted authorized_keys line for the mesh.
    Only allows port forwarding to the dashboard, no shell access."""
    restrictions = (
        f'no-pty,no-X11-forwarding,no-user-rc,'
        f'permitlisten=127.0.0.1:{permitopen_port},'
        f'command="echo a2a-mesh-restricted"'
    )
    return f'{restrictions} {pub_key}'


def build_connection_bundle(host, port=22, node_name="", dashboard_port=8650):
    """Build a connection bundle for a new node to join the mesh."""
    pub_key = get_public_key()
    return {
        "host": host,
        "ssh_port": port,
        "node_name": node_name,
        "dashboard_port": dashboard_port,
        "ssh_key_path": KEY_PATH,
        "public_key": pub_key,
        "restrictions": build_authorized_keys_line(pub_key, dashboard_port),
        "connection_command": f"ssh -i {KEY_PATH} -L {dashboard_port}:127.0.0.1:{dashboard_port} -p {port} user@{host}",
    }


def validate_host_address(host):
    """Validate that a host address is a valid IP or hostname (not email)."""
    if not host:
        return False, "Empty host"
    # Must not contain @ (email)
    if "@" in host:
        return False, "Host looks like an email address"
    # Basic IP or hostname check
    parts = host.split(".")
    if len(parts) == 4:
        try:
            for p in parts:
                if not (0 <= int(p) <= 255):
                    return False, "Invalid IP"
            return True, "IP"
        except ValueError:
            pass
    # Hostname
    if all(c.isalnum() or c in "-." for c in host) and len(host) <= 253:
        return True, "hostname"
    return False, "Invalid host format"


def get_enrollment_status():
    """Get current enrollment status for dashboard display."""
    pub_path = KEY_PATH + ".pub"
    has_key = os.path.exists(pub_path)
    pub_key = ""
    if has_key:
        with open(pub_path, "r") as f:
            pub_key = f.read().strip()
    return {
        "key_exists": has_key,
        "key_path": KEY_PATH,
        "public_key": pub_key[:50] + "..." if pub_key else "",
        "key_type": ACCEPTED_KEY_TYPE,
        "ready": has_key,
    }