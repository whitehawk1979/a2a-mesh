"""
Network Info — LAN IP detection for A2A Mesh.

Inspired by Marveen's network-info:
  - Detect best LAN IPv4 for mobile/remote access
  - Skip loopback, VPN, docker, virtualization interfaces
  - QR code generation for mobile login

For A2A Mesh:
  - Detect Tailscale IP + LAN IP
  - Show all accessible addresses
  - QR code for phone dashboard access
"""

import socket
import logging
import subprocess

log = logging.getLogger("network_info")


def get_lan_ip():
    """Get the best LAN IPv4 address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def get_tailscale_ip():
    """Get Tailscale IP."""
    try:
        result = subprocess.run(
            ["tailscale", "ip", "-4"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            return lines[0] if lines else None
    except Exception:
        pass
    return None


def get_all_interfaces():
    """Get all network interfaces."""
    interfaces = []
    try:
        result = subprocess.run(
            ["ifconfig"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            current_name = None
            for line in result.stdout.split("\n"):
                if line and not line.startswith("\t") and not line.startswith(" "):
                    current_name = line.split(":")[0].strip()
                elif "inet " in line and current_name:
                    ip = line.strip().split("inet ")[1].split(" ")[0]
                    if not ip.startswith("127."):
                        interfaces.append({"name": current_name, "ip": ip})
    except Exception:
        pass
    return interfaces


def get_network_status():
    """Get network status for dashboard."""
    lan_ip = get_lan_ip()
    tailscale_ip = get_tailscale_ip()
    interfaces = get_all_interfaces()
    
    return {
        "lan_ip": lan_ip,
        "tailscale_ip": tailscale_ip,
        "dashboard_urls": [
            f"http://{lan_ip}:8650",
            f"http://{tailscale_ip}:8650" if tailscale_ip else None,
            "http://localhost:8650",
        ],
        "interfaces": interfaces,
        "hostname": socket.gethostname(),
    }