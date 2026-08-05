#!/usr/bin/env python3
"""A2A Mesh Bootstrap CLI — lightweight entry point.

This script does NOT import the full mesh stack (node.py, router.py, etc).
It only needs: PyYAML, asyncpg (for PG discovery), openssl (for TLS).

Usage:
    python3 bootstrap.py --name <node_name> [--pg-host 192.168.1.30]
    python3 bootstrap.py --name lennie --platform windows
"""

import asyncio
import os
import sys

# Add parent to path so we can import core.bootstrap
_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _script_dir)

# Auto-install minimal deps
def _ensure_deps():
    missing = []
    for mod, pip in [('yaml', 'PyYAML>=6.0'), ('asyncpg', 'asyncpg>=0.29.0')]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pip)
    if missing:
        print(f"📦 Installing: {', '.join(missing)}")
        import subprocess
        in_venv = (hasattr(sys, 'real_prefix') or 
                   (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix))
        cmd = [sys.executable, '-m', 'pip', 'install', '--quiet']
        if not in_venv:
            cmd.append('--user')
        cmd += missing
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0 and 'externally-managed' in result.stderr:
            # PEP 668 fallback
            cmd_fb = [sys.executable, '-m', 'pip', 'install', '--quiet', '--break-system-packages'] + missing
            subprocess.run(cmd_fb, timeout=120)

_ensure_deps()

from core.bootstrap import bootstrap, detect_platform, detect_ip_addresses


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        prog="a2a-bootstrap",
        description="A2A Mesh Auto-Bootstrap — configure a new mesh node",
    )
    parser.add_argument("--name", "-n", required=True, help="Node name (e.g. nova, morzsa, lennie)")
    parser.add_argument("--pg-host", default="192.168.1.30", help="PostgreSQL host")
    parser.add_argument("--pg-port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--pg-db", default="agent_memory", help="PostgreSQL database")
    parser.add_argument("--pg-user", default="nova", help="PostgreSQL user")
    parser.add_argument("--pg-password", default="nova_agent_2026", help="PostgreSQL password")
    parser.add_argument("--platform", default="auto", 
                        help="Platform override (macos/linux/docker/ha_addon/windows)")
    parser.add_argument("--config-dir", default="", help="Config output directory")
    parser.add_argument("--script-dir", default="", help="Mesh script directory")
    parser.add_argument("--no-service", action="store_true", help="Skip service installation")
    parser.add_argument("--no-tls", action="store_true", help="Skip TLS cert generation")
    parser.add_argument("--detect-only", action="store_true", help="Only detect platform and IPs, then exit")
    
    args = parser.parse_args()
    
    if args.detect_only:
        plat = detect_platform()
        ips = detect_ip_addresses()
        print(f"Platform: {plat}")
        print(f"LAN IP: {ips['lan']}")
        print(f"Tailscale: {ips['tailscale'] or 'none'}")
        if plat in ('docker', 'ha_addon'):
            from core.bootstrap import get_docker_host_ip
            print(f"Docker host: {get_docker_host_ip()}")
        return
    
    platform_override = args.platform if args.platform != "auto" else ""
    
    result = asyncio.run(bootstrap(
        node_name=args.name,
        pg_host=args.pg_host,
        pg_port=args.pg_port,
        pg_db=args.pg_db,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
        platform_override=platform_override,
        config_dir=args.config_dir,
        script_dir=args.script_dir,
        install_svc=not args.no_service,
        tls_enabled=not args.no_tls,
    ))


if __name__ == "__main__":
    main()