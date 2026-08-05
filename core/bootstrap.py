"""
A2A Mesh Auto-Bootstrap — Platform detection, config generation, service install.

Usage:
    python cli.py bootstrap --name <node_name> [--pg-host 192.168.1.30] [--platform auto]

Supports: macOS, Linux, Docker, HA-addon, Windows.
"""

import asyncio
import ipaddress
import json
import os
import platform
import socket
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple, List, Dict

try:
    import yaml
except ImportError:
    yaml = None


# ─── Platform Detection ─────────────────────────────────────────────────────

def detect_platform() -> str:
    """Detect the platform for auto-configuration.
    
    Returns one of: 'macos', 'linux', 'docker', 'ha_addon', 'windows', 'unknown'.
    """
    # Docker container — check before everything else
    if os.path.exists('/.dockerenv'):
        return 'docker'
    
    # HA addon — Home Assistant Supervisor container
    if os.path.exists('/data/options.json') or os.environ.get('HASS_URL'):
        return 'ha_addon'
    
    system = platform.system().lower()
    if system == 'darwin':
        return 'macos'
    elif system == 'linux':
        return 'linux'
    elif system == 'windows':
        return 'windows'
    return 'unknown'


def detect_ip_addresses() -> Dict[str, str]:
    """Detect LAN and Tailscale IPs.
    
    Returns {'lan': '192.168.x.x', 'tailscale': '100.x.x.x' or ''}
    """
    result = {'lan': '', 'tailscale': ''}
    
    # LAN IP — connect a UDP socket to detect primary interface
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        result['lan'] = s.getsockname()[0]
        s.close()
    except Exception:
        result['lan'] = '127.0.0.1'
    
    # Tailscale IP — check `tailscale ip -4` or hostname
    try:
        r = subprocess.run(['tailscale', 'ip', '-4'], capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            ts_ip = r.stdout.strip().split('\n')[0].strip()
            if ts_ip.startswith('100.'):
                result['tailscale'] = ts_ip
    except Exception:
        pass
    
    # Fallback: check all interfaces for 100.x.x.x
    if not result['tailscale']:
        try:
            hostname = socket.gethostname()
            ips = socket.getaddrinfo(hostname, None, socket.AF_INET)
            for addr in ips:
                ip = addr[4][0]
                if ip.startswith('100.'):
                    result['tailscale'] = ip
                    break
        except Exception:
            pass
    
    return result


def is_docker_container() -> bool:
    """Check if we're running inside a Docker container."""
    return os.path.exists('/.dockerenv')


def get_docker_host_ip() -> str:
    """Get the host IP when running inside Docker.
    
    Methods (in order):
    1. Check `host.docker.internal` (Docker Desktop)
    2. Check default gateway
    3. Check environment variable A2A_DOCKER_HOST_IP
    """
    # Environment override
    env_ip = os.environ.get('A2A_DOCKER_HOST_IP', '')
    if env_ip:
        return env_ip
    
    # Docker Desktop — host.docker.internal
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('host.docker.internal', 80))
        ip = s.getsockname()[0]
        s.close()
        # Actually we want the host IP, not our IP
        # host.docker.internal resolves to the host
        import subprocess
        r = subprocess.run(['getent', 'hosts', 'host.docker.internal'], 
                          capture_output=True, text=True, timeout=3)
        if r.returncode == 0 and r.stdout:
            return r.stdout.split()[0]
    except Exception:
        pass
    
    # Default gateway = host in Docker bridge network
    try:
        r = subprocess.run(['ip', 'route'], capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            if 'default via' in line:
                return line.split()[2]
    except Exception:
        pass
    
    # Fallback: read /proc/net/route
    try:
        with open('/proc/net/route') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3 and parts[1] == '00000000':
                    # Default gateway in hex, little-endian
                    gw_hex = parts[2]
                    gw = socket.inet_ntoa(bytes.fromhex(gw_hex)[::-1])
                    return gw
    except Exception:
        pass
    
    return '127.0.0.1'


# ─── Port Selection ─────────────────────────────────────────────────────────

def find_free_port(start: int = 8645, max_tries: int = 20) -> int:
    """Find a free port starting from `start`."""
    for port in range(start, start + max_tries):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('0.0.0.0', port))
            s.close()
            return port
        except OSError:
            continue
    return start  # fallback


# ─── PG Discovery ───────────────────────────────────────────────────────────

async def discover_pg_nodes(pg_host: str, pg_port: int = 5432, 
                            pg_db: str = 'agent_memory', pg_user: str = 'nova',
                            pg_password: str = '') -> List[Dict]:
    """Query mesh_nodes table for existing nodes.
    
    Returns list of {'name', 'host', 'p2p_port', 'health_port', 'role', 'status'}.
    """
    try:
        import asyncpg
        conn = await asyncpg.connect(
            host=pg_host, port=pg_port, database=pg_db,
            user=pg_user, password=pg_password, timeout=10
        )
        rows = await conn.fetch("""
            SELECT node_name, host, p2p_port, health_port, role, status,
                   capabilities
            FROM mesh.mesh_nodes 
            WHERE status = 'active'
            ORDER BY node_name
        """)
        await conn.close()
        
        nodes = []
        for r in rows:
            nodes.append({
                'name': r['node_name'],
                'host': r['host'],
                'p2p_port': r['p2p_port'] or 8645,
                'health_port': r['health_port'] or 8650,
                'role': r['role'],
                'status': r['status'],
            })
        return nodes
    except Exception as e:
        print(f"⚠️  PG discovery failed: {e}")
        return []


async def check_coordinator(pg_host: str, pg_port: int = 5432,
                            pg_db: str = 'agent_memory', pg_user: str = 'nova',
                            pg_password: str = '') -> Optional[str]:
    """Check if there's an active coordinator in the mesh."""
    try:
        import asyncpg
        conn = await asyncpg.connect(
            host=pg_host, port=pg_port, database=pg_db,
            user=pg_user, password=pg_password, timeout=10
        )
        row = await conn.fetchrow("""
            SELECT node_name FROM mesh.mesh_nodes 
            WHERE role = 'coordinator' AND status = 'active'
            ORDER BY last_heartbeat DESC LIMIT 1
        """)
        await conn.close()
        return row['node_name'] if row else None
    except Exception:
        return None


# ─── Config Generation ──────────────────────────────────────────────────────

def generate_config(
    node_name: str,
    platform_name: str,
    lan_ip: str,
    tailscale_ip: str,
    p2p_port: int,
    health_port: int,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    pg_user: str,
    pg_password: str,
    discovered_nodes: List[Dict],
    advertise_host: str = '',
    tls_enabled: bool = True,
    tls_cert: str = '',
    tls_key: str = '',
    tls_ca: str = '',
    capabilities: List[str] = None,
) -> dict:
    """Generate a minimal mesh config dictionary."""
    
    if capabilities is None:
        capabilities = [
            'a2a_messaging', 'file_transfer', 'p2p_transport',
            'pg_transport', 'health_monitor', 'registry', 'dashboard',
        ]
    
    # Static peers from PG discovery
    static_nodes = []
    for node in discovered_nodes:
        if node['name'] == node_name:
            continue  # Don't add self
        static_nodes.append({
            'name': node['name'],
            'ip': node['host'],
            'p2p_port': node['p2p_port'],
            'health_port': node['health_port'],
            'transport': 'p2p',
        })
    
    # P2P config
    p2p_config = {
        'enabled': True,
        'listen_host': '0.0.0.0',
        'listen_port': p2p_port,
        'max_connections': 50,
        'idle_timeout': 120,
    }
    
    # Docker/HA: advertise_host to report the host IP instead of container IP
    if advertise_host:
        p2p_config['advertise_host'] = advertise_host
        p2p_config['advertise_port'] = p2p_port
    
    # TLS config
    if tls_enabled:
        p2p_config['tls_enabled'] = True
        p2p_config['tls_cert'] = tls_cert
        p2p_config['tls_key'] = tls_key
        p2p_config['tls_ca'] = tls_ca
        p2p_config['tls_verify_peer'] = True
    else:
        p2p_config['tls_enabled'] = False
    
    # Webhook port — auto-select
    webhook_port = find_free_port(8765, 10)
    
    config = {
        'mesh': {
            'node_name': node_name,
            'node_id': '',
            'transport_priority': ['p2p', 'pg_notify', 'http'],
            'capabilities': capabilities,
            'skills': [
                {'id': 'mesh_send', 'name': 'Send Message',
                 'description': 'Send a message to another agent or broadcast to all',
                 'tags': ['messaging', 'send']},
                {'id': 'mesh_discover', 'name': 'Discover Agents',
                 'description': 'List all agents in the mesh and their capabilities',
                 'tags': ['discovery', 'agents']},
                {'id': 'mesh_health', 'name': 'Health Check',
                 'description': 'Get health status and metrics of this agent',
                 'tags': ['health', 'monitoring']},
                {'id': 'task_execution', 'name': 'Task Execution',
                 'description': 'Execute delegated tasks and report results back to the coordinator',
                 'tags': ['task', 'execution', 'delegation']},
            ],
            'webhook_port': webhook_port,
            'health_port': health_port,
            'auth_mode': 'open',
            'wake_agent_on_message': True,
            'transports': {
                'pg_notify': {
                    'host': pg_host,
                    'port': pg_port,
                    'dbname': pg_db,
                    'user': pg_user,
                    'password': pg_password,
                    'channels': ['a2a_channel', 'a2a_steer_channel', 
                                'delegation_channel', 'mesh_channel', 'diagnostic_channel'],
                },
                'p2p': p2p_config,
                'http': {
                    'url': f'http://{lan_ip}:{health_port}',
                    'health_url': f'http://{lan_ip}:{health_port}/health',
                    'timeout': 5,
                    'retries': 3,
                },
            },
            'discovery': {
                'min_target_peers': 2,
                'mdns': {
                    'enabled': platform_name != 'docker',  # mDNS not useful in Docker
                    'service': '_a2a._tcp',
                    'port': p2p_port,
                },
                'static': {
                    'nodes': static_nodes,
                },
            },
        },
    }
    
    # Platform-specific additions
    if platform_name in ('docker', 'ha_addon'):
        config['mesh']['platform'] = platform_name
        config['mesh']['docker'] = {
            'host_ip': advertise_host or lan_ip,
            'auto_forward': True,  # Auto socat/iptables forward
        }
    
    if platform_name == 'windows':
        config['mesh']['log_file'] = f'C:\\a2a_mesh\\logs\\{node_name}.log'
    else:
        config['mesh']['log_file'] = f'~/a2a_mesh/logs/{node_name}.log'
    
    return config


def write_config(config: dict, config_path: str) -> str:
    """Write config to YAML file. Returns the absolute path."""
    if yaml is None:
        raise RuntimeError("PyYAML not installed")
    
    p = Path(config_path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    
    with open(p, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    return str(p)


# ─── TLS Certificate Generation ─────────────────────────────────────────────

def generate_tls_cert(node_name: str, certs_dir: str) -> Tuple[str, str]:
    """Generate self-signed TLS cert for the node.
    
    Returns (cert_path, key_path).
    """
    certs = Path(certs_dir).expanduser()
    certs.mkdir(parents=True, exist_ok=True)
    
    cert_path = certs / f'{node_name}.crt'
    key_path = certs / f'{node_name}.key'
    
    # Check if openssl is available
    try:
        subprocess.run(['openssl', 'version'], capture_output=True, timeout=5)
    except Exception:
        print("⚠️  OpenSSL not found — skipping TLS cert generation")
        return '', ''
    
    # Generate private key
    subprocess.run([
        'openssl', 'genrsa', '-out', str(key_path), '2048'
    ], capture_output=True, timeout=30)
    
    # Generate self-signed certificate
    subprocess.run([
        'openssl', 'req', '-new', '-x509', '-key', str(key_path),
        '-out', str(cert_path), '-days', '365',
        '-subj', f'/CN={node_name}/O=A2A-Mesh',
    ], capture_output=True, timeout=30)
    
    # Set permissions
    try:
        os.chmod(str(key_path), 0o600)
    except Exception:
        pass
    
    return str(cert_path), str(key_path)


def get_or_create_ca(certs_dir: str) -> str:
    """Get existing CA cert or create a new one. Returns CA cert path."""
    certs = Path(certs_dir).expanduser()
    ca_cert = certs / 'a2a-mesh-ca.crt'
    ca_key = certs / 'a2a-mesh-ca.key'
    
    if ca_cert.exists():
        return str(ca_cert)
    
    certs.mkdir(parents=True, exist_ok=True)
    
    # Generate CA private key
    subprocess.run([
        'openssl', 'genrsa', '-out', str(ca_key), '2048'
    ], capture_output=True, timeout=30)
    
    # Generate CA certificate
    subprocess.run([
        'openssl', 'req', '-new', '-x509', '-key', str(ca_key),
        '-out', str(ca_cert), '-days', '3650',
        '-subj', '/CN=A2A-Mesh-CA/O=A2A-Mesh',
    ], capture_output=True, timeout=30)
    
    return str(ca_cert)


# ─── Service Installation ───────────────────────────────────────────────────

def install_service_macos(node_name: str, config_path: str, 
                           script_dir: str) -> str:
    """Install launchd service for macOS. Returns plist path."""
    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.hermes.a2a-mesh-node.{node_name}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{sys.executable}</string>
        <string>{script_dir}/cli.py</string>
        <string>start</string>
        <string>--name</string>
        <string>{node_name}</string>
        <string>--config</string>
        <string>{config_path}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{script_dir}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/a2a-mesh-{node_name}.out.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/a2a-mesh-{node_name}.err.log</string>
</dict>
</plist>
"""
    plist_path = f'{Path.home()}/Library/LaunchAgents/com.hermes.a2a-mesh-node.{node_name}.plist'
    Path(plist_path).parent.mkdir(parents=True, exist_ok=True)
    with open(plist_path, 'w') as f:
        f.write(plist_content)
    
    # Load the service
    subprocess.run(['launchctl', 'load', plist_path], capture_output=True)
    
    return plist_path


def install_service_linux(node_name: str, config_path: str,
                           script_dir: str) -> str:
    """Install systemd user service for Linux. Returns service file path."""
    service_content = f"""[Unit]
Description=A2A Mesh Node ({node_name})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={sys.executable} {script_dir}/cli.py start --name {node_name} --config {config_path}
WorkingDirectory={script_dir}
Restart=always
RestartSec=10
Environment=PYTHONPATH={script_dir}

[Install]
WantedBy=default.target
"""
    service_path = f'{Path.home()}/.config/systemd/user/a2a-mesh-{node_name}.service'
    Path(service_path).parent.mkdir(parents=True, exist_ok=True)
    with open(service_path, 'w') as f:
        f.write(service_content)
    
    # Reload and enable
    subprocess.run(['systemctl', '--user', 'daemon-reload'], capture_output=True)
    subprocess.run(['systemctl', '--user', 'enable', f'a2a-mesh-{node_name}'], capture_output=True)
    
    return service_path


def install_service_windows(node_name: str, config_path: str,
                             script_dir: str) -> str:
    """Install Windows Task Scheduler service. Returns script path."""
    
    # Create a batch file to start the node
    bat_content = f"""@echo off
cd /d {script_dir}
{sys.executable} cli.py start --name {node_name} --config {config_path}
"""
    bat_path = f'C:\\a2a_mesh\\start_{node_name}.bat'
    Path(bat_path).parent.mkdir(parents=True, exist_ok=True)
    with open(bat_path, 'w') as f:
        f.write(bat_content)
    
    # Create a VBS wrapper for silent startup (no console window)
    vbs_content = f"""Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /c {bat_path}", 0, False
"""
    vbs_path = f'C:\\a2a_mesh\\start_{node_name}.vbs'
    with open(vbs_path, 'w') as f:
        f.write(vbs_content)
    
    # Register with Task Scheduler — run at logon, restart on failure
    subprocess.run([
        'schtasks', '/Create', '/TN', f'A2A-Mesh-{node_name}',
        '/TR', f'wscript.exe "{vbs_path}"',
        '/SC', 'ONLOGON',
        '/RL', 'HIGHEST',
        '/F',  # Force overwrite
    ], capture_output=True)
    
    # Also create a PowerShell-based service wrapper for background mode
    ps1_content = f"""# A2A Mesh Node Service — {node_name}
# Run as: powershell -ExecutionPolicy Bypass -File this_script.ps1
while ($true) {{
    try {{
        Push-Location "{script_dir}"
        & "{sys.executable}" cli.py start --name "{node_name}" --config "{config_path}"
        Pop-Location
    }} catch {{
        Write-Error $_
    }}
    Write-Host "Process exited, restarting in 10s..."
    Start-Sleep -Seconds 10
}}
"""
    ps1_path = f'C:\\a2a_mesh\\service_{node_name}.ps1'
    with open(ps1_path, 'w') as f:
        f.write(ps1_content)
    
    return bat_path


def install_service(node_name: str, config_path: str, script_dir: str,
                    platform_name: str) -> Optional[str]:
    """Install platform-appropriate auto-start service.
    
    Returns the service file path or None if not supported.
    """
    if platform_name == 'macos':
        return install_service_macos(node_name, config_path, script_dir)
    elif platform_name == 'linux':
        return install_service_linux(node_name, config_path, script_dir)
    elif platform_name == 'windows':
        return install_service_windows(node_name, config_path, script_dir)
    elif platform_name in ('docker', 'ha_addon'):
        # Docker: container restart policy handles this
        # HA addon: supervisor handles restart
        print(f"ℹ️  Platform '{platform_name}' — service managed by container/supervisor")
        return None
    else:
        print(f"⚠️  Platform '{platform_name}' — auto service install not supported")
        return None


# ─── Venv Setup ─────────────────────────────────────────────────────────────

def setup_venv(script_dir: str) -> str:
    """Create a Python virtual environment if not exists. Returns venv python path."""
    venv_dir = os.path.join(script_dir, '.venv')
    
    if os.path.exists(venv_dir):
        # Already exists — return the python path
        if sys.platform == 'win32':
            return os.path.join(venv_dir, 'Scripts', 'python.exe')
        return os.path.join(venv_dir, 'bin', 'python3')
    
    print("📦 Creating virtual environment...")
    result = subprocess.run([sys.executable, '-m', 'venv', venv_dir],
                          capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        print(f"⚠️  venv creation failed: {result.stderr}")
        return sys.executable
    
    # Determine venv python
    if sys.platform == 'win32':
        venv_python = os.path.join(venv_dir, 'Scripts', 'python.exe')
    else:
        venv_python = os.path.join(venv_dir, 'bin', 'python3')
    
    # Install dependencies
    print("📦 Installing dependencies...")
    deps = ['aiohttp>=3.9.0', 'msgpack>=1.0.7', 'psycopg2-binary>=2.9.9',
            'PyYAML>=6.0', 'zeroconf>=0.130.0', 'asyncpg>=0.29.0',
            'cryptography>=42.0.0']
    
    subprocess.run([venv_python, '-m', 'pip', 'install', '--quiet'] + deps,
                   capture_output=True, text=True, timeout=300)
    
    return venv_python


# ─── Main Bootstrap Function ────────────────────────────────────────────────

async def bootstrap(
    node_name: str,
    pg_host: str = '192.168.1.30',
    pg_port: int = 5432,
    pg_db: str = 'agent_memory',
    pg_user: str = 'nova',
    pg_password: str = 'nova_agent_2026',
    platform_override: str = '',
    config_dir: str = '',
    script_dir: str = '',
    install_svc: bool = True,
    tls_enabled: bool = True,
) -> dict:
    """Run the full bootstrap sequence.
    
    Returns a summary dict with all generated paths and detected info.
    """
    print(f"\n🚀 A2A Mesh Bootstrap — Node: {node_name}")
    print("=" * 60)
    
    # 1. Platform detection
    platform_name = platform_override or detect_platform()
    print(f"📋 Platform: {platform_name}")
    
    # 2. IP detection
    ips = detect_ip_addresses()
    lan_ip = ips['lan']
    tailscale_ip = ips['tailscale']
    print(f"🌐 LAN IP: {lan_ip}")
    if tailscale_ip:
        print(f"🔵 Tailscale IP: {tailscale_ip}")
    
    # Docker: get host IP for advertise_host
    advertise_host = ''
    if platform_name in ('docker', 'ha_addon'):
        advertise_host = get_docker_host_ip()
        print(f"🐳 Docker host IP: {advertise_host}")
    
    # 3. Port selection
    p2p_port = find_free_port(8645)
    health_port = find_free_port(8650)
    print(f"🔌 P2P port: {p2p_port}, Health port: {health_port}")
    
    # 4. Determine paths
    if not script_dir:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # If we're in core/, go up one level
        if os.path.basename(script_dir) == 'core':
            script_dir = os.path.dirname(script_dir)
    
    if not config_dir:
        if platform_name == 'windows':
            config_dir = f'C:\\a2a_mesh'
        else:
            config_dir = os.path.join(script_dir)
    
    config_path = os.path.join(config_dir, f'mesh_config_{node_name}.yaml')
    certs_dir = os.path.join(script_dir, 'certs')
    
    # 5. PG Discovery — find existing nodes
    print(f"\n🔍 Discovering existing mesh nodes from PG ({pg_host})...")
    discovered_nodes = await discover_pg_nodes(
        pg_host, pg_port, pg_db, pg_user, pg_password
    )
    if discovered_nodes:
        print(f"   Found {len(discovered_nodes)} active nodes:")
        for n in discovered_nodes:
            print(f"   • {n['name']} @ {n['host']}:{n['p2p_port']} ({n['role']})")
    else:
        print("   No existing nodes found — this will be the first node")
    
    # 6. Coordinator check
    coordinator = await check_coordinator(
        pg_host, pg_port, pg_db, pg_user, pg_password
    )
    if coordinator:
        print(f"👑 Active coordinator: {coordinator}")
    else:
        print("👑 No active coordinator — this node will become coordinator")
    
    # 7. TLS cert generation
    tls_cert, tls_key, tls_ca = '', '', ''
    if tls_enabled:
        print(f"\n🔐 Generating TLS certificates...")
        tls_ca = get_or_create_ca(certs_dir)
        tls_cert, tls_key = generate_tls_cert(node_name, certs_dir)
        if tls_cert:
            print(f"   Cert: {tls_cert}")
            print(f"   Key:  {tls_key}")
            print(f"   CA:   {tls_ca}")
        else:
            print("   ⚠️  TLS cert generation failed — using no TLS")
            tls_enabled = False
    
    # 8. Venv setup (Windows especially needs this)
    if platform_name == 'windows' and not os.path.exists(os.path.join(script_dir, '.venv')):
        venv_python = setup_venv(script_dir)
        print(f"📦 venv: {venv_python}")
    
    # 9. Config generation
    print(f"\n📝 Generating config: {config_path}")
    config = generate_config(
        node_name=node_name,
        platform_name=platform_name,
        lan_ip=lan_ip,
        tailscale_ip=tailscale_ip,
        p2p_port=p2p_port,
        health_port=health_port,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_db=pg_db,
        pg_user=pg_user,
        pg_password=pg_password,
        discovered_nodes=discovered_nodes,
        advertise_host=advertise_host,
        tls_enabled=tls_enabled,
        tls_cert=tls_cert,
        tls_key=tls_key,
        tls_ca=tls_ca,
    )
    write_config(config, config_path)
    print(f"✅ Config written: {config_path}")
    
    # 10. Service installation
    service_path = None
    if install_svc:
        print(f"\n🔧 Installing auto-start service...")
        service_path = install_service(node_name, config_path, script_dir, platform_name)
        if service_path:
            print(f"✅ Service installed: {service_path}")
    
    # 11. Summary
    summary = {
        'node_name': node_name,
        'platform': platform_name,
        'lan_ip': lan_ip,
        'tailscale_ip': tailscale_ip,
        'advertise_host': advertise_host,
        'p2p_port': p2p_port,
        'health_port': health_port,
        'config_path': config_path,
        'certs': {'cert': tls_cert, 'key': tls_key, 'ca': tls_ca} if tls_enabled else {},
        'discovered_nodes': len(discovered_nodes),
        'coordinator': coordinator,
        'service_path': service_path,
        'script_dir': script_dir,
    }
    
    print(f"\n{'=' * 60}")
    print(f"✅ Bootstrap complete!")
    print(f"\nNext steps:")
    print(f"  1. Review config: {config_path}")
    if platform_name == 'windows':
        print(f"  2. Start node:    {script_dir}\\.venv\\Scripts\\python.exe cli.py start --name {node_name} --config {config_path}")
        print(f"     Or use:        {service_path or 'start_' + node_name + '.bat'}")
    else:
        print(f"  2. Start node:    {script_dir}/.venv/bin/python3 cli.py start --name {node_name} --config {config_path}")
        if service_path:
            if platform_name == 'macos':
                print(f"     Service:       launchctl start com.hermes.a2a-mesh-node.{node_name}")
            elif platform_name == 'linux':
                print(f"     Service:       systemctl --user start a2a-mesh-{node_name}")
    print(f"  3. Check health:  curl http://{lan_ip}:{health_port}/health")
    print(f"  4. Dashboard:     http://{lan_ip}:{health_port}/dashboard")
    print()
    
    return summary


if __name__ == '__main__':
    # Quick test
    plat = detect_platform()
    ips = detect_ip_addresses()
    print(f"Platform: {plat}")
    print(f"LAN IP: {ips['lan']}")
    print(f"Tailscale: {ips['tailscale']}")
    if plat in ('docker', 'ha_addon'):
        print(f"Docker host: {get_docker_host_ip()}")