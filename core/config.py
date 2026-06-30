"""A2A Mesh Config — Configuration loading and defaults."""

import logging
import os
import yaml
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

log = logging.getLogger("a2a_mesh.config")


@dataclass
class PGConfig:
    host: str = os.environ.get("A2A_PG_HOST", "192.168.1.30")
    port: int = int(os.environ.get("A2A_PG_PORT", "5432"))
    dbname: str = os.environ.get("A2A_PG_DBNAME", "agent_memory")
    user: str = os.environ.get("A2A_PG_USER", "nova")
    password: str = os.environ.get("A2A_PG_PASSWORD", "")
    channels: List[str] = field(default_factory=lambda: [
        "a2a_channel", "a2a_steer_channel", "delegation_channel", "mesh_channel"
    ])

    @classmethod
    def from_dsn(cls, dsn: str) -> "PGConfig":
        """Parse a PostgreSQL DSN string (postgresql://user:pass@host:port/dbname)."""
        import re
        m = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', dsn)
        if m:
            return cls(host=m.group(3), port=int(m.group(4)),
                       dbname=m.group(5), user=m.group(1), password=m.group(2))
        # Try without port
        m = re.match(r'postgresql://([^:]+):([^@]+)@([^/]+)/(.+)', dsn)
        if m:
            return cls(host=m.group(3), dbname=m.group(4),
                       user=m.group(1), password=m.group(2))
        log.warning(f"Could not parse DSN: {dsn[:30]}...")
        return cls()


@dataclass
class P2PConfig:
    enabled: bool = True
    listen_host: str = "0.0.0.0"
    listen_port: int = 8645
    max_connections: int = 50
    idle_timeout: int = 300  # seconds
    reconnect_interval: int = 5  # base retry interval in seconds (exponential backoff)
    tls_enabled: bool = False
    tls_cert: str = ""       # Path to TLS certificate (PEM)
    tls_key: str = ""        # Path to TLS private key (PEM)
    tls_ca: str = ""         # Path to CA certificate for peer verification
    tls_verify_peer: bool = False  # Verify peer certificates


@dataclass
class HTTPConfig:
    url: str = os.environ.get("A2A_HTTP_URL", "http://192.168.1.30:8199")
    health_url: str = os.environ.get("A2A_HEALTH_URL", "http://192.168.1.30:8198/health")
    timeout: int = 5
    retries: int = 3


@dataclass
class DiscoveryConfig:
    mdns_enabled: bool = True
    mdns_service: str = "_a2a._tcp"
    mdns_port: int = 8645
    udp_broadcast_enabled: bool = True
    udp_broadcast_port: int = 8646  # Convention: p2p_port + 1
    udp_broadcast_interval: int = 15  # seconds between announcements
    tailscale_interface: str = ""  # Tailscale IP for cross-subnet discovery
    static_nodes: List[Dict] = field(default_factory=list)


@dataclass
class SecurityConfig:
    signing_key: str = ""  # Ed25519 private key hex (auto-generated if empty)
    trusted_keys: Dict[str, str] = field(default_factory=dict)
    encryption: str = "nacl"  # "nacl" or "none"
    transport_auth: str = "hmac"  # "hmac" or "none"


@dataclass
class LoopPreventionConfig:
    self_reference_filter: bool = True
    not_for_me_filter: bool = True
    re_chain_limit: int = 4
    dedup_cache_size: int = 5000
    dedup_ttl: int = 300


@dataclass
class AutoSteerConfig:
    priority_threshold: int = 10  # P10+ = immediate webhook
    queue_lower_priorities: bool = True  # P1-9 = queued backlog


@dataclass
class HeartbeatConfig:
    interval: int = 300  # seconds
    warning_threshold: int = 300  # 5 min
    critical_threshold: int = 900  # 15 min
    silent_on_success: bool = True


@dataclass
class TopologyConfig:
    """Zigbee-inspired topology configuration."""
    node_role: str = "end_device"          # "coordinator", "router", "end_device"
    routing_mode: str = "hybrid"           # "flood", "tree", "hybrid"
    max_children: int = 20                 # Cm: max children per router
    max_routers: int = 6                   # Rm: max router children per node
    max_depth: int = 5                     # Lm: max tree depth
    trust_center_enabled: bool = True       # Coordinator validates joiners
    auto_approve_known_agents: bool = True  # Auto-approve agents with known keys
    allowed_public_keys: List[str] = field(default_factory=list)
    enable_sleepy_end_devices: bool = True   # Buffer messages for offline children
    message_buffer_max_size: int = 1000     # Per child
    message_buffer_ttl_seconds: int = 86400  # 24 hours
    re_association_timeout: int = 30         # Seconds before re-association
    coordinator_heartbeat_interval: int = 10  # Seconds
    enable_route_cache: bool = True
    route_cache_ttl: int = 300              # Seconds


@dataclass
class MeshConfig:
    """Full mesh configuration."""
    node_name: str = "nova"
    node_id: str = ""
    public_key: str = ""

    # Agent capabilities — declared here so each node advertises what it can do
    # These are registered in the Agent Registry on startup and shared via P2P discovery
    capabilities: List[str] = field(default_factory=lambda: [
        "a2a_messaging",       # Core mesh messaging (every node has this)
        "file_transfer",       # Can send/receive files via mesh
        "p2p_transport",       # Direct P2P TCP connections
        "pg_transport",        # PG NOTIFY transport
        "registry",            # Agent registry (coordinator nodes)
        "dashboard",           # Web dashboard (coordinator nodes)
        "health_monitor",      # Health monitoring and scoring
    ])

    # Agent skills — fine-grained abilities that other agents can discover via P2P handshake
    # Each skill has a name, description, and optional input/output schemas
    # Skills are shared during P2P connection and registered in the Agent Registry
    skills: List[Dict[str, Any]] = field(default_factory=lambda: [
        {
            "id": "mesh_send",
            "name": "Send Message",
            "description": "Send a message to another agent or broadcast to all",
            "tags": ["messaging", "send"],
        },
        {
            "id": "mesh_discover",
            "name": "Discover Agents",
            "description": "List all agents in the mesh and their capabilities",
            "tags": ["discovery", "agents"],
        },
        {
            "id": "mesh_health",
            "name": "Health Check",
            "description": "Get health status and metrics of this agent",
            "tags": ["health", "monitoring"],
        },
        {
            "id": "gdm",
            "name": "Group Decision Making",
            "description": "Coordinate multi-agent decisions with voting, consensus, and ranking protocols",
            "tags": ["gdm", "decision", "voting", "consensus", "coordination"],
        },
        {
            "id": "task_execution",
            "name": "Task Execution",
            "description": "Execute delegated tasks and report results back to the coordinator",
            "tags": ["task", "execution", "delegation"],
        },
    ])

    # Transport priority (first success wins for directed messages)
    # P2P first — PG is optional fallback for nodes without direct P2P connectivity
    transport_priority: List[str] = field(default_factory=lambda: [
        "p2p", "pg_notify", "http"
    ])

    # Sub-configs
    pg: PGConfig = field(default_factory=PGConfig)
    p2p: P2PConfig = field(default_factory=P2PConfig)
    http: HTTPConfig = field(default_factory=HTTPConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    loop_prevention: LoopPreventionConfig = field(default_factory=LoopPreventionConfig)
    auto_steer: AutoSteerConfig = field(default_factory=AutoSteerConfig)
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    topology: TopologyConfig = field(default_factory=TopologyConfig)

    # Webhook config
    webhook_port: int = 8644
    webhook_secret: str = ""

    # Telegram config
    telegram_chat_id: str = ""

    # Auth config
    auth_mode: str = "open"  # open, whitelist, tofu
    auth_whitelist: List[str] = field(default_factory=list)

    # Health endpoint
    health_port: int = 8650

    # Plugin config — each key is a plugin name, value is its config dict
    # Example: {"gateway": {"enabled": True, "platforms": {...}}, "notification": {...}}
    plugins: Dict[str, Any] = field(default_factory=dict)

    # Log file
    log_file: str = os.path.expanduser("~/.hermes/logs/a2a_mesh.log")

    @classmethod
    def from_yaml(cls, path: str) -> 'MeshConfig':
        """Load configuration from YAML file with env var interpolation."""
        if not os.path.exists(path):
            return cls()

        with open(path, 'r') as f:
            raw = f.read()

        # Interpolate environment variables
        import re
        def env_replace(match):
            key = match.group(1)
            return os.environ.get(key, match.group(0))

        raw = re.sub(r'\$\{(\w+)\}', env_replace, raw)
        raw = re.sub(r'\$(\w+)', env_replace, raw)

        data = yaml.safe_load(raw)
        if not data:
            return cls()

        config = cls()

        # Map YAML keys to dataclass fields
        mesh = data.get('mesh', {})
        config.node_name = mesh.get('node_name', config.node_name)

        # PG config — support A2A_MESH_PG_DSN env var for easy setup
        pg_dsn = os.environ.get("A2A_MESH_PG_DSN", "")
        pg_data = mesh.get('transports', {}).get('pg_notify', {})
        if pg_dsn:
            # DSN env var takes priority (easiest for new nodes)
            config.pg = PGConfig.from_dsn(pg_dsn)
            # Override with YAML values if present
            if pg_data:
                config.pg.host = pg_data.get('host', config.pg.host)
                config.pg.port = pg_data.get('port', config.pg.port)
                config.pg.channels = pg_data.get('channels', config.pg.channels)
        elif pg_data:
            config.pg = PGConfig(
                host=pg_data.get('host', config.pg.host),
                port=pg_data.get('port', config.pg.port),
                dbname=pg_data.get('dbname', config.pg.dbname),
                user=pg_data.get('user', config.pg.user),
                password=pg_data.get('password', config.pg.password),
                channels=pg_data.get('channels', config.pg.channels),
            )

        # P2P config
        p2p_data = mesh.get('transports', {}).get('p2p', {})
        if p2p_data:
            config.p2p = P2PConfig(
                enabled=p2p_data.get('enabled', config.p2p.enabled),
                listen_host=p2p_data.get('listen_host', config.p2p.listen_host),
                listen_port=p2p_data.get('listen_port', config.p2p.listen_port),
                max_connections=p2p_data.get('max_connections', config.p2p.max_connections),
                idle_timeout=p2p_data.get('idle_timeout', config.p2p.idle_timeout),
                reconnect_interval=p2p_data.get('reconnect_interval', config.p2p.reconnect_interval),
                tls_enabled=p2p_data.get('tls_enabled', config.p2p.tls_enabled),
                tls_cert=p2p_data.get('tls_cert', config.p2p.tls_cert),
                tls_key=p2p_data.get('tls_key', config.p2p.tls_key),
                tls_ca=p2p_data.get('tls_ca', config.p2p.tls_ca),
                tls_verify_peer=p2p_data.get('tls_verify_peer', config.p2p.tls_verify_peer),
            )

        # HTTP config
        http_data = mesh.get('transports', {}).get('http', {})
        if http_data:
            config.http = HTTPConfig(
                url=http_data.get('url', config.http.url),
                health_url=http_data.get('health_url', config.http.health_url),
                timeout=http_data.get('timeout', config.http.timeout),
                retries=http_data.get('retries', config.http.retries),
            )

        # Discovery config
        disc_data = mesh.get('discovery', {})
        if disc_data:
            static_nodes = disc_data.get('static', {}).get('nodes', [])
            config.discovery = DiscoveryConfig(
                mdns_enabled=disc_data.get('mdns', {}).get('enabled', True),
                mdns_service=disc_data.get('mdns', {}).get('service', '_a2a._tcp'),
                mdns_port=disc_data.get('mdns', {}).get('port', 8645),
                udp_broadcast_enabled=disc_data.get('udp_broadcast', {}).get('enabled', True),
                udp_broadcast_port=disc_data.get('udp_broadcast', {}).get('port', 8646),
                udp_broadcast_interval=disc_data.get('udp_broadcast', {}).get('interval', 15),
                tailscale_interface=disc_data.get('tailscale_interface', ''),
                static_nodes=static_nodes,
            )

        # Security config
        sec_data = mesh.get('security', {})
        if sec_data:
            config.security = SecurityConfig(
                signing_key=sec_data.get('signing_key', ''),
                trusted_keys=sec_data.get('trusted_keys', {}),
                encryption=sec_data.get('encryption', 'nacl'),
                transport_auth=sec_data.get('transport_auth', 'hmac'),
            )

        # Loop prevention
        lp_data = mesh.get('loop_prevention', {})
        if lp_data:
            config.loop_prevention = LoopPreventionConfig(**{
                k: v for k, v in lp_data.items()
                if k in LoopPreventionConfig.__dataclass_fields__
            })

        # Webhook
        config.webhook_port = int(os.environ.get('WEBHOOK_PORT', mesh.get('webhook_port', 8644)))
        config.webhook_secret = os.environ.get('WEBHOOK_SECRET', mesh.get('webhook_secret', ''))
        config.telegram_chat_id = os.environ.get('A2A_TELEGRAM_CHAT_ID', mesh.get('telegram_chat_id', ''))

        # Auth mode
        config.auth_mode = mesh.get('auth_mode', 'open')
        config.health_port = int(mesh.get('health_port', 8650))

        # Skills and capabilities from YAML (override defaults)
        if 'capabilities' in mesh:
            config.capabilities = mesh['capabilities']
        if 'skills' in mesh:
            config.skills = mesh['skills']

        # Plugin config
        if 'plugins' in mesh:
            config.plugins = mesh['plugins']

        # Validate: P2P port must differ from health port to avoid conflicts
        # Convention: P2P port = health_port - 5 (e.g., 8650→8645) for consistency across nodes
        if config.p2p.listen_port == config.health_port:
            config.p2p.listen_port = config.health_port - 5
            log.info(f"P2P port == health port, setting P2P port to {config.p2p.listen_port} (health_port - 5)")

        # Heartbeat config
        hb_data = mesh.get('heartbeat', {})
        if hb_data:
            config.heartbeat = HeartbeatConfig(
                interval=hb_data.get('interval', 300),
                warning_threshold=hb_data.get('warning_threshold', 300),
                critical_threshold=hb_data.get('critical_threshold', 900),
                silent_on_success=hb_data.get('silent_on_success', True),
            )

        # Topology config
        topo_data = mesh.get('topology', {})
        if topo_data:
            config.topology = TopologyConfig(
                node_role=topo_data.get('node_role', 'end_device'),
                routing_mode=topo_data.get('routing_mode', 'hybrid'),
                max_children=topo_data.get('max_children', 20),
                max_routers=topo_data.get('max_routers', 6),
                max_depth=topo_data.get('max_depth', 5),
                trust_center_enabled=topo_data.get('trust_center_enabled', True),
                auto_approve_known_agents=topo_data.get('auto_approve_known_agents', True),
                allowed_public_keys=topo_data.get('allowed_public_keys', []),
                enable_sleepy_end_devices=topo_data.get('enable_sleepy_end_devices', True),
                message_buffer_max_size=topo_data.get('message_buffer_max_size', 1000),
                message_buffer_ttl_seconds=topo_data.get('message_buffer_ttl_seconds', 86400),
                re_association_timeout=topo_data.get('re_association_timeout', 30),
                coordinator_heartbeat_interval=topo_data.get('coordinator_heartbeat_interval', 10),
                enable_route_cache=topo_data.get('enable_route_cache', True),
                route_cache_ttl=topo_data.get('route_cache_ttl', 300),
            )

        return config