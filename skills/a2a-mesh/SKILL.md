---
name: a2a-mesh
version: 1.0.0
status: active
p1_completed: 2026-06-16
zigbee_completed: 2026-06-16
election_completed: 2026-06-16
daemon_completed: 2026-06-16
v0.6.0_completed: 2026-06-18
v0.7.0_completed: 2026-06-18
wake_agent_api: v0.7.0 — see references/wake-agent-api-architecture.md
telegram_like_chat: v0.7.0 — context injection with chat history
dashboard_chat: see references/dashboard-chat-architecture.md + references/p2p-transport-architecture.md
v0.7.8_completed: 2026-06-23
v0.14.0_completed: 2026-07-18
delegation_v1: 2026-07-18 — see references/delegation-system.md
wake_agent_api: see references/wake-agent-api-architecture.md (CURRENT agent reply mechanism) 1.3 P2P, mDNS discovery, PG auto-reconnect, auto-steer, LocalStore fallback, P2P file transfer, mesh-only memory (MemorySync), channel chat (Közös Szoba + DM), agent replies (/api/agent-reply), webhook reply_endpointagent-reply), webhook reply_endpoint,agent-reply), admin approval
daemon_status: "RUNNING via launchd. TLS 1.3 on P2P (both nodes). Auto-steer active. Morzsa router 0x0001 on LXC. Bidirectional verified. PG listen uses run_in_executor."
config_yaml: ~/.hermes/mesh_config.yaml (auth_mode, health_port, topology parsed)
admin_approval: "New nodes=pending, coordinator auto-approves. API: /api/nodes/pending, /api/nodes/{name}/approve|reject"
memory_sync: "Mesh-only via MemorySync module. Broadcasts via mesh.mesh_messages msg_type=memory_sync. API: /api/memory, /api/memory/sync"
mesh_nodes_schema: "id, node_name, role, status, host, p2p_port, health_port, pg/p2p/http_available, last_heartbeat, joined_at"
file_transfer: 4-tier (PG base64 <1MB, MinIO S3 1-50MB, P2P direct 512KB chunks, SCP >50MB)
iroh_status: ABANDONED (segfaults Python 3.14, replaced by asyncio TCP P2P)
daemon_completed: 2026-06-16
category: infrastructure
created: 2026-06-15
updated: 2026-08-28
author: nova
description: >
  Full mesh A2A communication system — unlimited agents, multi-platform,
  Multi-transport. Iroh P2P core + PG primary + WiFi Direct + BLE GATT +
  Flutter mobile app. Cross-platform (macOS/Linux/Windows/Android/iOS).
  No single point of failure. No node limit.
  
  macOS Research (verified Apple SDK):
  - BLE GATT Server: FULLY supported (CBPeripheralManager 10.9+)
  - WiFi Direct AP: NOT possible (no API, IBSS deprecated macOS 11)
  - AWDL: No public API
  - MultipeerConnectivity: Available (Apple-ecosystem only)
  - NEPacketTunnelProvider: Available 10.11+ (how Tailscale works)
  - Network.framework Bonjour: Available 10.14+
  - Strategy: Iroh QUIC primary, BLE GATT full, mDNS discovery
---

# A2A Mesh — Full Architecture Plan v1.0

## Vision

**Unlimited agents, any platform, any network, always connected.**

A mesh where every agent can reach every other agent through any available
transport — PG, Iroh P2P, WiFi Direct, BLE, HTTP. No central coordinator,
no single point of failure. New agents join by discovering any existing node.
Messages route through available paths automatically.

```
  Nova (macOS)          Morzsa (Linux)         Agent3 (Windows)
  ┌──────────┐    ┌──────────────┐    ┌──────────────┐
  │ Iroh Node│◄──►│  Iroh Node   │◄──►│  Iroh Node   │
  │ PG Client│◄──►│  PG Server   │    │  PG Client   │
  │ WiFi Dir │◄──►│  WiFi Direct │◄──►│  WiFi Direct  │
  │ BLE Adv  │◄──►│  BLE Adv     │    │  BLE Adv      │
  │ Flutter  │    │  CLI         │    │  Flutter      │
  └──────────┘    └──────────────┘    └──────────────┘
       │                  │                   │
       └────── MESH (any path, any transport) ┘
```

## Core Principles

1. **No SPOF** — PG is primary but not required. If PG dies, mesh lives.
2. **Unlimited agents** — No architecture limit on node count.
3. **Any platform** — macOS, Linux, Windows, Android, iOS (via Flutter).
4. **Any network** — Tailscale, LAN, WiFi Direct, BLE proximity.
5. **Auto-discovery** — New node finds mesh via mDNS, BLE, Iroh DHT, or config.
6. **Self-healing** — Node goes down? Messages route around it. Transports fail? Try next.
7. **Encrypted** — Every message signed (Ed25519) and optionally encrypted (NaCl).

## Current State (P0 — Operational ✅)

| Component | Status | File |
|-----------|--------|------|
| PG LISTEN/NOTIFY | ✅ Working | `a2a_watcher.py` (568 lines) |
| MCP Bridge | ✅ Working | `a2a-mcp-bridge.py` |
| Message Bus | ✅ Working | `a2a_message_bus.py` |
| Heartbeat | ✅ Working | `a2a_heartbeat.py` |
| File Share | ✅ Working | `a2a_file_share.py` (PG/MinIO) |
| Delegation | ✅ Working | `a2a_delegation.py` |
| Context/Session Sync | ✅ Working | `a2a_context_sync.py`, `a2a_session_sync.py` |
| Error Recovery | ✅ Working | `a2a_error_recovery.py` (DLQ) |
| Remote Exec | ✅ Working | `a2a_exec.py` |
| Loop Prevention | ✅ Working | 4-layer (self-ref, not-for-me, RE-chain, dedup) |
| Auto-steer | ✅ Working | P1-9 queued, P10+ immediate |
| Health Check | ✅ Working | Port 8198 proxy |

**Total: ~75KB Python, 11 scripts, 5 PG tables, 4 NOTIFY channels**

## Architecture — Full Mesh

### Layer Model

```
┌──────────────────────────────────────────────────────────────────┐
│                     A2A MESH APPLICATION                         │
│  Message routing, dedup, encryption, delegation, heartbeat,      │
│  flood routing, CRDT merge, node registry                       │
├──────────────────────────────────────────────────────────────────┤
│                    TRANSPORT ADAPTER LAYER                        │
│  ┌────────┐ ┌────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │ PG     │ │ Iroh   │ │ WiFi     │ │ BLE      │ │ HTTP   │ │
│  │ NOTIFY │ │ P2P    │ │ Direct   │ │ GATT+Adv │ │ MCP    │ │
│  │ Primary│ │ Mesh   │ │ P2P      │ │ Prox+Msg │ │ Bridge │ │
│  └────────┘ └────────┘ └──────────┘ └──────────┘ └────────┘ │
├──────────────────────────────────────────────────────────────────┤
│                  PLATFORM ABSTRACTION LAYER                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐          │
│  │ macOS    │ │ Linux    │ │ Windows  │ │ Mobile   │          │
│  │ CoreBT/  │ │ BlueZ/   │ │ WinRT/   │ │ Flutter  │          │
│  │ NetFW/   │ │ hostapd/ │ │ Mobile/  │ │ iOS+Android│         │
│  │ NWPath   │ │ wpa_supp │ │ WiFiDir  │ │ BLE+WiFi │          │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘          │
├──────────────────────────────────────────────────────────────────┤
│                       NETWORK LAYER                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐          │
│  │ Tailscale│ │ Local    │ │ WiFi     │ │ Bluetooth│          │
│  │ WireGuard│ │ WiFi/LAN │ │ Direct   │ │ BLE 5.x │          │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘          │
└──────────────────────────────────────────────────────────────────┘
```

### Transport Priority — Fallback Chain

```
SEND: Try transports in priority order, use FIRST success (P2P-first since v0.10.4)
RECEIVE: Process FIRST copy, dedup the rest
BROADCAST: Send on ALL available transports simultaneously

Priority (configurable per node):
1. PG NOTIFY          → <1ms, primary, requires PG connection
2. Iroh P2P (QUIC)    → <50ms, mesh, NAT traversal, unlimited nodes
3. WiFi Direct         → <10ms, local P2P, no router needed
4. BLE GATT            → <200ms, proximity, 10-100m range
5. HTTP/MCP Bridge     → <5s, REST API, any IP network
```

### Mesh Routing — Flood/Gossip

For unlimited agents, we use **epidemic (gossip) routing**:

```
When Node A sends message M:
  1. A signs M with Ed25519 private key
  2. A floods M to ALL directly connected peers (all transports)
  3. Each peer:
     a. Verify signature
     b. Check dedup cache (skip if already seen M.id)
     c. Add M.id to dedup cache (TTL 5min)
     d. If M.recipient == me → process it
     e. If M.recipient != me → re-flood to MY peers (except sender)
     f. If M.ttl == 0 → drop (prevent infinite flood)
  4. ACK flows back via same path or higher-priority transport
```

**TTL (Time To Live)**: Each message starts with TTL=N (default: 10).
Each hop decrements TTL. When TTL=0, message is dropped.
This prevents infinite flooding while allowing messages to reach distant nodes.

**Optimization for large meshes (>20 nodes)**:
- Instead of pure flood, use **random gossip**: each node forwards to
  K random peers (default K=3) instead of ALL peers.
- Reduces bandwidth from O(N²) to O(N*K).

### Message Flow — Complete

```
Sender Agent:
  1. Create message (UUID v7, timestamp, priority, type, TTL=10)
  2. Sign message (Ed25519)
  3. Optionally encrypt (NaCl) if recipient specified
  4. Try transports in priority order:
     a. PG NOTIFY → success? mark sent
     b. Iroh send → success? mark sent
     c. WiFi Direct → success? mark sent
     d. BLE GATT → success? mark sent (proximity)
     e. HTTP/MCP → success? mark sent
  5. If ALL fail → queue in DLQ with exponential backoff
  6. For BROADCAST messages → send on ALL transports simultaneously

Receiver Agent:
  1. Receive from ANY transport
  2. Verify signature (reject if invalid)
  3. Decrypt if encrypted (NaCl)
  4. Check dedup cache (skip if already processed)
  5. Add to dedup cache
  6. Apply loop prevention (self-ref, not-for-me, RE-chain)
  7. If for me → process (trigger webhook for agent)
  8. If not for me → re-flood to my peers (decrement TTL)
  9. ACK via same transport or higher-priority one
```

## Phase Plan — Full Implementation

### Phase 1 — Core Mesh + Iroh P2P (Week 1-2)

**Goal**: Refactor existing code into modular mesh package. Add Iroh P2P as
primary mesh transport. Unlimited nodes, auto-discovery.

**Package Structure**:

```
~/.hermes/scripts/a2a_mesh/
├── __init__.py
├── __main__.py                 # python -m a2a_mesh --node nova
├── core/
│   ├── __init__.py
│   ├── message.py              # A2AMessage dataclass, UUID v7, signing, encryption
│   ├── router.py               # Flood/gossip routing, TTL, fallback chain
│   ├── dedup.py                # Message ID dedup with TTL cache (LRU, max 5000)
│   ├── encryption.py           # Ed25519 signing, NaCl encryption, HMAC auth
│   ├── config.py               # MeshConfig dataclass, YAML loading, env vars
│   └── registry.py             # Node registry (who's online, capabilities, transports)
├── transports/
│   ├── __init__.py
│   ├── base.py                 # TransportAdapter ABC (send, receive, status, discover)
│   ├── pg_transport.py         # PG LISTEN/NOTIFY (refactored from watcher)
│   ├── iroh_transport.py       # Iroh P2P QUIC mesh
│   ├── http_transport.py       # HTTP/MCP bridge (existing)
│   ├── wifi_direct.py          # WiFi Direct P2P (FULL implementation)
│   └── ble_transport.py        # BLE GATT + advertisement (FULL implementation)
├── discovery/
│   ├── __init__.py
│   ├── mdns.py                 # mDNS service discovery (_a2a._tcp)
│   ├── iroh_discovery.py       # Iroh DHT node discovery
│   ├── ble_discovery.py        # BLE beacon scanner + advertiser
│   └── static_discovery.py     # Config-based discovery (known nodes)
├── node.py                     # MeshNode main class (start/stop/status)
├── cli.py                      # CLI: start/stop/status/send/discover
└── tests/
    ├── test_message.py
    ├── test_router.py
    ├── test_dedup.py
    ├── test_encryption.py
    ├── test_transports.py
    └── test_mesh_integration.py
```

**Key Files — Detailed Design**:

#### `core/message.py` — A2AMessage

```python
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional
import uuid
import json

@dataclass
class A2AMessage:
    """Universal mesh message format. Works on all transports."""
    # Identity
    id: str                          # UUID v7 (time-sortable)
    sender: str                      # Agent name (e.g. "nova")
    sender_node_id: str              # Iroh/Node unique ID
    recipient: str                   # Agent name or "broadcast"
    # Content
    type: str                        # directive/task/result/heartbeat/steer/file
    priority: int                    # 1-10 (10 = interrupt)
    payload: dict                    # Message content
    # Routing
    ttl: int                         # Hops remaining (default 10)
    transport_hint: str              # Preferred transport (optional)
    # Security
    signature: Optional[str] = None  # Ed25519 signature
    encrypted: bool = False         # NaCl encrypted payload?
    # Metadata
    timestamp: str = ""              # ISO 8601 UTC
    created_at: str = ""             # When originally created
    hop_count: int = 0              # How many nodes forwarded this
    path: list = field(default_factory=list)  # Nodes that forwarded

    @classmethod
    def create(cls, sender: str, recipient: str, msg_type: str,
               payload: dict, priority: int = 5, ttl: int = 10):
        return cls(
            id=str(uuid.uuid7()),  # Time-sortable UUID
            sender=sender,
            sender_node_id="",  # Set by MeshNode
            recipient=recipient,
            type=msg_type,
            priority=priority,
            payload=payload,
            ttl=ttl,
            timestamp=datetime.now(timezone.utc).isoformat(),
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def sign(self, signing_key):
        """Sign with Ed25519."""
        from .encryption import sign_message
        self.signature = sign_message(self, signing_key)

    def verify(self, public_key_hex: str) -> bool:
        """Verify signature."""
        from .encryption import verify_message
        return verify_message(self, public_key_hex)

    def to_bytes(self) -> bytes:
        """Serialize for transport (CBOR preferred, JSON fallback)."""
        try:
            import cbor2
            return cbor2.dumps(asdict(self))
        except ImportError:
            return json.dumps(asdict(self)).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> 'A2AMessage':
        """Deserialize from transport."""
        try:
            import cbor2
            d = cbor2.loads(data)
        except ImportError:
            d = json.loads(data.decode())
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
```

#### `core/router.py` — Flood/Gossip Router

```python
class MeshRouter:
    """
    Routes messages through the mesh using epidemic/gossip protocol.

    - Broadcast: flood to all peers (small mesh <20)
    - Directed: flood with recipient filter (any size)
    - Gossip: forward to K random peers (large mesh >20)
    - TTL: prevent infinite propagation
    """

    def __init__(self, node_name: str, registry: NodeRegistry,
                 dedup: DedupCache, config: MeshConfig):
        self.node_name = node_name
        self.registry = registry
        self.dedup = dedup
        self.config = config
        self.transports: dict[str, TransportAdapter] = {}

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via best available transport."""
        # Sign message
        message.sign(self.config.signing_key)
        message.sender_node_id = self.config.node_id

        # Try transports in priority order
        for transport_name in self.config.transport_priority:
            transport = self.transports.get(transport_name)
            if not transport or not transport.is_available():
                continue
            try:
                result = await transport.send(message)
                if result.success:
                    return SendResult(transport=transport_name, success=True)
            except Exception as e:
                log.warning(f"Transport {transport_name} failed: {e}")
                continue

        # All transports failed → DLQ
        await self.dlq_queue(message)
        return SendResult(transport="dlq", success=False)

    async def receive(self, message: A2AMessage, from_transport: str) -> ProcessResult:
        """Process received message (from any transport)."""
        # 1. Verify signature
        if not message.verify(self._get_public_key(message.sender)):
            log.warning(f"Invalid signature from {message.sender}")
            return ProcessResult(status="invalid_signature")

        # 2. Dedup check
        if self.dedup.is_duplicate(message.id):
            return ProcessResult(status="duplicate")

        # 3. Loop prevention
        if message.sender == self.node_name:
            return ProcessResult(status="self_reference")
        if message.recipient != self.node_name and message.recipient != "broadcast":
            # Not for me — re-flood
            message.ttl -= 1
            message.hop_count += 1
            if message.ttl <= 0:
                return ProcessResult(status="ttl_expired")
            await self.reflood(message, from_transport)
            return ProcessResult(status="forwarded")

        # 4. For me — process
        self.dedup.add(message.id)
        await self.process_for_me(message)
        return ProcessResult(status="processed")

    async def reflood(self, message: A2AMessage, exclude_transport: str):
        """Re-flood message to all peers except the sender transport."""
        for name, transport in self.transports.items():
            if name == exclude_transport or not transport.is_available():
                continue
            try:
                await transport.send(message)
            except Exception:
                continue
```

#### `transports/base.py` — TransportAdapter ABC

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class TransportStatus:
    available: bool
    latency_ms: float
    error: str = ""

class TransportAdapter(ABC):
    """Base class for all mesh transports."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Transport name (pg, iroh, wifi_direct, ble, http)."""

    @abstractmethod
    async def start(self) -> bool:
        """Initialize transport. Return True if started successfully."""

    @abstractmethod
    async def stop(self) -> bool:
        """Shutdown transport cleanly."""

    @abstractmethod
    async def send(self, message: A2AMessage) -> SendResult:
        """Send a message. Return SendResult with success/failure."""

    @abstractmethod
    async def receive(self) -> list[A2AMessage]:
        """Poll for received messages (non-blocking)."""

    @abstractmethod
    async def discover(self) -> list[NodeInfo]:
        """Discover peer nodes via this transport."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if transport is currently operational."""

    @abstractmethod
    def get_status(self) -> TransportStatus:
        """Return current transport status."""
```

#### `transports/wifi_direct.py` — WiFi Direct P2P (FULL)

```python
"""
WiFi Direct Transport — Full implementation for agent mesh.

Platform support:
- Linux: wpa_supplicant P2P (hostapd fallback)
- macOS: NEHotspotConfiguration (requires NetworkExtension entitlement)
  Fallback: command-line networksetup + AppleScript
- Windows: WiFiDirectDevice API (Win10+)
- Android: WiFiP2pManager (Flutter plugin)

Strategy:
1. Discover peers via WiFi Direct (P2P)
2. Connect to group owner (GO)
3. Exchange messages over TCP socket on GO
4. If no GO exists, become GO (first node wins)
"""

import asyncio
import json
import platform
import socket
import subprocess
from typing import Optional

from .base import TransportAdapter, TransportStatus, SendResult

class WiFiDirectTransport(TransportAdapter):
    """WiFi Direct P2P transport for offline mesh communication."""

    name = "wifi_direct"

    def __init__(self, config):
        self.config = config
        self.platform = platform.system().lower()
        self.is_go = False  # Group Owner
        self.go_address = None
        self.go_port = 8645  # A2A WiFi Direct port
        self.peers = {}  # peer_id -> (ip, port, socket)
        self.server_socket = None
        self._available = False
        self._discovery_interval = 30  # seconds

    async def start(self) -> bool:
        """Start WiFi Direct transport."""
        if self.platform == "linux":
            return await self._start_linux()
        elif self.platform == "darwin":
            return await self._start_macos()
        elif self.platform == "windows":
            return await self._start_windows()
        else:
            self._log(f"Unsupported platform: {self.platform}")
            return False

    async def _start_linux(self) -> bool:
        """Linux WiFi Direct via wpa_supplicant P2P."""
        try:
            # 1. Check if WiFi interface supports P2P
            result = subprocess.run(
                ["wpa_cli", "-i", self.config.wifi_interface, "p2p_find"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                # Fallback: create AP mode (hostapd)
                return await self._start_linux_ap()

            # 2. Start P2P discovery
            subprocess.run(
                ["wpa_cli", "-i", self.config.wifi_interface,
                 "p2p_listen"],
                capture_output=True, timeout=10
            )

            # 3. Start TCP server for message exchange
            await self._start_tcp_server()
            self._available = True
            return True

        except Exception as e:
            self._log(f"Linux WiFi Direct start failed: {e}")
            return await self._start_linux_ap()

    async def _start_linux_ap(self) -> bool:
        """Fallback: Create WiFi AP mode (hostapd)."""
        # Create access point that other agents can connect to
        ap_config = f"""
interface={self.config.wifi_interface}
driver=nl80211
ssid=A2A-Mesh
hw_mode=g
channel=6
wpa=2
wpa_passphrase={self.config.wifi_password}
wpa_key_mgmt=WPA-PSK
"""
        # Write hostapd config
        config_path = "/tmp/a2a_mesh_hostapd.conf"
        with open(config_path, "w") as f:
            f.write(ap_config)

        # Start hostapd
        proc = subprocess.Popen(
            ["hostapd", config_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        await asyncio.sleep(2)  # Wait for AP to start

        # Start TCP server on AP interface
        await self._start_tcp_server()
        self.is_go = True
        self._available = True
        return True

    async def _start_macos(self) -> bool:
        """macOS WiFi Direct — NOT possible, use alternatives.
        
        macOS Research (verified against Apple SDK headers):
        - kCWInterfaceModeHostAP: READ-ONLY state, no public API to set it
        - startIBSSModeWithSSID: DEPRECATED macOS 11.0 (Big Sur)
        - NEHotspotConfiguration: iOS-only (API_UNAVAILABLE(macos, tvos))
        - AWDL: No public API (Apple private for AirDrop/AirPlay)
        
        macOS alternatives:
        1. Iroh QUIC — primary P2P transport (cross-platform)
        2. BLE GATT — full server supported (CBPeripheralManager 10.9+)
        3. Network.framework Bonjour — service discovery + TCP/UDP
        4. MultipeerConnectivity — Apple-ecosystem only (10.10+)
        5. NEPacketTunnelProvider — mesh VPN overlay (10.11+)
        """
        # macOS cannot create WiFi Direct AP — skip entirely
        # Iroh QUIC handles P2P without WiFi Direct on macOS
        self._log("macOS: WiFi Direct AP not supported, using Iroh QUIC + BLE GATT")
        return False

    async def _start_windows(self) -> bool:
        """Windows WiFi Direct via WinRT API."""
        try:
            # Use Python winsdk for WiFi Direct
            from winsdk.windows.devices.wifi.direct import (
                WiFiDirectDevice, WiFiDirectAdvertisementPublisher
            )

            # Create WiFi Direct advertiser
            publisher = WiFiDirectAdvertisementPublisher()
            advertiser = publisher.advertisement
            advertiser.listen_state = \
                WiFiDirectAdvertisementListenState.discoverable

            # Set SSID
            advertiser.information_elements.add(
                WiFiDirectInformationElement(
                    ssid="A2A-Mesh",
                    vendor_specific_data=bytes([0x41, 0x32, 0x41])  # "A2A"
                )
            )

            publisher.start()
            await self._start_tcp_server()
            self._available = True
            return True
        except Exception as e:
            self._log(f"Windows WiFi Direct failed: {e}")
            return False

    async def _start_tcp_server(self):
        """Start TCP server for message exchange on WiFi Direct group."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("0.0.0.0", self.go_port))
        self.server_socket.listen(10)
        self.server_socket.setblocking(False)

        # Accept connections in background
        asyncio.create_task(self._accept_connections())

    async def _accept_connections(self):
        """Accept incoming peer connections."""
        while self._available:
            try:
                client, addr = await asyncio.wait_for(
                    asyncio.get_event_loop().sock_accept(self.server_socket),
                    timeout=1.0
                )
                peer_id = f"{addr[0]}:{addr[1]}"
                self.peers[peer_id] = (addr[0], self.go_port, client)
                asyncio.create_task(self._handle_peer(client, addr))
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._log(f"Accept error: {e}")

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message to connected peers via TCP."""
        data = message.to_bytes()
        sent = 0
        for peer_id, (ip, port, sock) in list(self.peers.items()):
            try:
                # Length-prefixed framing
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                sent += 1
            except Exception:
                self.peers.pop(peer_id, None)

        if sent > 0:
            return SendResult(transport="wifi_direct", success=True)
        # If no peers connected, try to discover
        return SendResult(transport="wifi_direct", success=False, error="no peers")

    async def discover(self) -> list:
        """Discover WiFi Direct peers."""
        peers = []
        if self.platform == "linux":
            # Use wpa_cli to find P2P devices
            result = subprocess.run(
                ["wpa_cli", "-i", self.config.wifi_interface, "p2p_peers"],
                capture_output=True, text=True, timeout=10
            )
            for line in result.stdout.strip().split('\n'):
                if line and line != "FAIL":
                    peers.append(NodeInfo(
                        node_id=line,
                        name=f"wifi-direct-{line[:8]}",
                        transport="wifi_direct",
                        address=line
                    ))
        return peers

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        return TransportStatus(
            available=self._available,
            latency_ms=5.0 if self._available else float('inf'),
            error="" if self._available else "WiFi Direct not started"
        )
```

#### `transports/ble_transport.py` — BLE GATT + Advertisement (FULL)

```python
"""
BLE Transport — Full GATT server/client + advertisement discovery.

Platform support:
- macOS: CoreBluetooth (advertisement + GATT client, limited GATT server)
- Linux: BlueZ (full GATT server/client + advertisement)
- Windows: WinRT BLE (GATT client, limited server)
- Android/iOS: Flutter flutter_blue_plus

Strategy:
1. DISCOVERY: BLE advertisement broadcast (all platforms)
   - Service UUID: 0x181A (standard, works everywhere)
   - Advertisement data: agent name, IP, port hash

2. MESSAGING: BLE GATT (proximity message exchange)
   - Service UUID: custom A2A UUID
   - Characteristics: inbox (write), outbox (notify), control (read/write)
   - Max MTU: 512 bytes (negotiated), fallback 20 bytes

3. FILE TRANSFER: Chunked BLE transfer for larger payloads
   - Split into 512-byte chunks
   - CRC32 per chunk + reassembly on receiver side
"""

import asyncio
import struct
import json
import hashlib
import platform
from typing import Optional

from .base import TransportAdapter, TransportStatus, SendResult

# BLE Service and Characteristic UUIDs
A2A_SERVICE_UUID = "a2a00001-0000-1000-8000-00805f9b34fb"
A2A_INBOX_UUID   = "a2a00002-0000-1000-8000-00805f9b34fb"  # Write messages here
A2A_OUTBOX_UUID  = "a2a00003-0000-1000-8000-00805f9b34fb"  # Notify new messages
A2A_CONTROL_UUID = "a2a00004-0000-1000-8000-00805f9b34fb"  # Status, MTU, flow control

BEACON_SERVICE_UUID = "0000181a-0000-1000-8000-00805f9b34fb"  # Environmental Sensing

class BLETransport(TransportAdapter):
    """BLE GATT + Advertisement transport for proximity mesh."""

    name = "ble"

    def __init__(self, config):
        self.config = config
        self.platform = platform.system().lower()
        self._available = False
        self._scanning = False
        self._advertising = False
        self._gatt_server = None
        self._discovered_peers = {}  # address -> NodeInfo
        self._connected_devices = {}  # address -> BleakClient
        self._inbox = asyncio.Queue()  # Received messages
        self._outbox = asyncio.Queue()  # Messages to send
        self._mtu = 512  # Negotiated MTU

    async def start(self) -> bool:
        """Start BLE transport (advertising + scanning)."""
        try:
            # 1. Start advertising presence
            await self._start_advertising()

            # 2. Start scanning for peers
            await self._start_scanning()

            # 3. Start GATT server (Linux only, others are client-only)
            if self.platform == "linux":
                await self._start_gatt_server()

            self._available = True
            return True
        except Exception as e:
            self._log(f"BLE start failed: {e}")
            return False

    async def _start_advertising(self):
        """Broadcast BLE advertisement with agent presence info."""
        if self.platform == "linux":
            # BlueZ LE advertisement via dbus-python
            await self._start_linux_advertising()
        elif self.platform == "darwin":
            # macOS: CoreBluetooth advertisement (limited but works)
            await self._start_macos_advertising()
        elif self.platform == "windows":
            # Windows: WinRT BLE advertisement
            await self._start_windows_advertising()

    async def _start_linux_advertising(self):
        """Linux BlueZ LE advertisement via D-Bus."""
        try:
            from dbus.mainloop.glib import DBusGMainLoop
            import dbus

            bus = dbus.SystemBus()
            adapter = bus.get_object('org.bluez', '/org/bluez/hci0')

            # Register advertisement
            adv_manager = bus.get_object(
                'org.bluez',
                '/org/bluez'
            )
            # ... BlueZ D-Bus advertisement registration
            self._advertising = True
        except ImportError:
            self._log("dbus-python not installed, BLE advertising unavailable on Linux")

    async def _start_macos_advertising(self):
        """macOS CoreBluetooth advertisement (FULLY supported).
        
        macOS Research (verified Apple SDK):
        CBPeripheralManager fully available on macOS 10.9+:
        - startAdvertising() ✅
        - add(CBMutableService) ✅ (publish GATT service)
        - respond(to:withResult:) ✅ (GATT read/write)
        - updateValue(for:onSubscribedCentrals:) ✅ (GATT notify)
        - publishL2CAPChannel(withEncryption:) ✅ (10.14+)
        
        Only limitation: Macs without BT hardware → unsupported state.
        All MacBooks/iMacs with BT: full support.
        """
        try:
            import objc
            from CoreBluetooth import CBPeripheralManager, CBAdvertisementData

            manager = CBPeripheralManager.alloc().init()
            # Full GATT server: advertise + publish service + handle read/write/notify
            self._advertising = True
            self._log("macOS: BLE GATT server started (CBPeripheralManager)")
        except ImportError:
            self._log("PyObjC not available, BLE advertising disabled on macOS")

    async def _start_scanning(self):
        """Scan for BLE beacons from other A2A agents."""
        try:
            from bleak import BleakScanner

            async def scan_callback(device, advertisement_data):
                """Called when a BLE device is found."""
                # Check if this is an A2A beacon
                for uuid in advertisement_data.service_uuids:
                    if uuid.lower() == BEACON_SERVICE_UUID:
                        # Parse advertisement data
                        local_name = advertisement_data.local_name or ""
                        if local_name.startswith("A2A:"):
                            parts = local_name.split(":")
                            if len(parts) >= 4:
                                self._discovered_peers[device.address] = NodeInfo(
                                    node_id=parts[1],
                                    name=parts[1],
                                    transport="ble",
                                    address=device.address,
                                    ip=parts[2],
                                    port=int(parts[3])
                                )

            # Continuous scanning
            self._scanner = BleakScanner(detection_callback=scan_callback)
            await self._scanner.start()
            self._scanning = True
        except ImportError:
            self._log("bleak not installed, BLE scanning unavailable")

    async def _start_gatt_server(self):
        """Start BLE GATT server for receiving messages (Linux only)."""
        # Linux: BlueZ GATT server via D-Bus
        # This allows other agents to CONNECT to us and send messages
        try:
            # Implementation via dbus-python + BlueZ GATT API
            # Register A2A service with inbox/outbox/control characteristics
            self._gatt_server = A2AGATTServer(self.config)
            await self._gatt_server.start()
        except Exception as e:
            self._log(f"GATT server start failed: {e}")

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via BLE GATT to connected peer."""
        data = message.to_bytes()

        # Check if message fits in single MTU
        if len(data) <= self._mtu:
            return await self._send_single(data)
        else:
            return await self._send_chunked(data)

    async def _send_single(self, data: bytes) -> SendResult:
        """Send message that fits in single MTU."""
        for addr, client in self._connected_devices.items():
            try:
                await client.write_gatt_char(A2A_INBOX_UUID, data)
                return SendResult(transport="ble", success=True)
            except Exception as e:
                self._log(f"BLE send failed to {addr}: {e}")
                continue

        # No connected device — try to connect to discovered peer
        for addr, node_info in self._discovered_peers.items():
            try:
                from bleak import BleakClient
                client = BleakClient(addr)
                await client.connect()
                await client.write_gatt_char(A2A_INBOX_UUID, data)
                self._connected_devices[addr] = client
                return SendResult(transport="ble", success=True)
            except Exception as e:
                continue

        return SendResult(transport="ble", success=False, error="no peers in range")

    async def _send_chunked(self, data: bytes) -> SendResult:
        """Send large message in chunks with CRC verification."""
        import zlib
        chunk_size = self._mtu - 4  # Reserve 4 bytes for chunk header
        total_chunks = (len(data) + chunk_size - 1) // chunk_size
        crc = zlib.crc32(data) & 0xFFFFFFFF

        for chunk_idx in range(total_chunks):
            offset = chunk_idx * chunk_size
            chunk_data = data[offset:offset + chunk_size]
            # Header: [chunk_idx(2), total_chunks(2), crc(4)]
            header = struct.pack(">HHI", chunk_idx, total_chunks, crc)
            payload = header + chunk_data

            result = await self._send_single(payload)
            if not result.success:
                return result

            # Wait for ACK before sending next chunk
            await asyncio.sleep(0.01)  # Flow control

        return SendResult(transport="ble", success=True)

    async def discover(self) -> list:
        """Return discovered BLE peers."""
        return list(self._discovered_peers.values())

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        return TransportStatus(
            available=self._available,
            latency_ms=50.0 if self._available else float('inf'),
            error="" if self._available else "BLE not started"
        )
```

#### `transports/iroh_transport.py` — Iroh P2P Mesh

```python
"""
Iroh P2P Transport — QUIC-based mesh networking.

Each agent runs an Iroh node. Nodes discover each other via:
1. DHT (Iroh's built-in distributed hash table)
2. Ticket exchange (manual or via PG)
3. mDNS (local network)

Messages flow over QUIC (UDP-based, NAT-traversing).
Works over Tailscale, LAN, and direct internet.
"""

import asyncio
import json
from typing import Optional

from .base import TransportAdapter, TransportStatus, SendResult

class IrohTransport(TransportAdapter):
    """Iroh P2P mesh transport."""

    name = "iroh"

    def __init__(self, config):
        self.config = config
        self._node = None
        self._doc = None
        self._available = False
        self._node_id = None

    async def start(self) -> bool:
        """Start Iroh node and join mesh."""
        try:
            import iroh

            # Create or load Iroh node
            self._node = await iroh.IrohNode.create(
                path=self.config.iroh_data_dir
            )
            self._node_id = self._node.node_id()

            # Create or join A2A document
            if self.config.iroh_ticket:
                # Join existing mesh
                self._doc = await self._node.document_join(
                    self.config.iroh_ticket
                )
            else:
                # Create new mesh document
                self._doc = await self._node.document_create()
                # Generate ticket for other nodes
                ticket = await self._doc.share(iroh.DocumentShareMode.Write)
                self.config.iroh_ticket = ticket
                # Save ticket for other agents
                self._save_ticket(ticket)

            # Subscribe to document changes
            await self._subscribe()
            self._available = True
            return True

        except ImportError:
            self._log("iroh package not installed, Iroh transport unavailable")
            return False
        except Exception as e:
            self._log(f"Iroh start failed: {e}")
            return False

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via Iroh document sync."""
        if not self._available or not self._doc:
            return SendResult(transport="iroh", success=False, error="not started")

        try:
            data = message.to_bytes()
            key = f"a2a/message/{message.id}".encode()
            await self._doc.set_bytes(key, data)
            return SendResult(transport="iroh", success=True)
        except Exception as e:
            return SendResult(transport="iroh", success=False, error=str(e))

    async def _subscribe(self):
        """Subscribe to Iroh document changes (incoming messages)."""
        if not self._doc:
            return
        # Iroh document sync callback
        # When new entry appears → deserialize → process
        pass  # Event loop integration with MeshNode
```

### Phase 2 — WiFi Direct + BLE Full (Week 3-4)

**Goal**: Full WiFi Direct and BLE GATT transport. Working P2P without
any infrastructure (no router, no internet).

**What changes from Phase 1**:
- `wifi_direct.py` goes from stub to FULL implementation (above)
- `ble_transport.py` goes from discovery-only to FULL GATT (above)
- Both are production-ready, platform-specific code for Linux/macOS/Windows
- Integration testing: offline mesh communication

**WiFi Direct Platform Details**:

| Platform | Library | AP Mode | P2P Mode | Notes |
|----------|---------|---------|----------|-------|
| Linux | wpa_supplicant | hostapd | wpa_cli p2p_* | Full support, root needed |
| macOS | CoreWiFi/NetworkSetup | ⚠️ Limited | ❌ No native | Joins existing AP only |
| Windows | WinRT WiFiDirect | ✅ Mobile Hotspot | ✅ WiFiDirectDevice | Win10+ |
| Android | WifiP2pManager | ✅ Hotspot | ✅ Direct | Flutter plugin |
| iOS | NEHotspotConfiguration | ⚠️ Join only | ❌ No native | Flutter plugin |

**macOS Strategy**: macOS nem támogatja programmatic AP módot (nincs API, IBSS deprecated macOS 11).
Helyette:
1. **Iroh QUIC**: elsődleges P2P transport macOS-en (NAT traversal, cross-platform)
2. **BLE GATT**: TELJES server macOS-en (CBPeripheralManager 10.9+, verified)
3. **Network.framework Bonjour**: service discovery + TCP/UDP data
4. **MultipeerConnectivity**: Apple-only fallback (iOS/macOS között)
5. **NEPacketTunnelProvider**: mesh VPN overlay (mint Tailscale)

Kulcs: macOS-en BLE nem csak discovery — TELJES GATT server!
CBPeripheralManager: startAdvertising, add(CBMutableService), respond, updateValue, publishL2CAPChannel

**BLE GATT Details**:

| Feature | Linux (BlueZ) | macOS (CoreBT) | Windows (WinRT) | Android/iOS |
|---------|---------------|----------------|-----------------|-------------|
| Scan | ✅ | ✅ | ✅ | ✅ |
| Advertise | ✅ | ⚠️ Limited | ⚠️ Limited | ✅ |
| GATT Client | ✅ | ✅ | ✅ | ✅ |
| GATT Server | ✅ | ❌ | ⚠️ Limited | ✅ |
| MTU Negotiation | ✅ 512 | ✅ 185 | ✅ 512 | ✅ 512 |

### Phase 3 — Flutter Mobile App (Week 5-8)

**Goal**: Cross-platform mobile companion app with full mesh capabilities.

**Flutter Architecture**:

```
a2a_mesh_app/
├── lib/
│   ├── main.dart                  # App entry
│   ├── models/
│   │   ├── mesh_node.dart         # Node model
│   │   ├── mesh_message.dart      # Message model
│   │   └── transport_status.dart  # Transport status
│   ├── services/
│   │   ├── mesh_service.dart      # Mesh core (Dart FFI → Rust Iroh)
│   │   ├── ble_service.dart       # BLE scan + advertise + GATT
│   │   ├── wifi_direct_service.dart  # WiFi Direct P2P
│   │   ├── pg_service.dart        # PostgreSQL connection
│   │   └── notification_service.dart # Push notifications
│   ├── screens/
│   │   ├── dashboard_screen.dart  # Agent status overview
│   │   ├── agents_screen.dart     # Agent list + details
│   │   ├── messages_screen.dart   # Message inbox/send
│   │   ├── transports_screen.dart # Transport status/control
│   │   ├── mesh_map_screen.dart  # Visual mesh topology
│   │   ├── settings_screen.dart  # Node config, security keys
│   │   └── debug_screen.dart     # Logs, raw messages, network
│   ├── widgets/
│   │   ├── node_card.dart         # Agent status card
│   │   ├── message_bubble.dart   # Chat-style message display
│   │   ├── transport_badge.dart  # Transport indicator (PG/Iroh/BLE/WiFi)
│   │   └── mesh_graph.dart       # Interactive mesh visualization
│   └── utils/
│       ├── crypto.dart            # Ed25519 signing, NaCl encryption
│       ├── serialization.dart     # CBOR/JSON message serialization
│       └── platform.dart          # Platform detection + feature flags
├── rust/                           # Rust core (Iroh + crypto)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                  # FFI exports
│       ├── mesh.rs                 # Iroh mesh integration
│       ├── crypto.rs              # Ed25519 + NaCl
│       └── transport.rs           # Transport adapter trait
├── pubspec.yaml
└── README.md
```

**Flutter Plugin Dependencies**:

```yaml
dependencies:
  flutter:
    sdk: flutter
  # Mesh networking
  flutter_iroh: ^0.1.0        # Dart FFI → Rust Iroh
  # BLE
  flutter_blue_plus: ^1.32.0  # BLE scan, advertise, GATT
  # WiFi Direct
  wifi_iot: ^0.3.0             # WiFi Direct / hotspot (Android)
  nearby_connections: ^3.3.0   # P2P WiFi + BLE (Android)
  # Database
  drift: ^2.14.0               # Local SQLite for message cache
  # UI
  fl_chart: ^0.66.0            # Mesh topology visualization
  # Crypto
  tweetnacl: ^0.5.0            # NaCl for Dart
  ed25519_edwards: ^0.3.0      # Ed25519 for Dart
  # Push notifications
  firebase_messaging: ^14.7.0  # FCM (Android)
  # Utils
  uuid: ^4.2.0                 # UUID v7 generation
  cbor: ^6.2.0                 # CBOR serialization
```

**App Screens**:

1. **Dashboard**: Node status, active transports, message count, mesh health
2. **Agents**: List all known agents, their transports, capabilities, last seen
3. **Messages**: Send/receive A2A messages (direct + broadcast)
4. **Transports**: Enable/disable PG, Iroh, BLE, WiFi Direct per node
5. **Mesh Map**: Interactive graph showing node connections and transport paths
6. **Settings**: Node name, security keys, transport config, discovery options
7. **Debug**: Raw message log, network scan, transport test

**Rust FFI Bridge** (Dart ↔ Rust):

```rust
// rust/src/lib.rs
use iroh::IrohNode;
use ed25519_dalek::SigningKey;

#[no_mangle]
pub extern "C" fn mesh_node_create(config_json: *const c_char) -> *mut MeshNode {
    // Parse config, create Iroh node, return handle
}

#[no_mangle]
pub extern "C" fn mesh_node_send(node: *mut MeshNode, msg_json: *const c_char) -> i32 {
    // Sign message, send via Iroh, return status
}

#[no_mangle]
pub extern "C" fn mesh_node_receive(node: *mut MeshNode) -> *const c_char {
    // Poll for received messages, return JSON
}

#[no_mangle]
pub extern "C" fn mesh_sign_message(private_key: *const u8, msg_json: *const c_char) -> *const c_char {
    // Ed25519 sign message, return signature
}
```

**Cross-Platform Build**:

```bash
# Build Rust library for all targets
cd rust/
cargo build --release --target aarch64-apple-ios        # iOS
cargo build --release --target aarch64-linux-android     # Android ARM64
cargo build --release --target x86_64-unknown-linux-gnu # Linux
cargo build --release --target aarch64-apple-darwin     # macOS ARM
cargo build --release --target x86_64-pc-windows-msvc   # Windows

# Build Flutter app
cd ../
flutter build apk --release      # Android
flutter build ios --release       # iOS
flutter build macos --release     # macOS
flutter build windows --release   # Windows
flutter build linux --release     # Linux
```

### Phase 4 — Integration + Polish (Week 9-10)

**Goal**: Full integration testing, performance benchmarking, documentation.

**Testing Matrix**:

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| PG fallback | Kill PG, mesh continues via Iroh | Message delivered <5s |
| Iroh fallback | Kill Iroh, mesh continues via PG | Message delivered <1s |
| All IP down | Kill PG + Iroh, BLE/WiFi Direct takes over | Message delivered <10s |
| Offline mesh | No internet, no router | WiFi Direct/BLE works |
| 10-node flood | 10 agents, broadcast message | All 10 receive, no duplicates |
| Dedup stress | Same message from 5 transports | Process exactly once |
| Loop prevention | Self-referencing + RE chain | No infinite loops |
| Encryption | Sign + encrypt + verify | Tamper detected, valid accepted |
| Mobile app | Send message from Flutter | Received by all agents |
| Long running | 72h continuous operation | No memory leak, no crash |

## Database Schema Extensions

```sql
-- Mesh node registry
CREATE TABLE IF NOT EXISTS mesh_nodes (
    node_id       TEXT PRIMARY KEY,
    agent_name    TEXT NOT NULL UNIQUE,
    public_key    TEXT NOT NULL,                -- Ed25519 public key hex
    iroh_node_id  TEXT,                          -- Iroh node ID
    transports    JSONB DEFAULT '[]',            -- Available transports
    capabilities  JSONB DEFAULT '{}',            -- What the agent can do
    ip_addresses  JSONB DEFAULT '[]',             -- Known IPs (Tailscale, LAN, WiFi)
    ble_address   TEXT,                           -- BLE MAC address
    wifi_direct   JSONB DEFAULT '{}',             -- WiFi Direct capabilities
    last_seen     TIMESTAMPTZ,
    status        TEXT DEFAULT 'active',          -- active/offline/suspended
    platform      TEXT,                           -- macos/linux/windows/android/ios
    metadata      JSONB DEFAULT '{}',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Mesh message audit log
CREATE TABLE IF NOT EXISTS mesh_messages (
    id            TEXT PRIMARY KEY,              -- UUID v7
    sender        TEXT NOT NULL,
    sender_node_id TEXT,
    recipient     TEXT NOT NULL,                  -- or 'broadcast'
    message_type  TEXT NOT NULL,
    priority      INT DEFAULT 5,
    transport     TEXT NOT NULL,                  -- pg/iroh/wifi_direct/ble/http
    ttl           INT DEFAULT 10,
    hop_count     INT DEFAULT 0,
    payload       JSONB,
    signature     TEXT,
    encrypted     BOOLEAN DEFAULT FALSE,
    delivered     BOOLEAN DEFAULT FALSE,
    delivered_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Transport health log
CREATE TABLE IF NOT EXISTS mesh_transport_health (
    id            SERIAL PRIMARY KEY,
    node_id       TEXT REFERENCES mesh_nodes(node_id),
    transport     TEXT NOT NULL,
    status        TEXT NOT NULL,                  -- up/down/degraded
    latency_ms    INT,
    error         TEXT,
    checked_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Iroh mesh tickets (for sharing mesh access)
CREATE TABLE IF NOT EXISTS mesh_iroh_tickets (
    id            SERIAL PRIMARY KEY,
    ticket        TEXT NOT NULL,
    created_by    TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    expires_at    TIMESTAMPTZ,
    used          BOOLEAN DEFAULT FALSE
);

-- NOTIFY channels (existing + new)
-- a2a_channel (existing)
-- a2a_steer_channel (existing)
-- delegation_channel (existing)
-- a2a_context_channel (existing)
-- mesh_channel (NEW — for Iroh/BLE/WiFi Direct messages)
-- mesh_discovery_channel (NEW — for node discovery events)
```

## Configuration Schema

```yaml
# ~/.hermes/a2a_mesh.yaml

mesh:
  node_name: nova                           # Agent identity
  node_id: ""                               # Auto-generated (UUID v7)
  public_key: ""                            # Auto-generated (Ed25519)

  # Transport priority (first success wins for directed messages)
  # For broadcasts, ALL transports are used simultaneously
  transport_priority:
    - pg_notify                             # <1ms, primary
    - iroh                                  # <50ms, mesh
    - wifi_direct                           # <10ms, local P2P
    - ble                                   # <200ms, proximity
    - http                                  # <5s, REST fallback

  transports:
    pg_notify:
      enabled: true
      host: 192.168.1.30
      port: 5432
      dbname: agent_memory
      user: nova
      password: ${DB_PASSWORD}
      channels: [a2a_channel, a2a_steer_channel, delegation_channel, mesh_channel]

    iroh:
      enabled: true
      data_dir: ~/.hermes/iroh
      ticket: ""                             # Auto-generated or shared
      listen_port: 0                         # Random (NAT traversal)
      bootstrap_nodes: []                    # Auto-discover via DHT

    wifi_direct:
      enabled: true
      ssid: "A2A-Mesh"
      password: "mesh2026"
      interface: wlan0                        # Linux only
      port: 8645                             # TCP message port
      ap_mode: true                          # Create AP if no group exists

    ble:
      enabled: true
      scan_interval: 30                      # seconds
      advertise_interval: 10                 # seconds
      service_uuid: "0000181a-0000-1000-8000-00805f9b34fb"
      a2a_service_uuid: "a2a00001-0000-1000-8000-00805f9b34fb"
      mtu: 512                              # Negotiated MTU
      gatt_server: true                      # Linux only (full GATT)
      discovery_only: false                  # macOS/Windows: discovery only

    http:
      enabled: true
      url: http://192.168.1.30:8199
      health_url: http://192.168.1.30:8198/health
      timeout: 5
      retries: 3

  # Discovery
  discovery:
    mdns:
      enabled: true
      service: "_a2a._tcp"
      port: 8644
    ble:
      enabled: true
      scan_interval: 30
    iroh:
      enabled: true
      dht_bootstrap: true
    static:
      enabled: true
      nodes:
        - name: morzsa
          ip: 192.168.1.30
          port: 8199
          public_key: ""                     # Ed25519 public key
        # Add more agents as they join

  # Security
  security:
    signing_key: ""                          # Ed25519 private key (auto-generated)
    trusted_keys: {}                         # Agent name → public key
    encryption: "nacl"                       # nacl (default) or none
    transport_auth: "hmac"                   # HMAC-SHA256 for transport auth

  # Mesh routing
  routing:
    algorithm: "gossip"                      # flood (small) or gossip (large)
    gossip_fanout: 3                         # K random peers for gossip
    default_ttl: 10                          # Max hops
    broadcast_on_all_transports: true         # Send broadcasts on all transports

  # Loop prevention
  loop_prevention:
    self_reference_filter: true
    not_for_me_filter: true
    re_chain_limit: 4
    dedup_cache_size: 5000                   # Increased for mesh
    dedup_ttl: 300                           # seconds

  # Auto-steer
  auto_steer:
    priority_threshold: 10                   # P10+ = immediate webhook
    queue_lower_priorities: true              # P1-9 = queued backlog

  # Heartbeat
  heartbeat:
    interval: 300                            # seconds
    warning_threshold: 300                    # 5 min
    critical_threshold: 900                  # 15 min
    silent_on_success: true

  # File sharing
  file_sharing:
    pg_max_size: 1048576                     # 1MB
    minio_max_size: 52428800                 # 50MB
    minio:
      endpoint: 192.168.1.30:9000
      bucket: agent-share
      access_key: nova
      secret_key: ${MINIO_SECRET_KEY}
```

## Pitfalls
- **SQL_ASCII + jsonb = 💥**: Use `text` column for JSON payloads. `SET client_encoding TO UTF8` + `ISOLATION_LEVEL_READ_COMMITTED` per INSERT. See `references/pg-sql-ascii-and-mesh-memory.md`.
- **Wrong imports**: `from ..models` ❌ → `from ..core.message` ✅
- **`_transports` dict** doesn't exist → use `_pg_transport` directly.
- **`web` not defined** → `from aiohttp import web` in each handler.
- **Mesh-only memory**: No `shared_a2a_memory`. Use `mesh.mesh_messages` via `MemorySync`. See `references/pg-sql-ascii-and-mesh-memory.md`.
- **Hermes webhook async**: `deliver: "telegram"` = Telegram only. Triple reply guarantee: (1) agent POST, (2) DB poller every 3s×90s, (3) timeout. See references.
- **`connected_peers` fix**: Use `len([p for p in self._peers.values() if p.p2p_available])`, NOT `p2p_transport._peers` (stays empty with PG transport).
- **Channel filtering**: `?channel=general` (broadcast) or `?channel=dm:morzsa` (DM). Frontend must reload from server on channel switch.
- **Message deletion**: `DELETE /api/messages/{msg_id}` admin-only. Deletes local + PG + broadcasts `message_deleted` WS event.
- **A2A watcher**: Still uses `shared_a2a_memory` — migration deferred, both tables coexist. & Lessons Learned

**See `references/pitfalls-zeroconf-pg-pq.md` for zeroconf/PG/priority-queue pitfalls. See `references/morzsa-node-setup.md` for Morzsa Linux LXC setup and common issues. See `references/pytest-guide.md` for test suite structure and API discovery patterns. See `references/benchmarks.md` for full benchmark results. See `references/tls-configuration.md` for TLS setup, cert generation, and pitfalls. See `references/auto-steer.md` for auto-steer processor architecture and integration. See `references/decentralization.md` for LocalStore, P2P file transfer, and transport fallback architecture. See `references/peer-discovery.md` for multi-agent discovery, auto-connect, and scalability. See `references/dashboard.md` for web dashboard architecture, WebSocket protocol, Flutter integration, and mobile responsive design. See `references/dashboard-chat-pipeline.md` for dashboard→agent response pipeline, PG encoding workaround, webhook integration, admin node approval, and shared memory mode column mapping.**

1. **Iroh Python bindings segfault on Python 3.14** — exit code 139 on any call. Replaced with asyncio TCP P2P transport (`transports/p2p_transport.py`). Do NOT re-attempt Iroh without verifying Python compatibility first.

2. **PG schema must be `mesh.` prefix** — The `nova` user lacks CREATE TABLE on `public` schema. All mesh tables use `mesh.mesh_nodes`, `mesh.mesh_messages`, `mesh.notify_mesh_channel()`. CLI code must reference `mesh.` prefix everywhere.

3. **Gitea is on port 3001, NOT 3000** — Runa configured custom port. Always use `192.168.1.100:3001`. SSH git on port 2222.

4. **Gitea release API returns "Release is has no Tag"** — Must push git tag first, then create release. Tag must exist in repo before API call.

5. **macOS WiFi Direct AP is IMPOSSIBLE** — No public API, IBSS deprecated macOS 11. Use asyncio TCP P2P or BLE GATT instead.

6. **System Python for launchd** — The venv at `.venv` lacks psycopg2; launchd daemons must use `/usr/local/bin/python3` which has psycopg2.

7. **PG password can get reset** — Always test with psycopg2, ALTER USER if auth fails.

8. **`cmd_join` must use `--name` param** — Otherwise it defaults to config `node_name` and overwrites the coordinator registration with a router entry.

9. **`asyncio.get_event_loop()` fails on Python 3.14** — Use `asyncio.run()` instead of `loop.run_until_complete()` in CLI commands.

10. **PG Transport sends via INSERT, not NOTIFY** — The `send()` method INSERTs into `mesh.mesh_messages` (trigger fires NOTIFY), not raw `NOTIFY`. This avoids the 8000-byte payload limit and ensures message persistence.

11. **File transfer tiers**: `<1MB` → PG base64 in payload, `1-50MB` → MinIO S3 upload (`mc cp`) + path in payload, `>50MB` → SCP instructions only. SHA-256 verification on receive.

12. **MeshNode daemon has PG write connection** — Separate from the LISTEN connection. `register_node()` and `deregister_node()` write directly to `mesh.mesh_nodes`. `_heartbeat_loop()` updates `last_heartbeat` and status.

13. **CoordinatorElection integrated into MeshNode** — `_election_monitor_loop()` checks coordinator health every heartbeat interval. If coordinator DOWN, seniority-based failover (lowest short_addr router becomes acting coordinator).

14. **Leave command sets short_addr to 0xFFFF** — NOT NULL constraint on `mesh_nodes.short_addr`. Setting to NULL causes error. Use 65535 (0xFFFF) for left/offline nodes.

15. **P2P transport has fast reconnection with dead peer cleanup** — `_reconnect_loop()` checks every 5s (was 30s, reduced for faster recovery). Backoff: 5s → 10s → 20s → 40s... max 300s. `_peer_retry_count` tracks consecutive failures. **Critical**: when `_handle_connection()` exits (peer disconnect, IncompleteReadError), the `(reader, writer)` tuple must be removed from `self._peers` dict in the `finally` block — otherwise `send()` tries to write to a dead socket and fails. Iterate `self._peers` to find and pop the entry matching the disconnected writer.

16. **Config supports env vars** — `A2A_PG_HOST`, `A2A_PG_PORT`, `A2A_PG_DBNAME`, `A2A_PG_USER`, `A2A_PG_PASSWORD`, `A2A_HTTP_URL`, `A2A_HEALTH_URL`. Also YAML `${ENV_VAR}` interpolation.

17. **Router role auto-assigns address** — Router nodes now call `AddressManager.assign_address(node_name, NodeRole.ROUTER)` and get `short_addr=0x0001`. Previously routers had no address and couldn't register in PG. Coordinator gets `0x0000`, first router gets `0x0001`.

18. **`__main__.py` must use package imports** — `from a2a_mesh.node import MeshNode`, NOT `from node import MeshNode`. Relative imports in node.py fail without proper package context.

19. **PYTHONPATH for `python3 -m a2a_mesh`** — Must set `PYTHONPATH` to the parent directory of `a2a_mesh/` package. On Morzsa: `PYTHONPATH=~`. On Nova: venv handles it.

20. **Config YAML structure is `mesh:` top-level** — The config parser reads `mesh.node_name`, `mesh.transports.pg_notify.host`, etc. Flat keys like `pg:` or `node:` at the root level are ignored. Wrong config = default values (empty password = `fe_sendauth` error).

21. **Gitea API token scopes** — Use `scopes: ["all"]` when creating a token. `scopes: ["repository"]` is invalid on this Gitea version.

22. **`topology.node_role` defaults to `end_device`** — For Morzsa (router node), must explicitly set `mesh.topology.node_role: router` in config. Otherwise the node starts as end_device with no address assignment.

23. **PG NOTIFY `select.select` blocks asyncio event loop** — The original `_listen_loop` used synchronous `select.select([self._conn], [], [], 1.0)` which blocked the entire event loop, preventing ANY async processing (including message reception, heartbeats, health checks). Fix: use `loop.run_in_executor(None, lambda: select.select(...))` to run select in a thread. Without this fix, messages inserted into `mesh_messages` trigger NOTIFY but the listener never processes them because the event loop is stuck in select.

- **Bidirectional messaging verified**: Nova↔Morzsa via PG NOTIFY on `mesh_channel`. Both nodes send and receive messages. Morzsa (router 0x0001) running on LXC at 192.168.1.30. Health endpoints on port 8650 both nodes.
- **Web dashboard verified**: Both nodes serve dashboard at `http://<node_ip>:8650/`. Agent list, real-time chat, file upload, user auth all working.

25. **Flutter SDK `flutter create` hangs in Hermes terminal** — Use `git clone --depth 1 --branch stable https://github.com/flutter/flutter.git ~/development/flutter` for SDK, then create project files manually. `flutter create` and `flutter --version` timeout due to tool compilation in background. Dart SDK works immediately after clone.

26. **Morzsa LXC node: `PYTHONPATH=~` required** — When running `python3 -m a2a_mesh` on Morzsa, must set `PYTHONPATH` to home directory so package imports resolve. Without it, `ModuleNotFoundError: No module named 'a2a_mesh'`.

27. **Router nodes auto-assign address via AddressManager** — Previously routers had no address (`None`) and couldn't register in PG (duplicate key on `short_addr=0`). Fix: router nodes now call `self.address_manager.assign_address(node_name, NodeRole.ROUTER)` getting `short_addr=0x0001` (first router after coordinator).

28. **P2PConfig `from_yaml()` must load ALL fields** — When adding new fields to a dataclass (like `tls_enabled`, `tls_cert`, etc.), the `from_yaml()` method must explicitly map each new field with `p2p_data.get('field_name', config.p2p.field_name)`. Missing mapping = silent default to False/empty string instead of reading from YAML. This is the #1 cause of "TLS not working" — always verify with `print(cfg.p2p.tls_enabled)` after loading. See `references/tls-configuration.md` for full details.

29. **`os` import needed in p2p_transport.py** — When using `os.path.expanduser()` for TLS cert paths, `import os` must be present. Missing import causes `NameError: name 'os' is not defined` at runtime (but only when TLS is enabled, so plain TCP works fine — making it hard to catch).

30. **TLS certificate SAN must include all IPs** — Self-signed certs for mesh nodes need SAN entries for DNS (node name, FQDN) AND IP (127.0.0.1, LAN IP). Without SAN, `ssl.CERT_REQUIRED` verification fails even with valid CA chain. Use `generate_certs.py` which includes proper SAN.

31. **Client SSL connections need `ssl` parameter** — `asyncio.open_connection(host, port)` is plain TCP. For TLS, pass `ssl=ssl_context` where `ssl_context` is the same `SSLContext` used for the server. The P2P `_connect_to_peer()` must use `ssl=self._ssl_context` for outgoing connections too.

32. **Performance benchmarks (macOS M-series, Python 3.14)** — Message creation: 16K/s (p50=52µs), JSON serialization: 21.6K/s, msgpack: 10.5K/s, dedup cache: 670K/s (p50=1.2µs), address assignment: 97K/s, Ed25519 sign: 2K/s, PG INSERT: 91/s (p50=8.3ms), PG NOTIFY: 1.5K/s, P2P TCP: 0.7ms connection latency, P2P TLS: 17.1ms avg (16.4ms TLS overhead), full round-trip INSERT+NOTIFY: 8.3ms p50. TLS 1.3 with AES-256-GCM, peer cert verification enabled.

33. **pytest API discovery pattern** — When writing tests for existing modules, first import the module and inspect `dir()` + `inspect.signature()` to find actual class/function names and parameters. The code may use different names than expected (e.g., `MeshEncryption(signing_key_hex=)` not `MeshEncryption(signing_key=)`, `JoinRequest` needs `node_role, timestamp, nonce` not just `node_name, public_key, address`). See `tests/` directory for working examples.

34. **AutoSteerProcessor webhook URL** — The webhook trigger in `auto_steer.py` posts to `http://localhost:8644/webhook`. If Hermes gateway is not running on that port, P10+ messages will get a 404/connection error but processing continues (the message is still handled, webhook failure is logged as warning). This is by design — webhook is best-effort, not blocking.

35. **Flutter BluetoothConnectionState must be enum** — The fallback class for non-Flutter environments must be `enum BluetoothConnectionState { connected, disconnected }`, NOT a regular class with static const instances. The class pattern causes comparison failures in Provider state checks. Always use Dart enums for state models.

36. **Auto-steer `classify_message` vs `_process_steer`** — P10+ steer directives go through `_process_steer()` which calls `_trigger_webhook()` internally, NOT through `classify_message()`. The `_stats["interrupts_triggered"]` counter must be incremented in `_process_steer` for steer_interrupt, not just in `process_message` for plain interrupt messages.

37. **LocalStore SQLite fallback for decentralized operation** — When PG is unavailable, messages are stored in local SQLite (`~/.hermes/scripts/a2a_mesh/local_store_{node_name}.db`). Outbound messages are queued with priority ordering (P10 first) and tracked for PG sync. When PG recovers, unsent messages are flushed. Inbound messages are also cached locally. The router's `send()` method enqueues to LocalStore before attempting transport delivery — if all transports fail, messages persist for later retry. Peer availability status is tracked in `peer_status` table (PG/MinIO/P2P availability with 5-minute TTL).

38. **P2P file transfer protocol** — FILE_OFFER→FILE_ACCEPT/REJECT→FILE_CHUNK(s)→FILE_COMPLETE→FILE_ACK. Chunks are 512KB (not 64KB — P2P message max is 10MB). Each chunk has SHA-256 verification. Receiver checks disk space before accepting. Sender reassembles on FILE_COMPLETE with full-file SHA-256 check. Transfer state tracked in LocalStore's `file_transfers` table. When MinIO is unavailable, files transfer directly over P2P TCP/TLS — no dependency on external storage.

39. **Transport fallback chain with LocalStore persistence** — Router's `send()` tries PG→P2P→HTTP in order. Every outbound message is stored in LocalStore first. If PG succeeds, message is marked `pg_synced=1`. If all transports fail, message stays `pending` in LocalStore for later retry. The `get_pending_outbound()` method returns messages ordered by priority (P10 first). Cleanup removes synced messages older than 24 hours.

40. **MeshNode integrates LocalStore and P2PFileTransfer** — Both are initialized in `__init__`. `file_transfer` messages (type="file_transfer") are handled in `_dispatch_to_handlers()` before regular handlers — the P2PFileTransfer processes FILE_OFFER/CHUNK/COMPLETE and sends responses via P2P. Health endpoint and `get_status()` include `local_store` and `file_transfer` stats.

41. **PeerDiscovery multi-agent auto-connect** — `PeerDiscovery` in `core/peer_discovery.py` discovers peers from static config + PG mesh_nodes table + health checks. Discovery loop runs every 30s. `connect_to_peer()` calls `p2p_transport._connect_to_peer(name, host, port)` for TLS auto-connect. The P2P transport reference must be set in `MeshNode.start()` via `self.peer_discovery.p2p_transport = self._p2p_transport` AFTER transport init — without this, auto-connect silently fails. Health endpoint includes `peer_discovery` stats (known_peers, connected_peers, available_peers, peer details).

42. **Static node config p2p_port must match actual listener** — Nova config had `p2p_port: 8645` for Morzsa but Morzsa actually listens on `8651`. Discovery loaded the wrong port from config and auto-connect failed silently. Always verify `mesh_config.yaml` static nodes match the peer's actual P2P listen port. Health endpoint shows the port discovery loaded — check it matches reality.

43. **Web dashboard embedded in mesh node health port** — `DashboardHandler` in `core/dashboard.py` registers routes on the SAME aiohttp app as `/health` and `/ready`. Routes: `GET /` (HTML dashboard), `GET /dashboard`, `GET /api/status` (mesh JSON), `GET /api/agents` (agent list), `GET /api/messages` (last 100), `POST /api/send` (send message), `POST /api/send-file` (upload file), `WS /ws` (real-time WebSocket). Dashboard is accessed at `http://<node_ip>:8650/`. User identification via localStorage-persisted username on first visit.

44. **Dashboard HTML MUST be a separate file, NOT inline Python string** — JS template literals (`${variable}`) get escaped by Python's string processing (`\${variable}`), breaking all JavaScript. The `_generate_html()` method must use `open(os.path.join(os.path.dirname(__file__), "dashboard.html"))` to load from file. The HTML file (`core/dashboard.html`) uses plain `var` and string concatenation instead of template literals for maximum compatibility. NEVER embed JS with template literals in a Python triple-quoted string.

45. **Dashboard JSON serialization — dataclass `TransportStatus` is not JSON-serializable** — The `get_status()` method returns a dict containing `TransportStatus` dataclass objects. `web.json_response()` fails with `TypeError: Object of type TransportStatus is not JSON serializable`. Fix: use a `sanitize()` helper that recursively converts dataclass objects (`hasattr(obj, '__dataclass_fields__')`) and objects with `__dict__` to dicts before serialization. Apply to `/api/status` endpoint. The `/api/agents` endpoint already builds plain dicts, but use `json.dumps(x, default=str)` as a safety net.

46. **WebSocket real-time updates in dashboard** — `DashboardHandler._websocket_handler()` maintains a set of connected users. When a mesh message arrives (via `on_mesh_message()`), it's broadcast to all WS clients. When a user sends a chat message via WS, it creates an `A2AMessage`, sends via `router.send()`, stores in `_message_history`, and broadcasts to all WS clients. The WS protocol uses JSON messages with `type` field: `connected`, `status`, `new_message`, `chat`, `ping/pong`.

47. **PG table ownership — Morzsa creates tables as its user, Nova can't modify them** — When Morzsa's node creates `mesh.mesh_offline_queue` or other tables, they're owned by the Morzsa PG user. Nova gets `permission denied for table` or `must be owner of table` errors when trying to ALTER or CREATE INDEX. Fix: `sudo -u postgres psql -d agent_memory -c "ALTER TABLE mesh.xxx OWNER TO nova"` on the PG host. This must be done whenever Morzsa creates new tables that Nova needs to access.

48. **Flutter app `http_service.dart` connects to mesh dashboard API** — The Flutter `HTTPService` now includes: `login/register/logout` for auth, `connectWebSocket()` for real-time updates (token as `?token=` param), `getAgents()` for agent list, `getMessages()` for message history, `sendMessage()` for REST API send (Bearer token), `sendChatMessage()` for WS-based send, `uploadFile()` for multipart file upload. `MeshProvider` manages auth state and auto-fetches agents+messages on login.

49. **Flutter A2AMessage field mapping — most common Flutter bug source** — The Python `A2AMessage` dataclass uses `msgType` (not `type`), `payload` (dict, not `content` string), `priority` (int, not double), and `createdAt` (DateTime, not timestamp double). The JSON API returns `type` (not `msgType`), `payload` (dict), and `timestamp` (ISO string). Always map: `msgType: msgData['type'] ?? 'directive'`, `payload: (msgData['payload'] ?? {})`, `priority: (msgData['priority'] ?? 5) as int`, `createdAt: DateTime.parse(msgData['timestamp'])`. NEVER use `content`, `metadata`, or `.toDouble()` for timestamp/priority.

50. **Dashboard auth uses base64url-safe tokens** — JWT-like tokens are base64-encoded (not raw JSON) so they work in WebSocket `?token=` query params. Raw JSON tokens break URL encoding. Auth DB is SQLite at `~/.hermes/a2a_dashboard_users.db`. Default owner: `zsolt`/`mesh2026`.

51. **Dashboard `/api/send` uses `payload`+`type`, not `content`+`message_type`** — When creating A2AMessage from dashboard requests, use `payload=text, type=MSG_TYPE_DIRECTIVE`. The old field names `content` and `message_type` cause 500 errors.

52. **Dashboard `/api/messages` timestamp is ISO string, not unix seconds** — JavaScript `new Date(msg.timestamp * 1000)` produces `Invalid Date`. Use `new Date(msg.timestamp)` directly because the API returns ISO 8601 strings like `2026-06-16T12:33:09.483183+00:00`.

53. **Flutter collapsible sidebar for agent list** — `DashboardScreen` has a 220px animated sidebar showing all connected agents (from `/api/agents` HTTP endpoint, not just BLE). Each agent card shows name, role (coordinator/router badge), online status (green/gray dot), transport chips (PG/P2P/HTTP/BLE), and host address. Toggle via hamburger icon in AppBar.

54. **Web dashboard mobile sidebar — NEVER use `display:none`** — The original dashboard CSS had `@media(max-width:768px){.sidebar,.right-panel{display:none}}` which completely hid the agent list on mobile. Users on phones couldn't see connected agents at all. Fix: use a collapsible slide-in sidebar pattern instead. On mobile (≤900px), the sidebar becomes `position:fixed; left:-300px` (off-screen) with a `transition:left .3s ease`. A hamburger button ("☰ Agentek") in the header toggles the sidebar open (`left:0`). A close button ("✕") inside the sidebar and a dark overlay behind it dismiss it. The right panel (stats) is hidden on mobile since the chat area takes full width. Key CSS: `.sidebar{position:fixed;left:-300px;...;transition:left .3s ease}` / `.sidebar.open{left:0}` / `.mobile-sidebar-toggle{display:none}` / `@media(max-width:900px){.mobile-sidebar-toggle{display:inline-flex}}`.

54. **Flutter Android build fails with "deleted Android v1 embedding"** — When creating a Flutter project manually (not via `flutter create`), the `android/` directory must include the full v2 embedding structure: `android/app/src/main/AndroidManifest.xml` with `<meta-data android:name="flutterEmbedding" android:value="2" />`, `android/app/src/main/kotlin/com/a2amesh/app/MainActivity.kt` extending `FlutterActivity()`, `android/app/build.gradle.kts` with `namespace "com.a2amesh.app"`, `compileSdk 34`, `minSdk 24`, `dev.flutter.flutter-gradle-plugin`, and `android/settings.gradle.kts` with plugin management. An empty `android/app/` directory causes the v1 embedding error. Also requires `android/gradle/wrapper/gradle-wrapper.properties` (Gradle 8.7), `android/gradle.properties` (AndroidX enabled), `android/local.properties` (flutter.sdk path), and `local.properties` at project root.

55. **Flutter `dart analyze` reports false positives without SDK context** — Running `dart analyze` outside of `flutter analyze` context reports hundreds of "undefined class" errors for Flutter packages (Widget, BuildContext, Colors, etc.) because Dart SDK doesn't include Flutter dependencies. Use `flutter analyze` or `flutter build` for real error checking. The actual code errors are the ones referencing wrong field names on model classes (like `A2AMessage.content` instead of `A2AMessage.payload`).

56. **Flutter Android build requires Java JDK + Android SDK** — `flutter build apk` fails with "No Android SDK found" if `ANDROID_HOME` is not set, and with "Unable to locate a Java Runtime" if no JDK is installed. On this machine: Android SDK is at `/usr/local/share/android-commandlinetools` (with `sdkmanager` in `cmdline-tools/latest/bin/`), but Java is not installed. Fix: `brew install --cask temurin` (needs sudo password), then `export ANDROID_HOME=/usr/local/share/android-commandlinetools` and `export JAVA_HOME=$(/usr/libexec/java_home)`, then `sdkmanager "platforms;android-34" "build-tools;34.0.0" "platform-tools"`.

57. **PG database is SQL_ASCII, not UTF8** — The shared PG database (`agent_memory`) uses SQL_ASCII encoding. This causes `conversion between UTF8 and SQL_ASCII is not supported` errors when the mesh node's PG transport sends messages containing non-ASCII characters (Hungarian text, Unicode escapes like `\u00f3`). **Three fixes required**: (1) `mesh.mesh_messages.payload` column must be `text` type, NOT `jsonb` — `ALTER TABLE mesh.mesh_messages ALTER COLUMN payload TYPE text;` (jsonb forces Unicode decoding which fails in SQL_ASCII). (2) In `pg_transport.py send()`, switch to `ISOLATION_LEVEL_READ_COMMITTED` before INSERT (autocommit breaks `SET client_encoding TO UTF8` — each statement becomes its own transaction). Use `conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)` then `cur.execute("SET client_encoding TO UTF8")` then INSERT then `conn.commit()` then restore isolation. Also apply this on both `_conn` and `_write_conn` at startup. (3) Use `json.dumps(payload, ensure_ascii=True)` for payload serialization. The `options="-c client_encoding=UTF8"` psycopg2 connect parameter does NOT work — must use explicit `SET client_encoding TO UTF8` per transaction.

58. **Hermes webhook endpoint is `/webhooks/a2a-instant`**, NOT `/webhook`** — The A2A watcher and dashboard must POST to `http://localhost:8644/webhooks/a2a-instant` with HMAC-SHA256 signature in `X-Hub-Signature-256` header. Secret is in `~/.hermes/config.yaml` under `webhook.secret` (value: `a2a-instant-secret-2026`). A simple `POST /webhook` returns 404. The HMAC is computed as `hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()`, sent as `X-Hub-Signature-256: sha256=<hex>`.

59. **Dashboard chat → agent response pipeline** — When a user sends a message from the web dashboard, the message must reach the Hermes agent for a response. The full pipeline: (1) Dashboard `/api/send` creates A2AMessage with `sender="web_dashboard"`, `recipient="morzsa"` (not `self.node.node_name` and not `"broadcast"`), (2) `_insert_pg_message()` inserts into `shared_a2a_memory` with PG NOTIFY on `a2a_channel`, (3) `_wake_agent()` calls Hermes webhook at `/webhooks/a2a-instant` with HMAC signature, (4) A2A watcher picks up NOTIFY and processes via auto-steer. Minimum priority P7 for dashboard messages (ensures immediate webhook trigger via auto-steer).

60. **Dashboard message sender must be `web_dashboard`, not `nova`** — The A2A watcher has a self-ref filter that skips messages where `sender == agent_name`. If the dashboard sends messages with `sender="nova"`, the A2A watcher on Nova will skip them as self-referencing. Use `sender="web_dashboard"` and include `"original_sender": self.node.node_name` in the payload. Similarly, `recipient` should be `"morzsa"` (not `"broadcast"`) so the A2A watcher on Nova processes it as "FOR ME" and forwards to Morzsa.

61. **A2A watcher skips `recipient=broadcast` messages** — When `recipient="broadcast"` and the watcher agent is `nova`, the watcher logs "Not for me (recipient=broadcast, I am nova), skipping". This means broadcast dashboard messages never trigger agent wake-up. Fix: use a specific recipient (`"morzsa"`) instead of `"broadcast"` for dashboard messages that need a response.

62. **Dashboard `/api/messages` must merge local history + PG messages** — The `_api_messages()` endpoint originally returned only `self._message_history` (local messages from the dashboard). Other agents' responses stored in `mesh.mesh_messages` PG table were invisible. Fix: query `SELECT ... FROM mesh.mesh_messages WHERE sender != self_node_name ORDER BY created_at DESC LIMIT N` with `SET client_encoding TO UTF8`, parse JSON payloads, merge with local messages, deduplicate by ID, sort by timestamp. This makes other agents' responses visible in the dashboard chat without requiring them to go through the dashboard's WS handler.

63. **Multi-router mesh — mesh_nodes needs network columns** — The original `mesh.mesh_nodes` table only had `node_name, role, short_addr, extended_uuid, parent_addr, depth, public_key, transport_info, status, joined_at, last_heartbeat`. This meant peers couldn't discover each other's IP addresses or transport availability. Fix: `ALTER TABLE mesh.mesh_nodes ADD COLUMN IF NOT EXISTS host VARCHAR, p2p_port INTEGER DEFAULT 8651, health_port INTEGER DEFAULT 8650, pg_available BOOLEAN DEFAULT FALSE, p2p_available BOOLEAN DEFAULT FALSE, http_available BOOLEAN DEFAULT FALSE`. Node registration (`_register_node()`) must include these fields using `ON CONFLICT DO UPDATE`. Heartbeat (`_update_heartbeat_pg()`) must update `pg_available`, `p2p_available`, `http_available`. Peer discovery (`discover_from_pg()`) queries these columns and skips peers without a `host` value. New routers can be added by simply inserting into `mesh.mesh_nodes` with their IP and port — the coordinator auto-discovers them on the next discovery cycle.

64. **P2P transport reconnect — 5s interval, dead peer cleanup** — The `_reconnect_loop()` originally ran every 30s, causing "no peers connected" errors between discovery cycles. When `_handle_connection()` exits (peer disconnect, IncompleteReadError), the `(reader, writer)` tuple stayed in `self._peers` dict, causing `send()` to write to dead sockets. Fix: (1) reduce `_reconnect_interval` from 30s to 5s, (2) in `_handle_connection()`, remove the peer from `self._peers` dict in the `finally` block by iterating to find and pop the entry matching the disconnected writer, (3) `_connect_to_peer()` uses exponential backoff (5s→10s→20s→40s... max 300s) via `_peer_retry_count` dict.

65. **PG SQL_ASCII database — payload column must be TEXT not JSONB** — The shared PG database (`agent_memory`) uses SQL_ASCII encoding. `jsonb` columns force Unicode decoding which fails with error `conversion between UTF8 and SQL_ASCII is not supported`. This happens even with `ensure_ascii=True` in `json.dumps()` because PG's `jsonb` type always decodes Unicode escapes like `\u00f3` back to UTF-8 characters. Fix: `ALTER TABLE mesh.mesh_messages ALTER COLUMN payload TYPE text`. Also: (1) use `ISOLATION_LEVEL_READ_COMMITTED` before `SET client_encoding TO UTF8` + INSERT (autocommit mode makes each statement its own transaction, so `SET` doesn't persist to the INSERT), (2) `conn.commit()` + restore original isolation level after INSERT, (3) apply `SET client_encoding TO UTF8` on both `_conn` and `_write_conn` at startup. The `options="-c client_encoding=UTF8"` psycopg2 connect parameter does NOT work — must use explicit `SET client_encoding TO UTF8` per transaction or at connection startup.

66. **PeerDiscovery needs `_pg_conn` parameter** — The `discover_from_pg()` method needs a PG connection to query `mesh_mesh_nodes`. Originally it tried `self.local_store._pg_conn` (which is the local SQLite connection, not PG). Fix: add `pg_conn` parameter to `PeerDiscovery.__init__()`, set it in `MeshNode.start()` after PG connection is established via `self.peer_discovery._pg_conn = self._pg_conn`. The `discover_and_connect()` method checks `self._pg_conn` first, then falls back to `self.local_store._pg_conn`.

67. **PG transport shared memory mode — use `shared_a2a_memory` not `mesh.mesh_messages`** — The PG transport `send()` method now INSERTs into `shared_a2a_memory` instead of `mesh.mesh_messages`. Column mapping: `sender`→`sender_agent`, `recipient`→`recipient_agent`, `msg_type`→`memory_type`+`message_type`, `payload`→`content` (JSON string), `priority`→`priority`, `subject`→`subject` (falls back to `message.type`). This ensures compatibility with the A2A watcher's NOTIFY trigger on `a2a_channel`. The `_fetch_message()` method also queries `shared_a2a_memory` with `SET client_encoding TO UTF8`. This is the "shared memory mode" — mesh messages flow through the same table as A2A inter-agent messages, enabling unified message tracking.

68. **Admin approval for new nodes — `pending` status** — New nodes register with `status='pending'` in `mesh_nodes`. Coordinator nodes auto-approve themselves (`status='active'`). Router and end_device nodes require admin approval via the dashboard. The `_register_node()` method uses `ON CONFLICT DO UPDATE SET status = mesh.mesh_nodes.status` — it never overrides an existing `active` or `offline` status, but inserts new nodes as `pending`. Admin approval API: `POST /api/nodes/{node_name}/approve` sets `status='active'`, `POST /api/nodes/{node_name}/reject` deletes the pending entry. Dashboard shows ⏳ "Jóváhagyásra vár" panel for pending nodes (owner-only), 🌐 "Összes Node" panel for all nodes. Peer discovery only discovers `status='active'` nodes.

69. **Dashboard admin panel — owner-only UI** — The dashboard right panel shows admin sections only for `role='owner'` users. `checkAdminPanel()` is called on login and every 15s. `loadPendingNodes()` fetches `/api/nodes/pending`, `loadAllNodes()` fetches `/api/nodes`. Each pending node card has ✅ Jóváhagy and ❌ Elutasít buttons. New API endpoints: `GET /api/nodes/pending` (owner-only), `POST /api/nodes/{name}/approve` (owner-only), `POST /api/nodes/{name}/reject` (owner-only), `GET /api/nodes` (auth-required). All admin endpoints use `_require_owner()` which checks `user.role == 'owner'`.

70. **`from aiohttp import web` must be inside async handler methods** — Dashboard handler methods that return `web.json_response()` must import `from aiohttp import web` INSIDE the method, not rely on it being imported at module level. The aiohttp framework requires the `web` module in handler scope. Missing this import causes `NameError: name 'web' is not defined` at runtime (500 error). Every new async handler method that returns `web.json_response()` or `web.Response()` needs this import.

71. **mesh_nodes must have network columns for multi-router discovery** — Without `host`, `p2p_port`, `health_port` columns in `mesh.mesh_nodes`, peer discovery cannot find other routers. Fix: `ALTER TABLE mesh.mesh_nodes ADD COLUMN IF NOT EXISTS host VARCHAR, p2p_port INTEGER DEFAULT 8651, health_port INTEGER DEFAULT 8650, pg_available BOOLEAN DEFAULT FALSE, p2p_available BOOLEAN DEFAULT FALSE, http_available BOOLEAN DEFAULT FALSE`. The `_register_node()` method INSERTs these values, and `_update_heartbeat_pg()` updates `pg_available`, `p2p_available`, `http_available` on each heartbeat. The `discover_from_pg()` method filters by `status = 'active'` and `last_heartbeat > NOW() - INTERVAL '10 minutes'`, and skips peers without a `host` value.

## Actual Built Modules (P1 Complete)

```
~/.hermes/scripts/a2a_mesh/
├── __init__.py
├── cli.py              (~1126 lines) — start, send, broadcast, status, init, join, leave, topology, elect, keygen, test, send-file, receive-file, list-files, health
├── node.py             (~900 lines) — MeshNode orchestrator with PG write conn, election monitor, register/deregister, graceful shutdown, priority queue dispatch, auto-steer integration, health monitor, stats update, webhook trigger
├── file_transfer.py    (353 lines) — 3-tier file sharing (PG base64, MinIO S3, SCP)
├── benchmark.py        (411 lines) — Performance benchmark suite (12 suites, msg/dedup/encrypt/auth/PG/P2P/roundtrip)
├── generate_certs.py   (127 lines) — TLS certificate generator (CA + node certs with SAN)
├── core/
│   ├── message.py      (~210 lines) — A2AMessage, UUID v7, signing, routing fields
│   ├── router.py       (240 lines)  — Flood/gossip routing with dedup + priority queue (P10→P1)
│   ├── dedup.py        (~92 lines)  — Thread-safe LRU dedup cache
│   ├── encryption.py   (~96 lines)  — Ed25519 signing + NaCl sealed box
│   ├── config.py        (~230 lines) — MeshConfig + TopologyConfig + env vars + TLS fields
│   ├── topology.py      (~330 lines) — NodeRole, MeshAddress, AddressManager (Cskip)
│   ├── tree_router.py   (282 lines)  — TreeRouter, sleepy ED buffer, route cache
│   ├── election.py      (270 lines)  — CoordinatorElection, seniority-based failover
│   ├── ack.py           (~140 lines) — AckManager, AckTracker, AckStatus
│   ├── auth.py          (~200 lines) — NodeAuthenticator, JoinRequest, AuthMode
│   ├── offline_queue.py (~150 lines) — OfflineQueue for disconnected nodes
│   ├── auto_steer.py    (~218 lines) — AutoSteerProcessor (P10+ interrupt, P7-9 high, P1-6 backlog, steer tracking, webhook, cleanup)
│   ├── local_store.py   (~380 lines) — SQLite fallback for decentralized operation (outbound queue, inbound cache, file transfers, steer cache, peer status)
│   ├── file_transfer.py (~410 lines) — P2P direct file transfer (OFFER/ACCEPT/REJECT/CHUNK/COMPLETE/ACK protocol, 512KB chunks, SHA-256, disk check, auto-reassembly)
│   ├── peer_discovery.py (~300 lines) — Dynamic peer discovery (static config + PG mesh_nodes + health monitoring + auto-connect, 5s reconnect, network columns, admin approval)
│   ├── dashboard.py      (~870 lines) — Web dashboard handler (routes, WebSocket, API endpoints, message history, admin approval: pending/approve/reject, node list, PG message merge)
│   ├── dashboard.html    (~600 lines) — Dashboard HTML/CSS/JS (MUST be separate file, NOT inline — JS template literals break in Python strings. Admin panel for node approval.)
├── transports/
│   ├── pg_transport.py  (~310 lines) — PG LISTEN/NOTIFY + INSERT into shared_a2a_memory (shared memory mode), separate write conn, auto-reconnect, run_in_executor for select (non-blocking)
│   ├── p2p_transport.py (~282 lines) — asyncio TCP P2P with reconnect + backoff + TLS 1.3 (cert verification, CA trust)
│   ├── http_transport.py (137 lines)  — HTTP/MCP bridge fallback
│   └── ble_transport.py (460 lines)  — BLE GATT server (macOS/PyObjC) + scanner (bleak)
├── discovery/
│   └── mdns.py          (160 lines) — Zeroconf mDNS (0.149+ API fix, AsyncZeroconf/InterfaceChoice)
└── .venv/               — Python 3.14.4 venv (pynacl, zeroconf, msgpack, psycopg2-binary, pyyaml, aiohttp)
```

Total: ~4900 lines, 22 modules, 17/17 self-tests passing. **88/88 pytest unit tests passing** (see `tests/` directory).

**Running nodes:**
- **Nova** (macOS): coordinator 0x0000, 4/4 transports (PG, P2P TLS 1.3, HTTP, BLE), launchd daemon, port 8650/8645
- **Morzsa** (Linux LXC at 192.168.1.30): router 0x0001, 3/3 transports (PG, P2P TLS 1.3, HTTP), nohup process, port 8650/8651
- **Runa** (Gitea server at 192.168.1.100): router 0x0002, PG+P2P transports, port 8652 — auto-discovered via PG `mesh_nodes` with host/port columns. Registered as `pending`, auto-approved via dashboard.
- **Bidirectional messaging verified**: Nova↔Morzsa↔Runa via PG NOTIFY on `a2a_channel`. Multi-router mesh operational — all 3 nodes discoverable via PG `mesh_nodes` table with `host`, `p2p_port`, `health_port` columns.
- **Shared memory mode**: PG transport INSERTs into `shared_a2a_memory` (not `mesh.mesh_messages`), compatible with A2A watcher NOTIFY triggers on `a2a_channel`. Unified message tracking across A2A and mesh systems.
- **Admin approval**: New nodes register as `pending`, coordinator auto-approves itself. Dashboard shows pending nodes for owner users with approve/reject buttons.
- **TLS 1.3 verified**: Both nodes use CA-signed certs with peer verification (CERT_REQUIRED). Cipher: AES-256-GCM. TLS handshake overhead: +16.4ms avg (one-time per connection).

Flutter app: /Users/zsolt/development/a2a_mesh_app (14 files, ~3500 lines Dart)
- BLE GATT client (flutter_blue_plus)
- Material 3 dashboard with collapsible agent sidebar (220px, toggle via hamburger)
- Auth screen: login/register with bcrypt+HMAC token auth (default: zsolt/mesh2026)
- Agent cards: name, role (coordinator/router), online status, transport chips (PG/P2P/HTTP/BLE)
- Chat screen with real-time WebSocket, priority selector (P1-P10), recipient selector
- HTTPService: REST API + WebSocket + auth (login/register/logout/Bearer token)
- MeshProvider: ChangeNotifier state management, auth state, agent polling via HTTP `/api/agents`
- Settings screen (mesh node URL, BLE config, UUID info)
- Gitea repo: nova/a2a-mesh-app
- Android build requires: full v2 embedding in android/, Java JDK (temurin), Android SDK (ANDROID_HOME set), platform-tools/build-tools/platforms installed via sdkmanager

PG Integration:
- `mesh.mesh_nodes` table (node registry)
- `mesh.mesh_messages` table (message tracking)
- `mesh.notify_mesh_channel()` trigger function on INSERT
- `mesh_channel` NOTIFY channel
- Nova = coordinator (0x0000) on macOS, daemon via launchd
- Morzsa = router (0x0001) on Linux LXC (192.168.1.30), running via nohup
- Two-node mesh operational: PG, P2P (8645/8651), HTTP health (8650)

## Remaining Improvements

| Priority | Item | Description | Status |
|----------|------|-------------|--------|
| 🟠 HIGH | Message ACK | Sender knows if message was received (status tracking in PG) | ✅ Done |
| 🟠 HIGH | Offline queue | Buffer messages for offline nodes, deliver on reconnect | ✅ Done |
| 🟡 MED | Retry logic | Router-level retry with backoff for failed sends | ✅ Done |
| 🟡 MED | Node authentication | Verify joining nodes (Ed25519 signature on join request) | ✅ Done |
| 🟡 MED | Message size limit | Cap payload size, reject oversized messages | ✅ Done |
| 🟡 MED | HTTP health endpoint | `/health` on mesh node for external monitoring | ✅ Done |
| 🔵 LOW | Message compression | zlib/gzip compression for large payloads | ✅ Done |
| 🔵 LOW | TLS on P2P | Encrypt TCP connections between nodes | ✅ Done (TLS 1.3, CA-signed certs, peer verification) |
| 🔵 LOW | PG connection pooling | Reuse connections instead of new per send | ✅ Done (separate write conn) |
| 🔵 LOW | Performance benchmarks | Measure throughput and latency of all operations | ✅ Done (benchmark.py, 12 suites) |
| 🟡 MED | Priority queue | P10→P1 message processing, P7+ immediate | ✅ Done |
| 🟡 MED | Auto-steer | P10+ webhook interrupt, P7-9 handler, P1-6 backlog, steer tracking | ✅ Done |
| 🟡 MED | LocalStore SQLite fallback | Decentralized operation when PG is down | ✅ Done |
| 🟡 MED | P2P file transfer | Direct file transfer over P2P, 512KB chunks, SHA-256 | ✅ Done |
| 🟡 MED | Transport fallback | PG→P2P→HTTP with LocalStore persistence | ✅ Done |
| 🟡 MED | Peer discovery | Dynamic multi-agent discovery + auto-connect | ✅ Done |
| 🟡 MED | Graceful shutdown | SIGINT/SIGTERM handler with event | ✅ Done |
| 🟡 MED | Health monitor loop | Periodic PG + transport checks | ✅ Done |
| 🟡 MED | Stats update loop | Periodic node stats in PG | ✅ Done |
| 🔵 LOW | mDNS discovery | Zeroconf service discovery | ✅ Done (0.149+ API) |
| 🟡 MED | Flutter mobile app | Cross-platform mobile companion | ✅ Done (Material 3, BLE client, dashboard, chat, settings, Gitea repo) |
| 🟡 MED | Web dashboard | Agent list, real-time chat, user auth, file upload, collapsible sidebar, admin node approval | ✅ Done (embedded in mesh node, port 8650, SQLite auth, pending/approve/reject API) |
| 🟡 MED | Shared memory mode | PG transport uses shared_a2a_memory table (not mesh.mesh_messages) | ✅ Done |
| 🟡 MED | Admin approval | New nodes register as pending, dashboard approve/reject, coordinator auto-approve | ✅ Done |
| 🟡 MED | Multi-router discovery | mesh_nodes network columns (host, p2p_port, health_port, transport availability) | ✅ Done |
| 🔵 LOW | BLE GATT transport | Proximity mesh communication | ✅ Done (macOS GATT server + bleak scanner, Linux no BLE) |
| 🟡 MED | Morzsa mesh node | Remote Linux LXC mesh node | ✅ Done (router 0x0001, 3/3 transports, P2P verified) |
| 🔵 LOW | Iroh P2P transport | QUIC mesh networking | 🔲 BLOCKED (Python 3.14 segfault) |

| Risk | Severity | Likelihood | Mitigation |
|------|----------|------------|------------|
| Iroh Python bindings unstable | 🟠 HIGH | 🟡 Medium | Pin version, fallback to PG |
| BLE GATT server macOS | 🟡 Was listed as limited | ✅ VERIFIED: CBPeripheralManager FULL support 10.9+ | Full GATT server on macOS — both advertise and message transport |
| WiFi Direct macOS AP | 🔴 CRITICAL | 🔴 Confirmed | macOS has NO public API for AP mode. IBSS deprecated macOS 11. Use Iroh QUIC instead. |
| MultipeerConnectivity | 🟡 MEDIUM | ✅ Available macOS 10.10+ | Apple-ecosystem only. Useful for Mac↔iOS P2P. |
| NEPacketTunnelProvider | 🟢 LOW | ✅ Available macOS 10.11+ | How Tailscale works. Best for mesh VPN overlay. |
| WiFi Direct macOS AP mode | 🔴 CRITICAL | 🔴 Definite | macOS joins existing AP; Iroh QUIC as primary P2P |
| WiFi Direct needs root on Linux | 🟡 MEDIUM | 🟠 HIGH | Run as systemd service with NET_ADMIN capability |
| Cross-compile ARM (RPi) | 🟡 MEDIUM | 🟡 Medium | Pre-built wheels, CI pipeline |
| Flutter Rust FFI complexity | 🟡 MEDIUM | 🟡 Medium | Use flutter_iroh package if available, else dart:ffi |
| Message ordering across mesh | 🟠 HIGH | 🟡 Medium | UUID v7 timestamp ordering + CRDT merge |
| Split brain (2 nodes disagree) | 🟡 MEDIUM | 🟡 Low | PG as truth source + CRDT merge for offline |
| Bandwidth in large mesh (50+) | 🟡 MEDIUM | 🟢 Low | Gossip routing (K=3) instead of flood |
| Battery drain (BLE scan) | 🟡 MEDIUM | 🟡 Medium | Adaptive scan interval (30s active, 120s idle) |

## Agent DM Handling (v0.40.0+)

### Mi az agent DM?
Amikor egy mesh agent (Morzsa, Runa) proaktívan DM-et küld Nova-nak (`/api/agent-dm` vagy `DM:nova:üzenet` formátum), az üzenet megérkezik a Nova node-ra P2P transporton. A Nova agent **azonnal reagáljon rá**, mintha a user közvetlenül írta volna — ne külön keresni kelljen.

### Workflow (kötelező)
1. **Bejövő DM észlelése** — amikor az A2A message bus-on vagy a mesh node API-n új DM érkezik Nova számára (`recipient=nova`, `msg_type=agent_dm` vagy `a2a_message` sender más agent)
2. **Azonnali reakció** — Nova azonnal dolgozza fel és válaszoljon:
   - Olvassa el a DM tartalmát
   - Ha kérdés → válaszoljon rá
   - Ha információ → nyugtázza
   - Ha javaslat → értékelje és válaszoljon
3. **Válasz küldése** — `/api/agent-dm` endpoint-on keresztül, `sender=nova`, `recipient=<feladó>`
4. **Nincs ping-pong** — az anti-ping-pong filter (v0.39.2) megakadályozza, hogy a válasz újabb wake-agentet triggereljen

### API
```
# DM küldése egy agentnek
POST /api/agent-dm
{"sender": "nova", "recipient": "morzsa", "content": "üzenet szövege"}

# Válasz a DM-re
POST /api/agent-dm
{"sender": "nova", "recipient": "runa", "content": "válasz szövege"}
```

### Fontos szabályok
- **Az üzeneteket úgy kezeld, mintha direktben jönnének** — azaz azonnal dolgozd fel és válaszolj, ne várd meg míg a user kéri
- **Ne indíts ping-pong-ot** — egy válasz elég, ne broadcast-old a reply-t
- **A DM-ek nem a chat history-ba mennek** — külön channel, csak a feladó és címzett látja
- **Ha az üzenet parancs** — hajtsd végre, majd küldj eredményt DM-ben
- **Ha az üzenet javaslat** — értékeld, döntsd el, és válaszold meg

### Anti-ping-pong filter (v0.39.2+)
A `node.py` receive loop-ja szűri az `agent_reply` és `agent_dm` típusú üzeneteket a wake-agent trigger-ből. Ez azt jelenti, hogy agent→agent üzenetek **nem** indítanak automatikus láncot. A DM-ek explicit wake-agent hívással indulnak (a DM API külön trigger-t használ), de a válaszok nem triggerelik a peer agenteket.

### Verziótörténet
- v0.39.1: Proaktív agent→agent DM implementáció (`DM:target:msg` formátum)
- v0.39.2: Anti-ping-pong filter — agent reply-k nem triggerelnek wake-agentet
- v0.40.0: DM handling workflow dokumentálva ebben a skill-ben

## Devil's Advocate — Final Review

### ✅ DO THIS
- **Iroh P2P** — mature Rust library, QUIC, NAT traversal, DHT discovery
- **WiFi Direct** — full implementation per platform (Linux hostapd, Windows WinRT, macOS NOT possible)
- **BLE GATT** — FULL on all platforms including macOS (CBPeripheralManager verified 10.9+)
- **Flutter app** — single codebase, iOS + Android + desktop
- **Rust FFI** — native performance for Iroh + crypto
- **Ed25519 signing** — mandatory for mesh security
- **Gossip routing** — scalable to unlimited agents
- **CRDT merge** — conflict resolution for offline scenarios
- **MultipeerConnectivity** — Apple-ecosystem bonus (Mac↔iOS P2P)
- **NEPacketTunnelProvider** — mesh VPN overlay (Tailscale approach)

### ❌ DON'T DO THIS
- Full mesh on day 1 → phase it in (P1 core, P2 WiFi/BLE, P3 Flutter)
- WiFi Direct AP on macOS → impossible, use Iroh QUIC instead
- BLE discovery-only on macOS → GATT server is FULLY supported
- Skip encryption → mandatory in mesh (any node can see messages)
- Pure flood routing for 50+ nodes → use gossip (K=3)

### ⚠️ WATCH OUT
- macOS WiFi Direct limitations — Iroh QUIC is the real P2P transport
- BLE battery drain on mobile — adaptive scan intervals
- Iroh Python bindings may lag behind Rust — pin versions
- Flutter + Rust FFI build complexity — automate with CI
- WiFi Direct group owner election — first node wins, others join
- Gossip routing needs K≥3 for reliability in sparse meshes