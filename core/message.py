"""A2AMessage — Universal mesh message format.

Works on all transports (PG, TCP, HTTP, BLE, WiFi Direct).
UUID v7 for time-sortable ordering, Ed25519 signing, NaCl encryption.
"""

import json
import uuid
import hashlib
import time
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any

log = logging.getLogger("a2a_mesh.message")



def uuid_v7() -> str:
    """Generate UUID v7 (time-sortable). Falls back to UUID v4 with timestamp prefix."""
    try:
        # Python 3.12+ has uuid7 experimental support
        # For now, use timestamp + uuid4 hybrid
        ts = int(time.time() * 1000)
        uid = uuid.uuid4()
        # Embed timestamp in first 48 bits for sortability
        hex_ts = format(ts, '012x')
        hex_rand = uid.hex[12:]  # Use random part from uuid4
        return f"{hex_ts[:8]}-{hex_ts[8:12]}-7{hex_rand[0:3]}-{hex_rand[3:7]}-{hex_rand[7:19]}"
    except Exception:
        return str(uuid.uuid4())


# Message types
MSG_TYPE_DIRECTIVE = "directive"
MSG_TYPE_TASK = "task"
MSG_TYPE_RESULT = "result"
MSG_TYPE_HEARTBEAT = "heartbeat"
MSG_TYPE_STEER = "steer"
MSG_TYPE_FILE = "file"
MSG_TYPE_DISCOVERY = "discovery"
MSG_TYPE_ACK = "ack"
MSG_TYPE_BROADCAST = "broadcast"
MSG_TYPE_DELEGATION = "delegation"
MSG_TYPE_CONTEXT = "context"
MSG_TYPE_ERROR = "error"
MSG_TYPE_MESH = "mesh"  # Mesh-level messages (join, leave, ping)


@dataclass
class A2AMessage:
    """Universal mesh message format for A2A communication.

    Transport-agnostic: serializes to JSON (or msgpack for binary transports).
    Signed with Ed25519 for authenticity verification.
    Encrypted with NaCl for privacy (optional).
    """

    # Identity
    id: str = ""
    sender: str = ""
    sender_node_id: str = ""
    recipient: str = ""  # Agent name or "broadcast"

    # Content
    type: str = MSG_TYPE_DIRECTIVE
    priority: int = 5  # 1-10 (10 = interrupt)
    payload: Dict[str, Any] = field(default_factory=dict)

    # Routing
    ttl: int = 10  # Max hops remaining
    transport_hint: str = ""  # Preferred transport (optional)
    hop_count: int = 0  # How many nodes forwarded this
    path: list = field(default_factory=list)  # Nodes that forwarded

    # Security
    signature: str = ""
    encrypted: bool = False

    # Metadata
    timestamp: str = ""
    created_at: str = ""

    # Zigbee-inspired routing fields
    dst_address: Optional[Dict] = None  # Target MeshAddress (None = broadcast)
    src_address: Optional[Dict] = None  # Source MeshAddress
    route_path: list = field(default_factory=list)  # Short addresses traversed
    routing_mode: str = "hybrid"  # "flood", "tree", "hybrid"

    def __post_init__(self):
        if not self.id:
            self.id = uuid_v7()
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()
        if not self.created_at:
            self.created_at = self.timestamp

    @classmethod
    def create(cls, sender: str, recipient: str, msg_type: str,
                payload: dict, priority: int = 5, ttl: int = 10) -> 'A2AMessage':
        """Create a new message with auto-generated ID and timestamp."""
        return cls(
            sender=sender,
            recipient=recipient,
            type=msg_type,
            payload=payload,
            priority=priority,
            ttl=ttl,
        )

    def sign_content(self) -> str:
        """Get the content to sign (deterministic serialization)."""
        content = f"{self.id}:{self.sender}:{self.recipient}:{self.timestamp}:{self.type}:{self.priority}"
        return content

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), sort_keys=True, default=str)

    def to_bytes(self) -> bytes:
        """Serialize to bytes (msgpack if available, else JSON)."""
        try:
            import msgpack
            return msgpack.packb(self.to_dict(), use_bin_type=True)
        except ImportError:
            return self.to_json().encode('utf-8')

    @classmethod
    def from_dict(cls, d: dict) -> 'A2AMessage':
        """Deserialize from dictionary."""
        # Filter unknown fields
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)

    @classmethod
    def from_json(cls, data: str) -> 'A2AMessage':
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(data))

    @classmethod
    def from_bytes(cls, data: bytes) -> 'A2AMessage':
        """Deserialize from bytes (msgpack or JSON)."""
        try:
            import msgpack
            d = msgpack.unpackb(data, raw=False)
            return cls.from_dict(d)
        except (ImportError, Exception):
            return cls.from_json(data.decode('utf-8'))

    def is_broadcast(self) -> bool:
        """Check if this is a broadcast message."""
        return self.recipient == "broadcast"

    def is_expired(self) -> bool:
        """Check if TTL has expired."""
        return self.ttl <= 0

    def decrement_ttl(self) -> 'A2AMessage':
        """Decrement TTL and increment hop count (returns new message)."""
        msg = A2AMessage.from_dict(self.to_dict())
        msg.ttl = self.ttl - 1
        msg.hop_count = self.hop_count + 1
        return msg

    def add_hop(self, node_name: str) -> 'A2AMessage':
        """Add a hop to the path (returns new message)."""
        msg = A2AMessage.from_dict(self.to_dict())
        msg.path = list(self.path) + [node_name]
        return msg

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, A2AMessage):
            return self.id == other.id
        return False


@dataclass
class SendResult:
    """Result of sending a message via a transport."""
    transport: str
    success: bool
    error: str = ""
    latency_ms: float = 0.0

    def __repr__(self):
        status = "✅" if self.success else "❌"
        return f"SendResult({status} {self.transport} {self.latency_ms:.1f}ms {self.error})"


@dataclass
class ProcessResult:
    """Result of processing a received message."""
    status: str  # "processed", "duplicate", "forwarded", "self_reference", "ttl_expired", "invalid_signature"
    message: Optional[A2AMessage] = None

    def __repr__(self):
        return f"ProcessResult({self.status})"