"""A2A Mesh Node Authentication — Verify new nodes joining the mesh.

When a node sends a join request, the coordinator (or trust center) verifies:
1. The node's identity (Ed25519 signature verification)
2. The node is authorized (whitelist or trust-on-first-use)
3. The join request has a valid nonce and timestamp

Modes:
- "open" — any node can join (default for local mesh)
- "whitelist" — only pre-approved nodes can join
- "trust_on_first_use" — first node to claim a name is trusted
"""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set, Dict

log = logging.getLogger("a2a_mesh.auth")


class AuthMode(Enum):
    """Node authentication mode."""
    OPEN = "open"                     # Any node can join
    WHITELIST = "whitelist"            # Only whitelisted nodes
    TRUST_ON_FIRST_USE = "tofu"        # First claim wins


@dataclass
class AuthConfig:
    """Authentication configuration."""
    mode: str = "open"                 # AuthMode value
    trust_center: str = ""             # Node name of trust center (coordinator)
    whitelist: Set[str] = field(default_factory=set)  # Pre-approved node names
    max_join_age_seconds: int = 300    # Join request must be fresh (5 min)
    max_failed_attempts: int = 5       # Ban after this many failed attempts


@dataclass
class JoinRequest:
    """A node requesting to join the mesh."""
    node_name: str
    node_role: str                      # "coordinator", "router", "end_device"
    public_key: str                     # Ed25519 verify key (hex)
    timestamp: float                    # Unix timestamp
    nonce: str                          # Random challenge
    signature: str = ""                 # Ed25519 signature of join payload
    parent_node: str = ""               # Requested parent node
    proof: str = ""                     # Additional proof (e.g., shared secret)


class NodeAuthenticator:
    """Authenticates nodes joining the mesh.

    Usage:
        auth = NodeAuthenticator(config)
        result = auth.authenticate_join(join_request)
        if result:
            print("Node authorized!")
    """

    def __init__(self, config: AuthConfig):
        self.config = config
        self._known_keys: Dict[str, str] = {}       # node_name → public_key
        self._failed_attempts: Dict[str, int] = {}   # node_name → failed count
        self._banned_until: Dict[str, float] = {}    # node_name → ban expiry

    def authenticate_join(self, request: JoinRequest) -> tuple[bool, str]:
        """Authenticate a join request. Returns (authorized, reason)."""
        # Check if node is banned
        if request.node_name in self._banned_until:
            if time.time() < self._banned_until[request.node_name]:
                return False, f"Node {request.node_name} is temporarily banned"
            else:
                del self._banned_until[request.node_name]
                self._failed_attempts.pop(request.node_name, 0)

        # Check timestamp freshness
        age = time.time() - request.timestamp
        if age < 0 or age > self.config.max_join_age_seconds:
            return False, f"Join request too old ({age:.0f}s, max {self.config.max_join_age_seconds}s)"

        # Mode-specific checks
        mode = AuthMode(self.config.mode)

        if mode == AuthMode.OPEN:
            # Open mesh — any node can join
            self._register_node(request)
            return True, "Node authorized (open mode)"

        elif mode == AuthMode.WHITELIST:
            # Check whitelist
            if request.node_name not in self.config.whitelist:
                self._record_failure(request.node_name)
                return False, f"Node {request.node_name} not in whitelist"

            # Verify signature if provided
            if request.signature:
                if not self._verify_signature(request):
                    self._record_failure(request.node_name)
                    return False, "Invalid signature"

            self._register_node(request)
            return True, "Node authorized (whitelist mode)"

        elif mode == AuthMode.TRUST_ON_FIRST_USE:
            # First node to claim a name is trusted
            if request.node_name in self._known_keys:
                # Known node — verify public key matches
                if self._known_keys[request.node_name] != request.public_key:
                    self._record_failure(request.node_name)
                    return False, f"Public key mismatch for {request.node_name}"

                # Verify signature
                if request.signature and not self._verify_signature(request):
                    self._record_failure(request.node_name)
                    return False, "Invalid signature"
            else:
                # New node — trust first use
                log.info(f"Trust-on-first-use: accepting new node {request.node_name}")

            self._register_node(request)
            return True, "Node authorized (TOFU mode)"

        return False, f"Unknown auth mode: {self.config.mode}"

    def _verify_signature(self, request: JoinRequest) -> bool:
        """Verify Ed25519 signature of the join request."""
        try:
            from .encryption import MeshEncryption
            # Reconstruct the signed content
            content = f"{request.node_name}:{request.node_role}:{request.public_key}:{request.timestamp}:{request.nonce}"
            enc = MeshEncryption()
            return enc.verify_message(content, request.signature, request.public_key)
        except ImportError:
            log.warning("pynacl not installed — skipping signature verification")
            return True
        except Exception as e:
            log.error(f"Signature verification failed: {e}")
            return False

    def _register_node(self, request: JoinRequest):
        """Register a node's public key after successful authentication."""
        self._known_keys[request.node_name] = request.public_key
        log.info(f"Registered node {request.node_name} with key {request.public_key[:16]}...")

    def _record_failure(self, node_name: str):
        """Record a failed authentication attempt."""
        self._failed_attempts[node_name] = self._failed_attempts.get(node_name, 0) + 1
        count = self._failed_attempts[node_name]

        if count >= self.config.max_failed_attempts:
            # Ban for 1 hour
            self._banned_until[node_name] = time.time() + 3600
            log.warning(f"Node {node_name} banned for 1h after {count} failed attempts")

    def revoke_node(self, node_name: str):
        """Revoke a node's authentication (force re-authentication)."""
        self._known_keys.pop(node_name, None)
        log.info(f"Revoked authentication for node {node_name}")

    def get_known_nodes(self) -> Dict[str, str]:
        """Get all known authenticated nodes and their public keys."""
        return dict(self._known_keys)

    def is_authenticated(self, node_name: str) -> bool:
        """Check if a node is authenticated (has a known key)."""
        return node_name in self._known_keys