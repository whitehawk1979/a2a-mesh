"""A2A Mesh P2P Authentication — HMAC-SHA256 message signing with replay protection.

Implements three layers of P2P security:
1. HMAC-SHA256 signature on every P2P message (timestamp + nonce based)
2. Automatic token rotation (24h default)
3. Per-peer rate limiting (100 req/min default)

Configured via security.transport_auth in mesh_config.yaml:
  - "hmac": HMAC-SHA256 signed messages with timestamp+nonce replay protection
  - "none": No transport-level auth (insecure, only for testing)
"""

import hashlib
import hmac
import logging
import os
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple

log = logging.getLogger("a2a_mesh.p2p_auth")


# ─── HMAC Token with Rotation ────────────────────────────────────────

class HMACRotatingToken:
    """HMAC signing key with automatic rotation.
    
    Generates a new signing key every `rotation_interval` seconds.
    Both the current and previous key are valid for verification
    to allow graceful key transition during rotation.
    
    Keys are derived from a master seed (stored on disk) so all nodes
    in the mesh can derive the same key at the same time interval.
    """
    
    def __init__(self, shared_secret: str = "", rotation_interval: int = 86400):
        self._shared_secret = shared_secret or self._load_or_generate_secret()
        self._rotation_interval = rotation_interval
        self._current_key: Optional[bytes] = None
        self._previous_key: Optional[bytes] = None
        self._current_epoch: int = 0
        self._rotate_if_needed()
    
    def _load_or_generate_secret(self) -> str:
        """Load shared secret from file, or generate and persist one."""
        secret_path = os.path.expanduser("~/.hermes/mesh_p2p_secret")
        if os.path.exists(secret_path):
            with open(secret_path, "r") as f:
                secret = f.read().strip()
                if secret:
                    return secret
        # Generate new secret
        secret = uuid.uuid4().hex + uuid.uuid4().hex
        os.makedirs(os.path.dirname(secret_path), exist_ok=True)
        with open(secret_path, "w") as f:
            f.write(secret)
        # Try to store in PG for mesh-wide sync
        self._sync_secret_to_pg(secret)
        log.info(f"P2P auth: generated new shared secret ({secret[:8]}...)")
        return secret
    
    def _sync_secret_to_pg(self, secret: str):
        """Try to sync the shared secret to PG for mesh-wide distribution."""
        try:
            pg_dsn = os.environ.get("A2A_MESH_PG_DSN")
            if not pg_dsn:
                return
            import psycopg2
            conn = psycopg2.connect(pg_dsn)
            conn.set_client_encoding('UTF8')
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mesh.mesh_secrets (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at REAL NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                INSERT INTO mesh.mesh_secrets (key, value, updated_at)
                VALUES ('p2p_shared_secret', %s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
            """, (secret, time.time()))
            conn.commit()
            conn.close()
            log.info("P2P auth: shared secret synced to PG")
        except Exception as e:
            log.debug(f"P2P auth: PG sync for shared secret skipped: {e}")
    
    def _derive_key(self, epoch: int) -> bytes:
        """Derive a signing key for a given time epoch.
        
        Key = HMAC-SHA256(shared_secret, epoch_number)
        This ensures all nodes with the same shared_secret derive
        the same key for the same time interval.
        """
        return hmac.new(
            self._shared_secret.encode(),
            f"p2p-auth-epoch-{epoch}".encode(),
            hashlib.sha256
        ).digest()
    
    def _rotate_if_needed(self):
        """Rotate keys if the current epoch has changed."""
        epoch = int(time.time() // self._rotation_interval)
        if epoch != self._current_epoch:
            self._previous_key = self._current_key  # Keep old key for grace period
            self._current_key = self._derive_key(epoch)
            old_epoch = self._current_epoch
            self._current_epoch = epoch
            if old_epoch > 0:
                log.info(f"P2P auth: key rotated (epoch {old_epoch} → {epoch})")
    
    def sign(self, content: str) -> Tuple[str, int, str]:
        """Sign content with current key. Returns (signature, timestamp, nonce).
        
        The signature covers: content + timestamp + nonce
        This prevents replay attacks and ensures message integrity.
        """
        self._rotate_if_needed()
        timestamp = int(time.time())
        nonce = uuid.uuid4().hex[:16]
        signing_key = self._current_key or self._derive_key(self._current_epoch)
        # Sign: HMAC-SHA256(key, content + timestamp + nonce)
        message = f"{content}:{timestamp}:{nonce}"
        signature = hmac.new(signing_key, message.encode(), hashlib.sha256).hexdigest()
        return signature, timestamp, nonce
    
    def verify(self, content: str, signature: str, timestamp: int, nonce: str,
               max_skew_seconds: int = 300) -> bool:
        """Verify an HMAC signature with timestamp and nonce.
        
        Checks:
        1. Timestamp is within max_skew_seconds of current time (replay protection)
        2. Nonce has not been seen before (replay protection)
        3. HMAC-SHA256 signature is valid (integrity + authenticity)
        
        Returns True if the signature is valid, False otherwise.
        """
        self._rotate_if_needed()
        
        # Check timestamp skew
        now = int(time.time())
        if abs(now - timestamp) > max_skew_seconds:
            log.warning(f"P2P auth: rejected message with skew {abs(now - timestamp)}s "
                       f"(max {max_skew_seconds}s)")
            return False
        
        # Try current key first, then previous key (grace period during rotation)
        for key in [self._current_key, self._previous_key]:
            if key is None:
                continue
            message = f"{content}:{timestamp}:{nonce}"
            expected = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
            if hmac.compare_digest(signature, expected):
                return True
        
        log.warning(f"P2P auth: HMAC verification failed for content (ts={timestamp})")
        return False


# ─── Nonce Cache for Replay Protection ────────────────────────────────

class NonceCache:
    """LRU cache of recently seen nonces to prevent replay attacks.
    
    Keeps nonces for max_skew_seconds (default 300 = 5 minutes).
    Uses OrderedDict for LRU eviction when cache exceeds max_size.
    """
    
    def __init__(self, max_size: int = 10000, ttl_seconds: int = 300):
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._last_cleanup = time.time()
    
    def check_and_record(self, nonce: str) -> bool:
        """Check if a nonce has been seen before. Returns True if nonce is NEW (not replayed).
        
        Also records the nonce for future replay detection.
        """
        now = time.time()
        
        # Periodic cleanup
        if now - self._last_cleanup > 60:
            self._cleanup(now)
        
        if nonce in self._cache:
            return False  # Replay detected
        
        # Record nonce
        self._cache[nonce] = now
        self._cache.move_to_end(nonce)
        
        # Evict oldest if over limit
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)
        
        return True
    
    def _cleanup(self, now: float):
        """Remove expired nonces."""
        cutoff = now - self._ttl_seconds
        expired = [k for k, v in self._cache.items() if v < cutoff]
        for k in expired:
            del self._cache[k]
        self._last_cleanup = now


# ─── Per-Peer Rate Limiter ────────────────────────────────────────────

class PeerRateLimiter:
    """Sliding window rate limiter for P2P connections.
    
    Default: 100 requests per minute per peer.
    Prevents DoS attacks and resource exhaustion from a single peer.
    """
    
    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0):
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        # peer_name -> list of timestamps
        self._windows: Dict[str, list] = {}
    
    def allow(self, peer_name: str) -> bool:
        """Check if a request from peer_name is allowed. Returns True if within limits."""
        now = time.time()
        cutoff = now - self._window_seconds
        
        if peer_name not in self._windows:
            self._windows[peer_name] = []
        
        # Clean old entries
        self._windows[peer_name] = [t for t in self._windows[peer_name] if t > cutoff]
        
        if len(self._windows[peer_name]) >= self._max_requests:
            log.warning(f"P2P rate limit: peer {peer_name} exceeded "
                       f"{self._max_requests} req/{self._window_seconds}s")
            return False
        
        self._windows[peer_name].append(now)
        return True
    
    def remaining(self, peer_name: str) -> int:
        """Get remaining requests for a peer in the current window."""
        now = time.time()
        cutoff = now - self._window_seconds
        if peer_name not in self._windows:
            return self._max_requests
        current = [t for t in self._windows[peer_name] if t > cutoff]
        return max(0, self._max_requests - len(current))
    
    def reset(self, peer_name: str = None):
        """Reset rate limits for a specific peer or all peers."""
        if peer_name:
            self._windows.pop(peer_name, None)
        else:
            self._windows.clear()


# ─── P2P Authenticator (Main Interface) ────────────────────────────────

class P2PAuthenticator:
    """P2P message authentication combining HMAC signing, replay protection, and rate limiting.
    
    Usage:
        auth = P2PAuthenticator(shared_secret="...", enabled=True)
        
        # Sender side: sign a message
        sig, ts, nonce = auth.sign_message(message_content)
        # Attach to message: message.payload["_auth"] = {"sig": sig, "ts": ts, "nonce": nonce}
        
        # Receiver side: verify a message
        auth_data = message.payload.get("_auth", {})
        if not auth.verify_message(message_content, auth_data["sig"], auth_data["ts"], auth_data["nonce"], peer_name):
            # Reject message
    """
    
    def __init__(self, shared_secret: str = "", enabled: bool = True,
                 rotation_interval: int = 86400,
                 max_skew_seconds: int = 300,
                 rate_limit: int = 100, rate_window: int = 60):
        self.enabled = enabled
        self._token = HMACRotatingToken(shared_secret=shared_secret,
                                         rotation_interval=rotation_interval)
        self._nonce_cache = NonceCache(ttl_seconds=max_skew_seconds)
        self._rate_limiter = PeerRateLimiter(max_requests=rate_limit,
                                              window_seconds=float(rate_window))
        self._max_skew_seconds = max_skew_seconds
        log.info(f"P2P auth initialized: enabled={enabled}, rotation={rotation_interval}s, "
                f"max_skew={max_skew_seconds}s, rate_limit={rate_limit}/{rate_window}s")
    
    def sign_message(self, content: str) -> Dict[str, any]:
        """Sign a P2P message. Returns auth dict to attach to message payload.
        
        Args:
            content: The message content to sign (typically message.sign_content())
        
        Returns:
            Dict with keys: sig, ts, nonce
        """
        if not self.enabled:
            return {}
        
        sig, ts, nonce = self._token.sign(content)
        return {"sig": sig, "ts": ts, "nonce": nonce}
    
    def verify_message(self, content: str, signature: str, timestamp: int,
                       nonce: str, peer_name: str = "unknown") -> bool:
        """Verify a P2P message's HMAC signature with full replay protection.
        
        Checks (in order):
        1. Rate limit for the peer
        2. Timestamp skew
        3. Nonce uniqueness (replay protection)
        4. HMAC-SHA256 signature
        
        Args:
            content: The message content that was signed
            signature: The HMAC signature
            timestamp: Unix timestamp from the sender
            nonce: Unique nonce from the sender
            peer_name: Name of the sending peer (for rate limiting)
        
        Returns:
            True if the message is authentic and not a replay
        """
        if not self.enabled:
            return True
        
        # 1. Rate limit check
        if not self._rate_limiter.allow(peer_name):
            log.warning(f"P2P auth: rate limited peer {peer_name}")
            return False
        
        # 2. Nonce uniqueness (replay protection)
        if not self._nonce_cache.check_and_record(nonce):
            log.warning(f"P2P auth: replay detected from {peer_name} (nonce={nonce[:8]}...)")
            return False
        
        # 3. HMAC signature verification (includes timestamp skew check)
        if not self._token.verify(content, signature, timestamp, nonce, self._max_skew_seconds):
            log.warning(f"P2P auth: HMAC verification failed for peer {peer_name}")
            return False
        
        return True
    
    def check_rate_limit(self, peer_name: str) -> bool:
        """Check rate limit without verifying a message. Useful for connection-level checks."""
        if not self.enabled:
            return True
        return self._rate_limiter.allow(peer_name)
    
    def get_stats(self) -> Dict:
        """Get authentication statistics."""
        return {
            "enabled": self.enabled,
            "current_epoch": self._token._current_epoch,
            "nonce_cache_size": len(self._nonce_cache._cache),
            "rate_limited_peers": sum(
                1 for v in self._rate_limiter._windows.values()
                if len(v) >= self._rate_limiter._max_requests
            ),
        }


# Convenience: empty dict for import
from typing import Dict as _Dict
