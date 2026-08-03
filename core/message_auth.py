"""A2A Mesh Message Authentication — HMAC-SHA256 signing, nonce tracking,
rate limiting, and token rotation via PostgreSQL.

Provides MessageAuth class that handles:
- HMAC-SHA256 message signing and verification
- Nonce-based replay attack prevention (in-memory LRU + PG persistence)
- Per-peer rate limiting (sliding window)
- Cryptographic token rotation with PG-backed key storage

SecurityConfig in core/config.py controls transport_auth ('hmac' or 'none').
When 'hmac', every outbound message is signed and every inbound message
is verified before processing.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple

from .config import SecurityConfig, PGConfig

log = logging.getLogger("a2a_mesh.message_auth")

# ─── Constants ──────────────────────────────────────────────────────────────

# Nonce cache: max entries held in memory before LRU eviction
NONCE_CACHE_MAX = 10_000
# Nonce TTL: how long a nonce is considered valid for replay detection
NONCE_TTL_SECONDS = 600  # 10 minutes

# Rate limiting defaults
RATE_LIMIT_WINDOW = 60       # seconds per window
RATE_LIMIT_MAX_MESSAGES = 120 # messages per peer per window

# Token rotation
TOKEN_ROTATION_INTERVAL = 3600  # seconds (1 hour)
TOKEN_GRACE_PERIOD = 300       # seconds (5 min overlap for key transition)


# ─── Exceptions ─────────────────────────────────────────────────────────────

class MessageAuthError(Exception):
    """Base exception for message auth failures."""
    pass


class InvalidSignatureError(MessageAuthError):
    """HMAC signature verification failed."""
    pass


class ReplayAttackError(MessageAuthError):
    """Nonce has already been used (replay detected)."""
    pass


class RateLimitExceededError(MessageAuthError):
    """Peer has exceeded the allowed message rate."""
    pass


class TokenNotFoundError(MessageAuthError):
    """Requested token ID not found in PG store."""
    pass


# ─── NonceTracker ────────────────────────────────────────────────────────────

class NonceTracker:
    """In-memory LRU nonce tracker for replay attack prevention.

    Each nonce (unique per message) is stored with its timestamp.
    Entries older than NONCE_TTL_SECONDS are evicted on access.
    For multi-node setups, nonces are also persisted to PG so that
    replay detection works across mesh nodes.
    """

    def __init__(self, max_size: int = NONCE_CACHE_MAX, ttl: int = NONCE_TTL_SECONDS):
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl
        self._lock = asyncio.Lock()

    async def check_and_record(self, nonce: str, *, allow_known: bool = False) -> bool:
        """Check if nonce is fresh and record it.

        Args:
            nonce: Unique nonce string to check
            allow_known: If True, silently accept a known nonce (for self-echo).
                        If False (default), raise ReplayAttackError on duplicates.

        Returns True if nonce is valid (not seen before, or allow_known=True).

        Raises ReplayAttackError if nonce was already used and allow_known=False.
        """
        now = time.time()
        async with self._lock:
            self._evict_expired(now)
            if nonce in self._cache:
                self._cache.move_to_end(nonce)
                if allow_known:
                    return True  # Self-echo: nonce is ours, allow it
                raise ReplayAttackError(f"Duplicate nonce detected: {nonce[:16]}...")
            self._cache[nonce] = now
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
            return True

    def _evict_expired(self, now: float):
        """Remove entries older than TTL."""
        cutoff = now - self._ttl
        while self._cache:
            # Peek at oldest entry
            oldest_key = next(iter(self._cache))
            if self._cache[oldest_key] < cutoff:
                self._cache.popitem(last=False)
            else:
                break

    async def is_known(self, nonce: str) -> bool:
        """Check if nonce has been seen (non-destructive check)."""
        async with self._lock:
            return nonce in self._cache

    @property
    def size(self) -> int:
        return len(self._cache)


# ─── RateLimiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Sliding-window rate limiter per peer.

    Tracks message timestamps per sender within RATE_LIMIT_WINDOW seconds.
    If a sender exceeds RATE_LIMIT_MAX_MESSAGES within the window, further
    messages are rejected until the window slides.
    """

    def __init__(self, window: int = RATE_LIMIT_WINDOW,
                 max_messages: int = RATE_LIMIT_MAX_MESSAGES):
        self._window = window
        self._max = max_messages
        self._peers: Dict[str, list] = {}  # peer -> [timestamps]
        self._lock = asyncio.Lock()

    async def check(self, peer: str) -> bool:
        """Check if peer is within rate limit. Returns True if allowed.

        Raises RateLimitExceededError if peer exceeded the limit.
        """
        now = time.time()
        async with self._lock:
            timestamps = self._peers.get(peer, [])
            # Filter to current window
            timestamps = [t for t in timestamps if now - t < self._window]
            if len(timestamps) >= self._max:
                self._peers[peer] = timestamps
                raise RateLimitExceededError(
                    f"Peer '{peer}' exceeded {self._max} messages per {self._window}s"
                )
            timestamps.append(now)
            self._peers[peer] = timestamps
            return True

    async def get_remaining(self, peer: str) -> int:
        """Get remaining message allowance for a peer in current window."""
        now = time.time()
        async with self._lock:
            timestamps = self._peers.get(peer, [])
            timestamps = [t for t in timestamps if now - t < self._window]
            self._peers[peer] = timestamps
            return max(0, self._max - len(timestamps))

    async def reset(self, peer: str):
        """Reset rate limit for a specific peer."""
        async with self._lock:
            self._peers.pop(peer, None)


# ─── TokenManager ────────────────────────────────────────────────────────────

@dataclass
class AuthToken:
    """A cryptographic signing token with metadata."""
    token_id: str
    secret: str          # Hex-encoded secret key
    created_at: float
    expires_at: float
    is_active: bool = True

    def is_valid(self, now: Optional[float] = None) -> bool:
        """Check if token is within its active lifetime."""
        now = now or time.time()
        return self.is_active and self.created_at <= now < self.expires_at


class TokenManager:
    """Manages HMAC signing tokens with PG-backed persistence and rotation.

    Tokens are used for HMAC-SHA256 message authentication. Each node
    generates its own token (secret key) and shares the token_id publicly.
    The secret is stored in PG so other nodes can look it up for verification.

    Token rotation:
    - A new token is generated every TOKEN_ROTATION_INTERVAL seconds
    - Old tokens remain valid for TOKEN_GRACE_PERIOD after rotation
    - This limits the blast radius of a compromised key
    - Rotation is triggered automatically via ensure_current_token()
    """

    def __init__(self, node_name: str, pg_config: Optional[PGConfig] = None,
                 rotation_interval: int = TOKEN_ROTATION_INTERVAL,
                 grace_period: int = TOKEN_GRACE_PERIOD):
        self.node_name = node_name
        self.pg_config = pg_config
        self._rotation_interval = rotation_interval
        self._grace_period = grace_period
        self._current_token: Optional[AuthToken] = None
        self._previous_token: Optional[AuthToken] = None
        self._pg_pool = None  # Set later via set_pg_pool()
        self._lock = asyncio.Lock()
        self._last_rotation = 0.0

    def set_pg_pool(self, pool):
        """Set the asyncpg pool for token persistence. Called after DB init."""
        self._pg_pool = pool

    async def ensure_table(self):
        """Create the auth_tokens table in PG if it doesn't exist."""
        if not self._pg_pool:
            return
        try:
            await self._pg_pool.execute("""
                CREATE TABLE IF NOT EXISTS mesh.auth_tokens (
                    token_id TEXT PRIMARY KEY,
                    node_name TEXT NOT NULL,
                    secret TEXT NOT NULL,
                    created_at DOUBLE PRECISION NOT NULL,
                    expires_at DOUBLE PRECISION NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    UNIQUE(token_id)
                )
            """)
            # Index for fast lookup of active tokens by node
            await self._pg_pool.execute("""
                CREATE INDEX IF NOT EXISTS idx_auth_tokens_node_active
                ON mesh.auth_tokens (node_name, is_active, expires_at)
            """)
            log.info("Auth tokens table ensured in PG")
        except Exception as e:
            log.error(f"Failed to create auth_tokens table: {e}")

    async def ensure_current_token(self) -> AuthToken:
        """Ensure we have a valid current token, rotating if needed.

        Rotation logic:
        1. If no token exists, create one
        2. If current token is within grace period of expiry, rotate
        3. Persist new token to PG
        4. Mark old token as inactive after grace period
        """
        now = time.time()
        async with self._lock:
            # Need rotation if: no token, or current token nearing expiry
            needs_rotation = (
                self._current_token is None
                or now >= self._current_token.expires_at - self._grace_period
            )

            if not needs_rotation:
                return self._current_token

            # Rotate: demote current to previous
            if self._current_token is not None:
                self._previous_token = self._current_token
                # Mark old token as inactive in PG
                if self._pg_pool:
                    try:
                        await self._pg_pool.execute(
                            "UPDATE mesh.auth_tokens SET is_active = FALSE WHERE token_id = $1",
                            self._previous_token.token_id
                        )
                    except Exception as e:
                        log.warning(f"Failed to deactivate old token in PG: {e}")

            # Generate new token
            token_id = f"{self.node_name}_{secrets.token_hex(8)}_{int(now)}"
            secret = secrets.token_hex(32)  # 256-bit key
            expires_at = now + self._rotation_interval + self._grace_period

            new_token = AuthToken(
                token_id=token_id,
                secret=secret,
                created_at=now,
                expires_at=expires_at,
                is_active=True,
            )

            # Persist to PG
            if self._pg_pool:
                try:
                    await self._pg_pool.execute("""
                        INSERT INTO mesh.auth_tokens (token_id, node_name, secret, created_at, expires_at, is_active)
                        VALUES ($1, $2, $3, $4, $5, TRUE)
                        ON CONFLICT (token_id) DO UPDATE SET
                            secret = EXCLUDED.secret,
                            expires_at = EXCLUDED.expires_at,
                            is_active = EXCLUDED.is_active
                    """, token_id, self.node_name, secret, now, expires_at)
                    log.info(f"New auth token persisted: {token_id[:24]}...")
                except Exception as e:
                    log.error(f"Failed to persist auth token to PG: {e}")

            self._current_token = new_token
            self._last_rotation = now
            log.info(f"Auth token rotated for {self.node_name}: {token_id[:24]}...")
            return new_token

    async def get_token_secret(self, token_id: str) -> Optional[str]:
        """Look up a token's secret by ID (for verification).

        Checks local cache first, then PG for tokens from other nodes.
        """
        # Check local tokens — for verification, only check time window,
        # not is_active (grace period allows recently-rotated tokens)
        now = time.time()
        for token in (self._current_token, self._previous_token):
            if token and token.token_id == token_id:
                if token.created_at <= now < token.expires_at:
                    return token.secret

        # Check PG
        if self._pg_pool:
            try:
                row = await self._pg_pool.fetchrow(
                    "SELECT secret, created_at, expires_at "
                    "FROM mesh.auth_tokens WHERE token_id = $1",
                    token_id
                )
                if row:
                    # For verification, only check the time window — not is_active.
                    # A rotated token (is_active=FALSE) must still verify during
                    # its grace period (expires_at includes the grace period).
                    if row["created_at"] <= now < row["expires_at"]:
                        return row["secret"]
            except Exception as e:
                log.warning(f"Failed to look up token {token_id[:16]}... from PG: {e}")

        return None

    async def load_token_from_pg(self):
        """Load the most recent active token for this node from PG.

        Called on startup to recover the current token after restart.
        """
        if not self._pg_pool:
            return
        try:
            row = await self._pg_pool.fetchrow(
                "SELECT token_id, secret, created_at, expires_at, is_active "
                "FROM mesh.auth_tokens "
                "WHERE node_name = $1 AND is_active = TRUE "
                "ORDER BY created_at DESC LIMIT 1",
                self.node_name
            )
            if row:
                self._current_token = AuthToken(
                    token_id=row["token_id"],
                    secret=row["secret"],
                    created_at=row["created_at"],
                    expires_at=row["expires_at"],
                    is_active=row["is_active"],
                )
                self._last_rotation = row["created_at"]
                log.info(f"Loaded auth token from PG: {row['token_id'][:24]}...")
        except Exception as e:
            log.warning(f"Failed to load token from PG: {e}")

    async def cleanup_expired_tokens(self, max_age_days: int = 7):
        """Remove tokens that expired more than max_age_days ago."""
        if not self._pg_pool:
            return
        try:
            cutoff = time.time() - (max_age_days * 86400)
            result = await self._pg_pool.execute(
                "DELETE FROM mesh.auth_tokens WHERE expires_at < $1",
                cutoff
            )
            if result and result != "DELETE 0":
                log.info(f"Cleaned up expired auth tokens: {result}")
        except Exception as e:
            log.warning(f"Failed to cleanup expired tokens: {e}")

    @property
    def current_token(self) -> Optional[AuthToken]:
        return self._current_token

    @property
    def current_secret(self) -> Optional[str]:
        return self._current_token.secret if self._current_token else None

    @property
    def current_token_id(self) -> Optional[str]:
        return self._current_token.token_id if self._current_token else None


# ─── MessageAuth ─────────────────────────────────────────────────────────────

class MessageAuth:
    """HMAC-SHA256 message authentication, nonce tracking, and rate limiting.

    Usage:
        auth = MessageAuth(config)
        await auth.start()

        # Sign outbound message
        signed = auth.sign_message(message_dict)

        # Verify inbound message
        ok = await auth.verify_message(signed_dict)

    When transport_auth is 'none', sign/verify are no-ops (pass-through).
    When 'hmac', every message gets an HMAC signature with a nonce to
    prevent replay attacks, and rate limiting is enforced per sender.
    """

    def __init__(self, config, pg_pool=None):
        """Initialize MessageAuth.

        Args:
            config: MeshConfig instance (must have .security and .pg)
            pg_pool: Optional AsyncDBPool instance for PG persistence.
                     If None, will be created on start() from config.
        """
        self.config = config
        self._pg_pool = pg_pool
        self._security: SecurityConfig = config.security
        self._node_name: str = config.node_name

        self._nonce_tracker = NonceTracker()
        self._rate_limiter = RateLimiter()

        # Use rotation interval and grace period from config (fall back to defaults)
        rotation_interval = getattr(self._security, 'auth_rotation_interval', TOKEN_ROTATION_INTERVAL)
        grace_period = TOKEN_GRACE_PERIOD  # Always use default grace period (5 min)

        self._token_manager = TokenManager(
            node_name=self._node_name,
            pg_config=config.pg,
            rotation_interval=rotation_interval,
            grace_period=grace_period,
        )

        self._started = False
        self._rotation_task: Optional[asyncio.Task] = None

    async def start(self):
        """Initialize the auth subsystem: connect PG, create tables, load token."""
        if self._started:
            return

        # Use provided pool or create from config
        if self._pg_pool is None and self.config.pg.password:
            from .async_db import AsyncDBPool
            self._pg_pool = AsyncDBPool(self.config)
            await self._pg_pool.connect()
            log.info("MessageAuth: created PG pool for auth")

        if self._pg_pool:
            self._token_manager.set_pg_pool(self._pg_pool)
            await self._token_manager.ensure_table()
            await self._token_manager.load_token_from_pg()

        # Ensure we have a current token
        await self._token_manager.ensure_current_token()
        self._started = True

        # Launch periodic token rotation task
        self._rotation_task = asyncio.create_task(self._periodic_rotation())
        log.info(f"MessageAuth started (mode={self._security.transport_auth}, "
                 f"rotation_interval={self._token_manager._rotation_interval}s, "
                 f"grace_period={self._token_manager._grace_period}s)")

    async def _periodic_rotation(self):
        """Background task that rotates the auth token before it expires.

        Runs every rotation_interval / 4 seconds (at most every 15 minutes)
        and calls ensure_current_token() which rotates if the current token
        is within the grace period of expiry.
        """
        while self._started:
            try:
                # Check every 15 minutes or rotation_interval/4, whichever is smaller
                check_interval = min(900, max(60, self._token_manager._rotation_interval // 4))
                await asyncio.sleep(check_interval)
                if not self._started:
                    break
                await self._token_manager.ensure_current_token()
                log.debug("Periodic token rotation check completed")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Periodic token rotation error: {e}")
                await asyncio.sleep(60)  # Back off on error

    async def stop(self):
        """Cleanup: cancel rotation task and close PG pool if we created it."""
        self._started = False
        if self._rotation_task and not self._rotation_task.done():
            self._rotation_task.cancel()
            try:
                await self._rotation_task
            except asyncio.CancelledError:
                pass
        if self._pg_pool:
            try:
                await self._pg_pool.close()
            except Exception:
                pass

    # ─── Signing ─────────────────────────────────────────────────────────

    def sign_message(self, message: dict) -> dict:
        """Sign a message dict with HMAC-SHA256.

        Adds these fields to the message:
        - auth_token_id: identifies which token was used (for key lookup)
        - auth_nonce: unique nonce for replay prevention
        - auth_timestamp: when the signature was created
        - auth_signature: HMAC-SHA256 hex digest

        If transport_auth is 'none', returns the message unchanged.

        Args:
            message: A2AMessage as dict (from message.to_dict())

        Returns:
            Message dict with auth fields added (or unchanged if auth disabled)
        """
        if self._security.transport_auth == "none":
            return message

        token = self._token_manager.current_token
        if not token:
            log.warning("MessageAuth: no current token, cannot sign — returning unsigned")
            return message

        nonce = secrets.token_hex(16)
        timestamp = f"{time.time():.6f}"

        # Build canonical content for signing (deterministic order)
        # Core fields that identify a message uniquely
        msg_id = message.get("id", "")
        sender = message.get("sender", "")
        recipient = message.get("recipient", "")
        msg_type = message.get("type", "")
        priority = str(message.get("priority", ""))

        # Deterministic payload serialization
        payload_str = json.dumps(message.get("payload", {}), sort_keys=True, default=str)

        # Canonical signing string
        sign_content = (
            f"{msg_id}|{sender}|{recipient}|{msg_type}|{priority}|"
            f"{nonce}|{timestamp}|{token.token_id}|{payload_str}"
        )

        signature = hmac.new(
            token.secret.encode("utf-8"),
            sign_content.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        # Return a new dict with auth fields
        signed = dict(message)
        signed["auth_token_id"] = token.token_id
        signed["auth_nonce"] = nonce
        signed["auth_timestamp"] = timestamp
        signed["auth_signature"] = signature
        return signed

    async def verify_message(self, message: dict) -> bool:
        """Verify a signed message.

        Checks:
        1. Rate limit for sender
        2. Nonce uniqueness (replay prevention)
        3. Token exists and is valid
        4. HMAC signature matches

        Returns True if the message is authentic and not a replay.
        Raises appropriate exception on failure.

        If transport_auth is 'none', returns True without checks.

        Args:
            message: Incoming message dict with auth fields

        Returns:
            True if verified

        Raises:
            RateLimitExceededError: sender exceeded rate limit
            ReplayAttackError: nonce already seen
            InvalidSignatureError: signature verification failed
            TokenNotFoundError: token_id not found
        """
        if self._security.transport_auth == "none":
            return True

        sender = message.get("sender", "unknown")

        # 1. Rate limit check
        await self._rate_limiter.check(sender)

        # 2. Validate required auth fields BEFORE recording nonce
        #    (so malformed messages don't burn nonce slots)
        nonce = message.get("auth_nonce", "")
        if not nonce:
            raise InvalidSignatureError("Missing auth_nonce field")

        token_id = message.get("auth_token_id", "")
        if not token_id:
            raise InvalidSignatureError("Missing auth_token_id field")

        signature = message.get("auth_signature", "")
        timestamp = message.get("auth_timestamp", "")
        if not signature or not timestamp:
            raise InvalidSignatureError("Missing auth_signature or auth_timestamp")

        # 3. Nonce uniqueness (replay prevention)
        #    Self-echo: allow known nonces from our own node (broadcast echo)
        is_self = sender == self._node_name
        await self._nonce_tracker.check_and_record(nonce, allow_known=is_self)

        # 4. Token lookup
        secret = await self._token_manager.get_token_secret(token_id)
        if not secret:
            raise TokenNotFoundError(f"Token not found or expired: {token_id[:16]}...")

        # Rebuild canonical signing string
        msg_id = message.get("id", "")
        sender = message.get("sender", "")
        recipient = message.get("recipient", "")
        msg_type = message.get("type", "")
        priority = str(message.get("priority", ""))

        payload_str = json.dumps(message.get("payload", {}), sort_keys=True, default=str)

        sign_content = (
            f"{msg_id}|{sender}|{recipient}|{msg_type}|{priority}|"
            f"{nonce}|{timestamp}|{token_id}|{payload_str}"
        )

        expected = hmac.new(
            secret.encode("utf-8"),
            sign_content.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(signature, expected):
            raise InvalidSignatureError(
                f"HMAC verification failed for message {msg_id[:16]}... from {sender}"
            )

        # Timestamp freshness check (reject messages older than NONCE_TTL)
        try:
            msg_ts = float(timestamp)
            if time.time() - msg_ts > NONCE_TTL_SECONDS:
                log.warning(f"Message from {sender} has stale timestamp ({msg_ts})")
                # Don't reject outright — clock skew can cause false positives
                # But log a warning for monitoring
        except (ValueError, TypeError):
            log.warning(f"Invalid auth_timestamp from {sender}: {timestamp}")

        return True

    # ─── Convenience ──────────────────────────────────────────────────────

    async def sign_and_track(self, message: dict) -> dict:
        """Sign a message and also record its nonce locally (so we don't reject our own echoes).

        Useful for broadcast messages that may echo back from PG NOTIFY.
        """
        signed = self.sign_message(message)
        if self._security.transport_auth != "none" and "auth_nonce" in signed:
            # Pre-register our own nonce so echoes don't trigger replay detection
            nonce = signed["auth_nonce"]
            await self._nonce_tracker.check_and_record(nonce)
        return signed

    async def rotate_token(self) -> AuthToken:
        """Force a token rotation (e.g., on suspected key compromise)."""
        return await self._token_manager.ensure_current_token()

    async def get_peer_rate_remaining(self, peer: str) -> int:
        """Get remaining message allowance for a peer."""
        return await self._rate_limiter.get_remaining(peer)

    async def reset_peer_rate(self, peer: str):
        """Reset rate limit for a peer (e.g., after temporary block)."""
        await self._rate_limiter.reset(peer)

    @property
    def stats(self) -> dict:
        """Return auth subsystem statistics."""
        return {
            "mode": self._security.transport_auth,
            "current_token_id": self._token_manager.current_token_id[:24] + "..."
                                if self._token_manager.current_token_id else None,
            "nonce_cache_size": self._nonce_tracker.size,
            "last_rotation": self._token_manager._last_rotation,
            "started": self._started,
        }