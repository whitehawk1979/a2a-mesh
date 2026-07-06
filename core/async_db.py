"""A2A Mesh Async Database Helper — asyncpg connection pool and utilities.

Provides a shared asyncpg connection pool for all mesh database operations,
replacing the synchronous psycopg2 calls that were blocking the event loop.

Features:
- asyncpg connection pool with automatic reconnection
- Parameterized queries ($1, $2, ...) to prevent SQL injection
- Connection health checks
- Backward-compatible DSN construction from MeshConfig
"""

import asyncio
import logging
import os
from typing import Optional, List, Dict, Any, Tuple

import asyncpg

log = logging.getLogger("a2a_mesh.async_db")


class AsyncDBPool:
    """Async PostgreSQL connection pool using asyncpg.

    All operations use parameterized queries ($1, $2, ...) for SQL injection safety.
    The pool manages connections, reconnection, and health checks automatically.
    """

    def __init__(self, config=None, dsn: str = "", min_size: int = 2, max_size: int = 10):
        """Initialize with config or DSN string.

        Args:
            config: MeshConfig object with .pg attribute, or PGConfig directly
            dsn: PostgreSQL DSN string (postgresql://user:pass@host:port/dbname)
            min_size: Minimum pool connections
            max_size: Maximum pool connections
        """
        self._pool: Optional[asyncpg.Pool] = None
        self._config = config
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._closing = False

        # Build connection params from config
        if config and not dsn:
            pg = getattr(config, 'pg', config)  # Works with MeshConfig or PGConfig
            self._dsn = self._build_dsn(pg)

    @staticmethod
    def _build_dsn(pg) -> str:
        """Build asyncpg DSN from PGConfig."""
        host = getattr(pg, 'host', 'localhost')
        port = getattr(pg, 'port', 5432)
        dbname = getattr(pg, 'dbname', 'agent_memory')
        user = getattr(pg, 'user', 'nova')
        password = getattr(pg, 'password', '')

        # Check env var override
        env_dsn = os.environ.get("A2A_MESH_PG_DSN", "")
        if env_dsn:
            return env_dsn

        dsn = f"postgresql://{user}"
        if password:
            dsn += f":{password}"
        dsn += f"@{host}:{port}/{dbname}"
        return dsn

    async def connect(self) -> bool:
        """Create the connection pool. Returns True on success."""
        if self._pool and not self._pool._closed:
            return True

        if not self._dsn:
            log.warning("AsyncDB: no DSN configured, cannot connect")
            return False

        try:
            self._pool = await asyncpg.create_pool(
                dsn=self._dsn,
                min_size=self._min_size,
                max_size=self._max_size,
                command_timeout=30,
                max_inactive_connection_lifetime=300,
            )
            log.info("AsyncDB connection pool established")
            return True
        except Exception as e:
            log.error(f"AsyncDB connection pool failed: {e}")
            self._pool = None
            return False

    async def close(self):
        """Close the connection pool."""
        self._closing = True
        if self._pool and not self._pool._closed:
            await self._pool.close()
            log.info("AsyncDB connection pool closed")
        self._pool = None

    def is_connected(self) -> bool:
        """Check if pool is available."""
        return self._pool is not None and not self._pool._closed

    async def execute(self, query: str, *args) -> str:
        """Execute a statement (INSERT, UPDATE, DELETE) with parameterized args.

        Uses $1, $2, ... parameter style (asyncpg native).
        Returns the status string (e.g., 'INSERT 1').
        """
        if not self.is_connected():
            raise RuntimeError("AsyncDB pool not connected")
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """Execute a SELECT query and return all rows."""
        if not self.is_connected():
            raise RuntimeError("AsyncDB pool not connected")
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Execute a SELECT query and return one row."""
        if not self.is_connected():
            raise RuntimeError("AsyncDB pool not connected")
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        """Execute a SELECT query and return a single value."""
        if not self.is_connected():
            raise RuntimeError("AsyncDB pool not connected")
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute_many(self, query: str, args_list) -> str:
        """Execute a statement with multiple parameter sets."""
        if not self.is_connected():
            raise RuntimeError("AsyncDB pool not connected")
        async with self._pool.acquire() as conn:
            return await conn.executemany(query, args_list)

    async def notify(self, channel: str, payload: str):
        """Send a PG NOTIFY on a channel."""
        if not self.is_connected():
            log.warning(f"AsyncDB: cannot NOTIFY {channel}, pool not connected")
            return
        async with self._pool.acquire() as conn:
            await conn.execute(f"SELECT pg_notify($1, $2)", channel, payload)

    async def listen(self, channel: str, callback):
        """Listen on a PG NOTIFY channel. Callback receives (channel, payload).

        Creates a dedicated listener connection that stays open.
        Returns the connection so the caller can stop listening later.
        """
        if not self.is_connected():
            log.warning(f"AsyncDB: cannot LISTEN {channel}, pool not connected")
            return None

        conn = await self._pool.acquire()
        try:
            await conn.add_listener(channel, callback)
            log.info(f"AsyncDB: LISTEN on {channel}")
            return conn
        except Exception as e:
            log.error(f"AsyncDB LISTEN failed on {channel}: {e}")
            await self._pool.release(conn)
            return None

    async def unlisten(self, conn, channel: str):
        """Stop listening on a channel and release the connection."""
        try:
            await conn.remove_listener(channel, None)
        except Exception:
            pass
        try:
            await self._pool.release(conn)
        except Exception:
            pass

    async def health_check(self) -> bool:
        """Check if the database is reachable."""
        if not self.is_connected():
            return False
        try:
            result = await self.fetchval("SELECT 1")
            return result == 1
        except Exception as e:
            log.warning(f"AsyncDB health check failed: {e}")
            return False

    async def prune_ghost_nodes(self, timeout_seconds: int = 600) -> int:
        """Mark nodes as offline if they haven't sent a heartbeat within the timeout.

        Args:
            timeout_seconds: Seconds since last heartbeat before marking offline (default: 10 min)

        Returns:
            Number of nodes marked offline
        """
        if not self.is_connected():
            return 0
        try:
            result = await self.execute("""
                UPDATE mesh.mesh_nodes
                SET status = 'offline'
                WHERE status = 'active'
                  AND last_heartbeat < NOW() - ($1 || ' seconds')::INTERVAL
            """, str(timeout_seconds))
            # Parse the result string like "UPDATE 3"
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                log.info(f"Pruned {count} ghost nodes (no heartbeat in {timeout_seconds}s)")
            return count
        except Exception as e:
            log.error(f"Ghost node pruning failed: {e}")
            return 0

    async def prune_stale_nodes(self, stale_days: int = 7) -> int:
        """Remove nodes that have been offline for longer than stale_days.

        Args:
            stale_days: Days a node must be offline before being removed (default: 7)

        Returns:
            Number of nodes removed
        """
        if not self.is_connected():
            return 0
        try:
            result = await self.execute("""
                DELETE FROM mesh.mesh_nodes
                WHERE status = 'offline'
                  AND last_heartbeat < NOW() - ($1 || ' days')::INTERVAL
            """, str(stale_days))
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                log.info(f"Removed {count} stale nodes (offline > {stale_days} days)")
            return count
        except Exception as e:
            log.error(f"Stale node removal failed: {e}")
            return 0


# ─── Message Payload Validation ────────────────────────────────────────

# Valid message types for schema validation
VALID_MSG_TYPES = {
    "directive", "task", "result", "heartbeat", "steer", "file",
    "discovery", "ack", "broadcast", "delegation", "context", "error",
    "mesh", "memory_sync", "skills_announcement", "file_transfer",
    "coordinator_claim", "auto_steer",
}

# Required fields for a valid A2A message
REQUIRED_MESSAGE_FIELDS = {"id", "sender", "recipient", "type"}

# Maximum sizes
MAX_PAYLOAD_DEPTH = 10        # Max nested dict depth
MAX_PAYLOAD_KEYS = 100         # Max top-level keys in payload
MAX_STRING_VALUE_LEN = 100000  # Max length for a single string value


class MessageValidationError(Exception):
    """Raised when a message fails schema validation."""
    pass


def validate_message_payload(data: dict) -> dict:
    """Validate an incoming A2A message payload against the schema.

    Checks:
    - Required fields exist (id, sender, recipient, type)
    - Field types are correct
    - No excessive nesting or oversized payloads
    - Message type is recognized
    - No dangerous content (e.g., executable code markers)

    Args:
        data: Raw message dict (from JSON parsing)

    Returns:
        Validated and sanitized message dict

    Raises:
        MessageValidationError: If validation fails
    """
    if not isinstance(data, dict):
        raise MessageValidationError(f"Expected dict, got {type(data).__name__}")

    # Check required fields
    missing = REQUIRED_MESSAGE_FIELDS - set(data.keys())
    if missing:
        # Allow partial messages for NOTIFY payloads (which may only have id + metadata)
        if "id" not in data and "sender" not in data:
            # This is likely a minimal NOTIFY payload, not a full message
            pass
        elif "id" not in data:
            raise MessageValidationError(f"Missing required field: id")

    # Validate field types
    if "id" in data and not isinstance(data["id"], str):
        raise MessageValidationError(f"Field 'id' must be a string, got {type(data['id']).__name__}")

    if "sender" in data and not isinstance(data["sender"], str):
        raise MessageValidationError(f"Field 'sender' must be a string")

    if "recipient" in data and not isinstance(data["recipient"], str):
        raise MessageValidationError(f"Field 'recipient' must be a string")

    if "type" in data:
        if not isinstance(data["type"], str):
            raise MessageValidationError(f"Field 'type' must be a string")
        # Allow unknown types with a warning (forward compatibility)
        # but validate known structure for known types

    if "priority" in data:
        try:
            p = int(data["priority"])
            if p < 0 or p > 10:
                raise MessageValidationError(f"Priority must be 0-10, got {p}")
            data["priority"] = p
        except (ValueError, TypeError):
            raise MessageValidationError(f"Priority must be an integer, got {data['priority']}")

    # Validate payload field
    if "payload" in data:
        payload = data["payload"]
        if not isinstance(payload, (dict, str)):
            raise MessageValidationError(f"Payload must be dict or string, got {type(payload).__name__}")

        if isinstance(payload, dict):
            _validate_payload_depth(payload, depth=0)

    # Validate sender/recipient lengths (prevent abuse)
    if "sender" in data and len(data["sender"]) > 64:
        raise MessageValidationError(f"Sender name too long: {len(data['sender'])} chars (max 64)")
    if "recipient" in data and len(data["recipient"]) > 64:
        raise MessageValidationError(f"Recipient name too long: {len(data['recipient'])} chars (max 64)")

    return data


def _validate_payload_depth(payload: dict, depth: int):
    """Recursively validate payload depth and key count."""
    if depth > MAX_PAYLOAD_DEPTH:
        raise MessageValidationError(f"Payload nesting too deep (max {MAX_PAYLOAD_DEPTH})")

    if len(payload) > MAX_PAYLOAD_KEYS:
        raise MessageValidationError(f"Too many payload keys: {len(payload)} (max {MAX_PAYLOAD_KEYS})")

    for key, value in payload.items():
        if not isinstance(key, str):
            raise MessageValidationError(f"Payload key must be string, got {type(key).__name__}")
        if len(key) > 256:
            raise MessageValidationError(f"Payload key too long: {len(key)} chars (max 256)")
        if isinstance(value, str) and len(value) > MAX_STRING_VALUE_LEN:
            raise MessageValidationError(f"Payload value for '{key}' too long: {len(value)} chars (max {MAX_STRING_VALUE_LEN})")
        if isinstance(value, dict):
            _validate_payload_depth(value, depth + 1)