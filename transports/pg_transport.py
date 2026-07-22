"""A2A Mesh PG Transport — PostgreSQL NOTIFY/LISTEN transport using asyncpg.

Uses asyncpg for truly async database operations. Messages are delivered via
PostgreSQL NOTIFY/LISTEN for push-based delivery (no polling).

Key improvements over psycopg2 version:
- asyncpg: no event loop blocking
- Native NOTIFY/LISTEN: push-based message delivery instead of polling
- Parameterized queries ($1, $2, ...): SQL injection prevention
- Payload validation: schema checks on incoming messages
- Ghost node pruning: heartbeat-based cleanup of stale nodes
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

import asyncpg

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult, MAX_MESSAGE_SIZE
from ..core.async_db import AsyncDBPool, validate_message_payload, MessageValidationError

log = logging.getLogger("a2a_mesh.transports.pg")


class PGTransport(TransportAdapter):
    """PostgreSQL NOTIFY/LISTEN transport using asyncpg.

    Leverages the shared PG database for instant message delivery via NOTIFY.
    This is the fastest transport (<1ms on same server).

    Uses asyncpg connection pool for all database operations, eliminating
    event loop blocking caused by synchronous psycopg2 calls.
    """

    name = "pg_notify"

    def __init__(self, config):
        self.config = config
        self._available = False
        self._pool: Optional[AsyncDBPool] = None
        self._listener_conn: Optional[asyncpg.Connection] = None
        self._listener_task = None
        self._running = False
        self._incoming_queue: asyncio.Queue = asyncio.Queue()
        self._channels = config.pg.channels if config else [
            "a2a_channel", "a2a_steer_channel", "delegation_channel", "mesh_channel", "diagnostic_channel"
        ]
        self._reconnect_count = 0
        self._max_reconnects = 5

    async def start(self) -> bool:
        """Start PG LISTEN connection using asyncpg.

        PG is optional — if no password or DB unreachable, gracefully degrade to P2P-only mode.
        """
        if not self.config.pg.password and not os.environ.get("A2A_MESH_PG_DSN"):
            log.info("PG transport disabled — no password configured (P2P-only mode)")
            self._available = False
            return False

        try:
            # Create asyncpg connection pool for all DB operations
            self._pool = AsyncDBPool(self.config)
            if not await self._pool.connect():
                log.error("Failed to create asyncpg connection pool")
                self._available = False
                return False

            # Acquire a dedicated listener connection for NOTIFY/LISTEN
            self._listener_conn = await self._pool._pool.acquire()
            for channel in self._channels:
                await self._listener_conn.execute(f"LISTEN {channel}")
                log.info(f"PG LISTEN on {channel}")

            self._running = True
            self._available = True

            # Start async listener task
            self._listener_task = asyncio.create_task(self._listen_loop())
            log.info("PG transport started (asyncpg + NOTIFY/LISTEN)")
            return True

        except Exception as e:
            log.error(f"PG transport start failed: {e}")
            self._available = False
            return False

    async def stop(self) -> bool:
        """Stop PG connections."""
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._listener_conn:
            try:
                # Remove listeners before releasing connection back to pool
                try:
                    self._listener_conn.remove_log_listener(self._log_listener)
                except Exception:
                    pass
                await self._pool._pool.release(self._listener_conn)
            except Exception:
                pass
            self._listener_conn = None
        if self._pool:
            await self._pool.close()
        self._available = False
        log.info("PG transport stopped")
        return True

    async def _listen_loop(self):
        """Async loop for PG NOTIFY processing using asyncpg native LISTEN.

        Unlike psycopg2 which required select() polling, asyncpg provides
        native async notification callbacks — no thread pool or polling needed.
        """
        notification_queue = asyncio.Queue()

        def _on_notification(connection, pid, channel, payload):
            """Callback invoked by asyncpg when a NOTIFY is received."""
            try:
                asyncio.get_event_loop().call_soon_threadsafe(
                    notification_queue.put_nowait, (channel, payload)
                )
            except RuntimeError:
                pass  # Event loop closed during shutdown

        # Register notification callback — store reference for cleanup
        self._log_listener = lambda conn, msg: None  # Suppress log noise
        self._listener_conn.add_log_listener(self._log_listener)

        # We need to use add_listener for each channel
        for channel in self._channels:
            await self._listener_conn.add_listener(channel, _on_notification)

        log.info("PG NOTIFY listener active (asyncpg native)")

        while self._running:
            try:
                # Wait for notifications with timeout for graceful shutdown
                try:
                    channel, payload = await asyncio.wait_for(
                        notification_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                await self._process_notification(channel, payload)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"PG listen error: {e}")
                # Try to reconnect
                if self._reconnect_count < self._max_reconnects:
                    self._reconnect_count += 1
                    backoff = min(5 * (2 ** (self._reconnect_count - 1)), 60)
                    log.info(f"PG reconnect attempt {self._reconnect_count}/{self._max_reconnects} in {backoff}s")
                    await asyncio.sleep(backoff)
                    try:
                        # Reset listener connection
                        if self._listener_conn:
                            try:
                                await self._pool._pool.release(self._listener_conn)
                            except Exception:
                                pass
                        self._listener_conn = await self._pool._pool.acquire()
                        for ch in self._channels:
                            await self._listener_conn.add_listener(ch, _on_notification)
                            await self._listener_conn.execute(f"LISTEN {ch}")
                        self._available = True
                        self._reconnect_count = 0
                        log.info("PG reconnected successfully")
                    except Exception as re:
                        log.error(f"PG reconnect failed: {re}")
                else:
                    await asyncio.sleep(5)

    async def _process_notification(self, channel: str, payload: str):
        """Process a single PG NOTIFY payload."""
        if not payload:
            return

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            # Handle bare UUID/string payloads (backward compatibility)
            raw = payload.strip() if payload else ""
            if channel == "mesh_channel" and raw:
                message = await self._fetch_message(raw)
                if message:
                    await self._incoming_queue.put((message, "pg_notify"))
                    log.info(f"Received mesh message {raw[:8]} (bare UUID)")
                return
            log.warning(f"Invalid NOTIFY payload on {channel}: {payload[:50]}")
            return

        # Validate the notification payload
        try:
            data = validate_message_payload(data)
        except MessageValidationError as e:
            log.warning(f"Invalid notification payload on {channel}: {e}")
            # Still process — validation is advisory, not a hard block
            pass

        try:
            if channel == "mesh_channel":
                msg_id = data.get("id")
                sender = data.get("sender", "unknown")
                recipient = data.get("recipient")

                # Fetch full message from DB
                message = await self._fetch_message(msg_id) if msg_id else None
                if message:
                    await self._incoming_queue.put((message, "pg_notify"))
                    log.info(f"Received mesh message {msg_id[:8]} from {sender} (PG NOTIFY)")
                else:
                    # Fallback: create message from NOTIFY payload
                    msg_data = {
                        "id": data.get("id") or msg_id,
                        "sender": sender,
                        "recipient": recipient or "broadcast",
                        "type": data.get("msg_type", "unknown"),
                        "payload": data,
                        "priority": data.get("priority", 5),
                    }
                    message = A2AMessage.from_dict(msg_data)
                    await self._incoming_queue.put((message, "pg_notify"))
                    log.info(f"Received mesh message (fallback) from {sender}")
            else:
                # Other channels (a2a_channel, a2a_steer_channel, etc.)
                message = A2AMessage.from_dict(data)
                await self._incoming_queue.put((message, "pg_notify"))
                log.info(f"Received A2A message on {channel} from {getattr(message, 'sender', '?')}")

        except Exception as e:
            log.error(f"Failed to process NOTIFY payload: {e}")

    async def _fetch_message(self, msg_id: str) -> Optional[A2AMessage]:
        """Fetch a full message from mesh.mesh_messages by ID (async).

        Uses parameterized query to prevent SQL injection.
        """
        if not self._pool or not self._pool.is_connected():
            return None
        try:
            row = await self._pool.fetchrow("""
                SELECT id, sender, recipient, msg_type, priority, payload,
                       routing_mode, src_addr, dst_addr
                FROM mesh.mesh_messages WHERE id = $1
            """, msg_id)

            if row:
                # Parse payload JSON string — PG stores it as TEXT, not JSONB
                payload_data = row['payload']
                if isinstance(payload_data, str):
                    try:
                        payload_data = json.loads(payload_data)
                    except (json.JSONDecodeError, TypeError):
                        log.warning(f"Failed to parse payload JSON for message {msg_id}: {str(payload_data)[:80] if payload_data else 'empty'}")
                        payload_data = {}
                elif not payload_data:
                    payload_data = {}

                msg_data = {
                    "id": row['id'],
                    "sender": row['sender'],
                    "recipient": row['recipient'],
                    "type": row['msg_type'],
                    "priority": row['priority'],
                    "payload": payload_data,
                    "routing_mode": row['routing_mode'] if row['routing_mode'] else "hybrid",
                    "src_address": row['src_addr'],
                    "dst_address": row['dst_addr'],
                }
                return A2AMessage.from_dict(msg_data)
        except Exception as e:
            log.error(f"Failed to fetch message {msg_id}: {e}")
        return None

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via PG — INSERT into mesh.mesh_messages (async).

        Uses parameterized queries ($1, $2, ...) for SQL injection prevention.
        NOTIFY triggers on mesh_channel deliver instant notifications to connected agents.
        """
        if not self._pool or not self._pool.is_connected():
            return SendResult(transport="pg_notify", success=False, error="not connected")

        try:
            # Validate message payload
            payload_data = message.payload
            if isinstance(payload_data, str):
                try:
                    payload_data = json.loads(payload_data)
                except (json.JSONDecodeError, TypeError):
                    payload_data = {}
            elif not isinstance(payload_data, dict):
                payload_data = {}

            # Validate size
            valid, size = message.validate_size()
            if not valid:
                return SendResult(transport="pg_notify", success=False,
                                  error=f"Message too large: {size} bytes")

            payload_json = json.dumps(payload_data, default=str, ensure_ascii=True)

            # Insert using parameterized query ($1, $2, ...)
            await self._pool.execute("""
                INSERT INTO mesh.mesh_messages
                    (id, sender, recipient, msg_type, priority, payload,
                     routing_mode, src_addr, dst_addr, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'sent')
            """,
                message.id,
                message.sender,
                message.recipient,
                message.type,
                getattr(message, 'priority', 5),
                payload_json,
                getattr(message, 'routing_mode', 'hybrid'),
                None,  # src_addr
                None,  # dst_addr
            )

            # Send PG NOTIFY for instant delivery
            try:
                notify_payload = json.dumps({
                    "id": str(message.id),
                    "sender": message.sender,
                    "recipient": message.recipient,
                    "msg_type": message.type,
                    "priority": message.priority,
                }, ensure_ascii=True)
                await self._pool.notify("mesh_channel", notify_payload)
            except Exception as notify_err:
                log.debug(f"PG NOTIFY failed (non-critical): {notify_err}")

            return SendResult(transport="pg_notify", success=True, latency_ms=1.0)

        except Exception as e:
            log.error(f"PG send failed: {e}")
            self._available = False
            return SendResult(transport="pg_notify", success=False, error=str(e))

    def _get_channel(self, message: A2AMessage) -> str:
        """Determine which PG channel to use."""
        if message.type in ("steer", "directive") and message.priority >= 10:
            return "a2a_steer_channel"
        elif message.type == "delegation":
            return "delegation_channel"
        elif message.type == "mesh":
            return "mesh_channel"
        else:
            return "a2a_channel"

    async def receive(self) -> list:
        """Get received messages from the queue."""
        messages = []
        qsize = self._incoming_queue.qsize()
        if qsize > 0:
            log.info(f"PG receive: {qsize} messages in incoming queue")
        while not self._incoming_queue.empty():
            msg, transport = await self._incoming_queue.get()
            messages.append((msg, transport))
        return messages

    async def discover(self) -> list:
        """PG transport doesn't discover nodes — uses shared DB."""
        return []

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        return TransportStatus(
            available=self._available,
            latency_ms=0.5 if self._available else float('inf'),
            error="" if self._available else "not connected",
        )