"""A2A Mesh PG Transport — PostgreSQL NOTIFY-based transport.

Uses the existing PG connection for message delivery via LISTEN/NOTIFY.
This is the primary (fastest) transport for agents on the same PG instance.
"""

import asyncio
import json
import logging
import time
from typing import Optional

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult

log = logging.getLogger("a2a_mesh.transports.pg")


class PGTransport(TransportAdapter):
    """PostgreSQL LISTEN/NOTIFY transport.

    Leverages the existing shared PG database for instant message delivery.
    This is the fastest transport (<1ms on same server).
    """

    name = "pg_notify"

    def __init__(self, config):
        self.config = config
        self._available = False
        self._conn = None
        self._write_conn = None  # Separate connection for writes
        self._listener_task = None
        self._running = False
        self._incoming_queue: asyncio.Queue = asyncio.Queue()
        self._channels = config.pg.channels if config else [
            "a2a_channel", "a2a_steer_channel", "delegation_channel", "mesh_channel"
        ]
        self._reconnect_count = 0
        self._max_reconnects = 5

    async def start(self) -> bool:
        """Start PG LISTEN connection."""
        try:
            import psycopg2
            import psycopg2.extensions

            self._conn = psycopg2.connect(
                host=self.config.pg.host,
                port=self.config.pg.port,
                dbname=self.config.pg.dbname,
                user=self.config.pg.user,
                password=self.config.pg.password,
                async_=0,  # Synchronous for notify
                options="-c client_encoding=UTF8",
            )
            self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            # Force UTF8 client encoding for SQL_ASCII database compatibility
            cur = self._conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.close()

            # Start listening on all channels
            cur = self._conn.cursor()
            for channel in self._channels:
                cur.execute(f"LISTEN {channel};")
                log.info(f"PG LISTEN on {channel}")
            cur.close()

            self._running = True
            self._available = True

            # Create separate write connection
            try:
                self._write_conn = psycopg2.connect(
                    host=self.config.pg.host,
                    port=self.config.pg.port,
                    dbname=self.config.pg.dbname,
                    user=self.config.pg.user,
                    password=self.config.pg.password,
                    options="-c client_encoding=UTF8",
                )
                self._write_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                # Force UTF8 client encoding for write connection too
                wcur = self._write_conn.cursor()
                wcur.execute("SET client_encoding TO UTF8")
                wcur.close()
                log.info("PG write connection established")
            except Exception as e:
                log.warning(f"PG write connection failed (will use listener conn): {e}")
                self._write_conn = None

            # Start async listener
            self._listener_task = asyncio.create_task(self._listen_loop())
            log.info("PG transport started")
            return True

        except Exception as e:
            log.error(f"PG transport start failed: {e}")
            self._available = False
            return False

    async def stop(self) -> bool:
        """Stop PG connection."""
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
        for conn in (self._conn, self._write_conn):
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        self._conn = None
        self._write_conn = None
        self._available = False
        log.info("PG transport stopped")
        return True

    async def _listen_loop(self):
        """Async loop for PG NOTIFY processing using run_in_executor."""
        import select

        loop = asyncio.get_event_loop()

        while self._running:
            try:
                # Use run_in_executor to avoid blocking the event loop
                readable = await loop.run_in_executor(
                    None,  # default thread pool
                    lambda: select.select([self._conn], [], [], 1.0)
                )
                if readable == ([], [], []):
                    await asyncio.sleep(0.05)
                    continue

                self._conn.poll()
                while self._conn.notifies:
                    notify = self._conn.notifies.pop(0)
                    try:
                        payload = json.loads(notify.payload)
                    except json.JSONDecodeError:
                        # Handle bare UUID/string payloads (backward compatibility)
                        raw = notify.payload.strip() if notify.payload else ""
                        if notify.channel == "mesh_channel" and raw:
                            # Bare UUID — treat as message ID for DB lookup
                            log.debug(f"Received bare UUID on mesh_channel: {raw[:8]}")
                            message = self._fetch_message(raw)
                            if message:
                                await self._incoming_queue.put((message, "pg_notify"))
                                log.info(f"Received mesh message {raw[:8]} (bare UUID)")
                            continue
                        log.warning(f"Invalid NOTIFY payload on {notify.channel}: {notify.payload[:50]}")
                        continue
                        
                        # Handle different channels
                        if notify.channel == "mesh_channel":
                            # Full message from mesh_messages INSERT trigger
                            msg_id = payload.get("id")
                            msg_type = payload.get("msg_type", "unknown")
                            sender = payload.get("sender", "unknown")
                            recipient = payload.get("recipient")
                            priority = payload.get("priority", 5)
                            
                            # Fetch full message from DB
                            message = self._fetch_message(msg_id)
                            if message:
                                await self._incoming_queue.put((message, "pg_notify"))
                                log.info(f"Received mesh message {msg_id[:8]} from {sender} (PG NOTIFY)")
                            else:
                                # Fallback: create message from NOTIFY payload, preserving original ID
                                fallback_id = payload.get("id") or msg_id
                                msg_data = {
                                    "id": fallback_id,
                                    "sender": sender,
                                    "recipient": recipient or "broadcast",
                                    "type": msg_type,
                                    "payload": payload,
                                    "priority": priority,
                                }
                                message = A2AMessage.from_dict(msg_data)
                                await self._incoming_queue.put((message, "pg_notify"))
                                log.info(f"Received mesh message (fallback) from {sender}")
                        else:
                            # Other channels (a2a_channel, a2a_steer_channel, etc.)
                            message = A2AMessage.from_dict(payload)
                            await self._incoming_queue.put((message, "pg_notify"))
                            log.info(f"Received A2A message on {notify.channel} from {getattr(message, 'sender', '?')}")

                    except Exception as e:
                        log.error(f"Failed to parse NOTIFY payload: {e}")

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
                        if self._conn:
                            try:
                                self._conn.close()
                            except Exception:
                                pass
                        self._conn = psycopg2.connect(
                            host=self.config.pg.host,
                            port=self.config.pg.port,
                            dbname=self.config.pg.dbname,
                            user=self.config.pg.user,
                            password=self.config.pg.password,
                            async_=0,
                            options="-c client_encoding=UTF8",
                        )
                        self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                        # Force UTF8 client encoding after reconnect
                        cur = self._conn.cursor()
                        cur.execute("SET client_encoding TO UTF8")
                        cur.close()
                        cur = self._conn.cursor()
                        for channel in self._channels:
                            cur.execute(f"LISTEN {channel};")
                        cur.close()
                        self._available = True
                        self._reconnect_count = 0
                        log.info("PG reconnected successfully")
                    except Exception as re:
                        log.error(f"PG reconnect failed: {re}")
                else:
                    await asyncio.sleep(5)

    def _fetch_message(self, msg_id: str):
        """Fetch a full message from mesh.mesh_messages by ID (mesh-only memory)."""
        if not self._conn:
            return None
        try:
            cur = self._conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                SELECT id, sender, recipient, msg_type, priority, payload, 
                       routing_mode, src_addr, dst_addr
                FROM mesh.mesh_messages WHERE id = %s
            """, (msg_id,))
            row = cur.fetchone()
            cur.close()
            
            if row:
                # Use A2AMessage.from_dict to preserve the original message ID
                # instead of A2AMessage.create() which generates a new UUID.
                # This is critical for dedup — the message ID must match across
                # all transports to prevent duplicate processing.
                msg_data = {
                    "id": row[0],           # Preserve original ID!
                    "sender": row[1],
                    "recipient": row[2],
                    "type": row[3],
                    "priority": row[4],
                    "payload": row[5] if isinstance(row[5], dict) else {},
                    "routing_mode": row[6] if row[6] else "hybrid",
                    "src_address": row[7],
                    "dst_address": row[8],
                }
                msg = A2AMessage.from_dict(msg_data)
                return msg
        except Exception as e:
            log.error(f"Failed to fetch message {msg_id}: {e}")
        return None

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via PG — INSERT into mesh.mesh_messages (mesh-only memory).
        
        Uses mesh.mesh_messages as the sole message store. NOTIFY triggers
        on mesh_channel deliver instant notifications to connected agents.
        No dependency on shared_a2a_memory — mesh-only synchronization.
        """
        conn = self._write_conn or self._conn
        if not self._available or not conn:
            return SendResult(transport="pg_notify", success=False, error="not connected")

        try:
            import psycopg2

            # Check connection health
            if conn.closed:
                log.warning("PG connection closed, marking unavailable")
                self._available = False
                return SendResult(transport="pg_notify", success=False, error="connection closed")

            # Insert into mesh.mesh_messages — mesh-only memory mode
            # SQL_ASCII database requires explicit client_encoding SET + INSERT in same transaction
            old_isolation = conn.isolation_level
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            payload_json = json.dumps(message.payload, default=str, ensure_ascii=True)

            cur.execute("""
                INSERT INTO mesh.mesh_messages 
                    (id, sender, recipient, msg_type, priority, payload, 
                     routing_mode, src_addr, dst_addr, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'sent')
            """, (
                message.id,
                message.sender,
                message.recipient,
                message.type,
                getattr(message, 'priority', 5),
                payload_json,
                getattr(message, 'routing_mode', 'hybrid'),
                None,  # src_addr
                None,  # dst_addr
            ))
            conn.commit()

            # Send PG NOTIFY so all agents receive the message immediately
            try:
                notify_payload = json.dumps({
                    "id": str(message.id),
                    "sender": message.sender,
                    "recipient": message.recipient,
                    "msg_type": message.type,
                    "priority": message.priority,
                }, ensure_ascii=True)
                cur2 = conn.cursor()
                cur2.execute("NOTIFY mesh_channel, %s", (notify_payload,))
                conn.commit()
                cur2.close()
            except Exception as notify_err:
                log.debug(f"PG NOTIFY failed (non-critical): {notify_err}")

            cur.close()
            # Restore autocommit for subsequent operations
            conn.set_isolation_level(old_isolation)

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