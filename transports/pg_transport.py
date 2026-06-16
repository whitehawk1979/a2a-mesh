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
        self._listener_task = None
        self._running = False
        self._incoming_queue: asyncio.Queue = asyncio.Queue()
        self._channels = config.pg.channels if config else [
            "a2a_channel", "a2a_steer_channel", "delegation_channel", "mesh_channel"
        ]

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
            )
            self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            # Start listening on all channels
            cur = self._conn.cursor()
            for channel in self._channels:
                cur.execute(f"LISTEN {channel};")
                log.info(f"PG LISTEN on {channel}")
            cur.close()

            self._running = True
            self._available = True

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
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._available = False
        log.info("PG transport stopped")
        return True

    async def _listen_loop(self):
        """Async loop for PG NOTIFY processing."""
        import select

        while self._running:
            try:
                # Check for notifies (non-blocking with timeout)
                if select.select([self._conn], [], [], 1.0) == ([], [], []):
                    await asyncio.sleep(0.1)
                    continue

                self._conn.poll()
                while self._conn.notifies:
                    notify = self._conn.notifies.pop(0)
                    try:
                        payload = json.loads(notify.payload)
                        
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
                            else:
                                # Fallback: create message from NOTIFY payload
                                message = A2AMessage.create(
                                    sender=sender,
                                    recipient=recipient or "broadcast",
                                    msg_type=msg_type,
                                    payload=payload,
                                    priority=priority,
                                )
                                await self._incoming_queue.put((message, "pg_notify"))
                        else:
                            # Other channels (a2a_channel, a2a_steer_channel, etc.)
                            message = A2AMessage.from_dict(payload)
                            await self._incoming_queue.put((message, "pg_notify"))

                    except Exception as e:
                        log.error(f"Failed to parse NOTIFY payload: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"PG listen error: {e}")
                await asyncio.sleep(5)

    def _fetch_message(self, msg_id: str):
        """Fetch a full message from mesh.mesh_messages by ID."""
        if not self._conn:
            return None
        try:
            cur = self._conn.cursor()
            cur.execute("""
                SELECT id, sender, recipient, msg_type, priority, payload, 
                       routing_mode, src_addr, dst_addr
                FROM mesh.mesh_messages WHERE id = %s
            """, (msg_id,))
            row = cur.fetchone()
            cur.close()
            
            if row:
                msg = A2AMessage.create(
                    sender=row[1],
                    recipient=row[2],
                    msg_type=row[3],
                    payload=row[5] if isinstance(row[5], dict) else {},
                    priority=row[4],
                )
                return msg
        except Exception as e:
            log.error(f"Failed to fetch message {msg_id}: {e}")
        return None

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via PG — INSERT into mesh_messages (trigger sends NOTIFY)."""
        if not self._available or not self._conn:
            return SendResult(transport="pg_notify", success=False, error="not connected")

        try:
            import psycopg2

            # Insert into mesh.mesh_messages — the NOTIFY trigger will fire
            cur = self._conn.cursor()
            payload_json = json.dumps(message.payload, default=str)

            # Use routing mode from message or default
            routing_mode = getattr(message, 'routing_mode', 'hybrid')

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
                routing_mode,
                getattr(message, 'src_address', None),
                getattr(message, 'dst_address', None),
            ))
            self._conn.commit()
            cur.close()

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