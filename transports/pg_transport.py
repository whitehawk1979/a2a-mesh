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
                        message = A2AMessage.from_dict(payload)
                        await self._incoming_queue.put((message, "pg_notify"))
                    except Exception as e:
                        log.error(f"Failed to parse NOTIFY payload: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"PG listen error: {e}")
                await asyncio.sleep(5)

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via PG NOTIFY."""
        if not self._available or not self._conn:
            return SendResult(transport="pg_notify", success=False, error="not connected")

        try:
            import psycopg2

            # Determine channel based on message type
            channel = self._get_channel(message)
            payload = json.dumps(message.to_dict(), default=str)

            # Use NOTIFY with payload (PG 9.0+)
            cur = self._conn.cursor()
            # Escape single quotes in payload
            escaped_payload = payload.replace("'", "''")
            cur.execute(f"NOTIFY {channel}, '{escaped_payload}'")
            self._conn.commit()
            cur.close()

            return SendResult(transport="pg_notify", success=True, latency_ms=0.5)

        except Exception as e:
            log.error(f"PG NOTIFY send failed: {e}")
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