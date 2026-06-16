"""A2A Mesh HTTP Transport — HTTP/MCP bridge transport.

Uses the existing MCP bridge for message delivery.
Fallback transport when PG and P2P are unavailable.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult

log = logging.getLogger("a2a_mesh.transports.http")


class HTTPTransport(TransportAdapter):
    """HTTP/MCP bridge transport.

    Sends messages via the MCP bridge HTTP API.
    Slower than PG/P2P but works from anywhere with HTTP access.
    """

    name = "http"

    def __init__(self, config):
        self.config = config
        self._available = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._url = config.http.url if config else "http://192.168.1.30:8199"
        self._timeout = config.http.timeout if config else 5
        self._retries = config.http.retries if config else 3

    async def start(self) -> bool:
        """Initialize HTTP session."""
        try:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)

            # Health check
            health_url = getattr(self.config.http, 'health_url', f"{self._url}/health")
            try:
                async with self._session.get(health_url) as resp:
                    if resp.status == 200:
                        self._available = True
                        log.info(f"HTTP transport started, bridge at {self._url}")
                        return True
                    else:
                        log.warning(f"HTTP health check returned {resp.status}")
                        self._available = True  # Still available, health check might be flaky
                        return True
            except Exception as e:
                log.warning(f"HTTP health check failed: {e}")
                self._available = True  # Assume available, will retry on send
                return True

        except Exception as e:
            log.error(f"HTTP transport start failed: {e}")
            return False

    async def stop(self) -> bool:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        self._available = False
        return True

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via HTTP/MCP bridge."""
        if not self._session:
            return SendResult(transport="http", success=False, error="not initialized")

        # Try sending with retries
        last_error = ""
        for attempt in range(self._retries):
            try:
                # Send via MCP bridge's A2A message endpoint
                url = f"{self._url}/mcp"
                payload = {
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {
                        "name": "a2a_send",
                        "arguments": {
                            "recipient": message.recipient,
                            "content": json.dumps(message.payload),
                            "subject": message.payload.get("subject", ""),
                            "message_type": message.type,
                            "priority": message.priority,
                        }
                    },
                    "id": 1
                }

                start_time = time.time()
                async with self._session.post(url, json=payload) as resp:
                    latency = (time.time() - start_time) * 1000
                    if resp.status == 200:
                        self._available = True
                        return SendResult(transport="http", success=True, latency_ms=latency)
                    else:
                        text = await resp.text()
                        last_error = f"HTTP {resp.status}: {text[:200]}"

            except asyncio.TimeoutError:
                last_error = f"timeout (attempt {attempt + 1}/{self._retries})"
            except Exception as e:
                last_error = str(e)

            # Wait before retry (exponential backoff)
            if attempt < self._retries - 1:
                await asyncio.sleep(2 ** attempt)

        self._available = False
        return SendResult(transport="http", success=False, error=last_error)

    async def receive(self) -> list:
        """HTTP transport doesn't receive — it's send-only (MCP bridge handles incoming)."""
        return []

    async def discover(self) -> list:
        """HTTP transport doesn't discover nodes."""
        return []

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        return TransportStatus(
            available=self._available,
            latency_ms=100.0 if self._available else float('inf'),
            error="" if self._available else "unavailable",
        )