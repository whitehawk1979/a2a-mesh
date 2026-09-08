"""A2A Mesh HTTP Transport — HTTP bridge transport.

Sends messages via peer node dashboard API (/api/send).
Fallback transport when PG and P2P are unavailable.

Each peer has its own HTTP endpoint (dashboard port 8650 by default).
The transport uses the peer's /api/send endpoint for message delivery.
"""

import asyncio
import json
import logging
import time
from typing import Optional, Dict, List

import aiohttp

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult

log = logging.getLogger("a2a_mesh.transports.http")


class HTTPTransport(TransportAdapter):
    """HTTP dashboard bridge transport.

    Sends messages via peer node's /api/send endpoint.
    Works from anywhere with HTTP access to the peer dashboard.
    """

    name = "http"

    def __init__(self, config):
        self.config = config
        self._available = False
        self._session: Optional[aiohttp.ClientSession] = None
        # Main bridge URL (for health check) — defaults to Morzsa dashboard
        self._url = config.http.url if config else "http://192.168.1.30:8650"
        self._health_url = getattr(config.http, 'health_url', f"{self._url}/health") if config else f"{self._url}/health"
        self._timeout = config.http.timeout if config else 10
        self._retries = config.http.retries if config else 3
        # Peer HTTP endpoints — populated from discovery/config
        self._peer_urls: Dict[str, str] = {}  # node_name -> http_url
        # Auth token cache for peer dashboards
        self._peer_tokens: Dict[str, str] = {}

    async def start(self) -> bool:
        """Initialize HTTP session and check bridge availability."""
        try:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)

            # Health check — try /api/health first, fallback to /health
            for health_path in ["/api/health", "/health"]:
                health_url = self._url.rstrip("/") + health_path
                try:
                    async with self._session.get(health_url) as resp:
                        if resp.status == 200:
                            self._available = True
                            log.info(f"HTTP transport started, bridge at {self._url}")
                            return True
                except Exception:
                    continue

            # If health check failed, still mark as available — peer may not have /health
            # but /api/send might work. We'll discover on first send.
            log.warning(f"HTTP health check failed for {self._url}, assuming available")
            self._available = True
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

    async def health_check(self) -> bool:
        """Periodic health check for HTTP bridge availability."""
        if not self._session or self._session.closed:
            return False
        for health_path in ["/api/health", "/health"]:
            health_url = self._url.rstrip("/") + health_path
            try:
                async with self._session.get(health_url) as resp:
                    if resp.status == 200:
                        self._available = True
                        return True
            except Exception:
                continue
        # Don't mark unavailable on health check failure — peer might be temporarily busy
        # Only mark unavailable on actual send failures
        return self._available

    def register_peer_url(self, node_name: str, url: str):
        """Register a peer node's HTTP dashboard URL."""
        self._peer_urls[node_name.lower()] = url.rstrip("/")
        log.debug(f"HTTP peer registered: {node_name} -> {url}")

    def _get_peer_url(self, recipient: str) -> str:
        """Get the HTTP URL for a peer node, or fall back to main bridge."""
        return self._peer_urls.get(recipient.lower(), self._url.rstrip("/"))

    async def _get_peer_token(self, peer_url: str) -> str:
        """No longer needed — we use X-Mesh-Token header instead of user auth.
        Kept for backward compatibility but returns empty string."""
        return ""

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via peer's /api/send endpoint using X-Mesh-Token."""
        if not self._session:
            return SendResult(transport="http", success=False, error="not initialized")

        # Determine target URL
        if message.recipient and message.recipient != "broadcast":
            target_url = self._get_peer_url(message.recipient)
        else:
            target_url = self._url.rstrip("/")

        # Use mesh-internal shared secret — no user login needed
        headers = {"X-Mesh-Token": "mesh-wake-secret-2026"}

        # Build payload for /api/send
        payload = {
            "recipient": message.recipient,
            "msg_type": message.type,
            "text": json.dumps(message.payload) if isinstance(message.payload, dict) else str(message.payload),
            "priority": message.priority,
        }

        last_error = ""
        for attempt in range(self._retries):
            try:
                url = target_url + "/api/send"
                start_time = time.time()
                async with self._session.post(url, json=payload, headers=headers) as resp:
                    latency = (time.time() - start_time) * 1000
                    if resp.status == 200:
                        self._available = True
                        return SendResult(transport="http", success=True, latency_ms=latency)
                    elif resp.status == 401:
                        # Should not happen with X-Mesh-Token — but handle gracefully
                        last_error = f"HTTP 401 (X-Mesh-Token rejected, attempt {attempt + 1})"
                    elif resp.status == 429:
                        last_error = f"HTTP 429 (rate limited, attempt {attempt + 1})"
                        await asyncio.sleep(2 ** attempt)
                    else:
                        text = await resp.text()
                        last_error = f"HTTP {resp.status}: {text[:200]}"
            except asyncio.TimeoutError:
                last_error = f"timeout (attempt {attempt + 1}/{self._retries})"
            except aiohttp.ClientConnectorError as e:
                last_error = f"connection refused: {e}"
                self._available = False
            except Exception as e:
                last_error = str(e)

            if attempt < self._retries - 1:
                await asyncio.sleep(2 ** attempt)

        if last_error and "timeout" not in last_error and "401" not in last_error:
            self._available = False
        return SendResult(transport="http", success=False, error=last_error)

    async def receive(self) -> list:
        """HTTP transport doesn't receive — it's send-only (peer dashboard handles incoming)."""
        return []

    async def discover(self) -> list:
        """HTTP transport doesn't discover nodes — that's P2P/PG's job."""
        return []

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        return TransportStatus(
            available=self._available,
            latency_ms=100.0 if self._available else 1e6,
            error="" if self._available else "unavailable",
        )