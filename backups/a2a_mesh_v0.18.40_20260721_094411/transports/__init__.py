"""A2A Mesh Transports — Base transport adapter interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional
import asyncio
import logging

log = logging.getLogger("a2a_mesh.transports")


@dataclass
class TransportStatus:
    available: bool
    latency_ms: float
    error: str = ""


class TransportAdapter(ABC):
    """Base class for all mesh transports.

    Each transport must implement:
    - start(): Initialize and start the transport
    - stop(): Clean shutdown
    - send(): Send a message
    - receive(): Poll for received messages
    - discover(): Find peer nodes
    - is_available(): Check if operational
    - get_status(): Return current status
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Transport name (pg_notify, p2p, http, ble, wifi_direct)."""

    @abstractmethod
    async def start(self) -> bool:
        """Initialize transport. Return True if started successfully."""

    @abstractmethod
    async def stop(self) -> bool:
        """Shutdown transport cleanly."""

    @abstractmethod
    async def send(self, message) -> 'SendResult':
        """Send a message. Return SendResult with success/failure."""

    @abstractmethod
    async def receive(self) -> list:
        """Poll for received messages (non-blocking)."""

    @abstractmethod
    async def discover(self) -> list:
        """Discover peer nodes via this transport."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if transport is currently operational."""

    @abstractmethod
    def get_status(self) -> TransportStatus:
        """Return current transport status."""

    def _log(self, msg: str):
        log.debug(f"[{self.name}] {msg}")