"""A2A Mesh Stream Multiplexer — Content-based message routing.

Inspired by gensyn-ai/axl's Stream interface, this module provides a clean,
extensible pattern for routing messages based on content type rather than
transport. Each Stream registers with a stream_id and pattern-matches
incoming raw messages to determine routing.

Architecture:
    Raw bytes → Multiplexer → Stream.matches() → Stream.forward()
    
    Streams:
    - A2AStream: Agent-to-agent protocol messages (type=a2a_message, directive, task, result)
    - MeshControlStream: Mesh infrastructure messages (heartbeat, discovery, election, ack)
    - FileTransferStream: File chunk messages (type=file)
    - DashboardStream: Dashboard/web messages (source=web_dashboard)
    - DelegationStream: Task delegation messages (type=delegation)
    
    Unmatched messages fall through to the default handler.
"""

import json
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field

log = logging.getLogger("a2a_mesh.stream_mux")

# ─── Stream Interface ────────────────────────────────────────────

class MessageStream(ABC):
    """Abstract base class for content-based message streams.
    
    Each stream handles a specific category of messages based on content
    rather than transport. New protocols are added by implementing this
    interface and registering with the multiplexer.
    """
    
    @property
    @abstractmethod
    def stream_id(self) -> str:
        """Unique identifier for this stream (e.g., 'a2a', 'mesh_control', 'file_transfer')."""
        ...
    
    @abstractmethod
    def matches(self, raw_data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Check if this stream should handle the given raw message.
        
        Args:
            raw_data: Raw message bytes (may be JSON, msgpack, or binary)
            metadata: Optional metadata dict (transport name, peer info, etc.)
            
        Returns:
            True if this stream should handle the message
        """
        ...
    
    @abstractmethod
    async def forward(self, message: Any, from_peer: str, metadata: Optional[Dict] = None) -> Optional[Any]:
        """Process and forward the message through this stream.
        
        Args:
            message: Parsed A2AMessage or raw data
            from_peer: Source peer identifier
            metadata: Optional metadata dict
            
        Returns:
            Response data if this is a request/response pattern, None for fire-and-forget
        """
        ...


# ─── Built-in Stream Implementations ─────────────────────────────

class A2AStream(MessageStream):
    """Agent-to-agent protocol messages: directives, tasks, results, context sharing."""
    
    @property
    def stream_id(self) -> str:
        return "a2a"
    
    def matches(self, raw_data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Match A2A protocol messages (type field is agent communication type)."""
        try:
            if isinstance(raw_data, bytes):
                data = json.loads(raw_data.decode('utf-8'))
            elif isinstance(raw_data, dict):
                data = raw_data
            else:
                return False
            
            msg_type = data.get("type", data.get("msg_type", ""))
            # A2A messages are agent communication types
            a2a_types = {"directive", "task", "result", "a2a_message", "delegation", 
                        "context", "broadcast", "agent_reply", "steer"}
            return msg_type in a2a_types or data.get("a2a", False)
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            return False
    
    async def forward(self, message: Any, from_peer: str, metadata: Optional[Dict] = None) -> Optional[Any]:
        """Forward A2A messages to registered handlers."""
        log.debug(f"A2A stream forwarding message from {from_peer}")
        # Handlers are registered via the multiplexer
        return None


class MeshControlStream(MessageStream):
    """Mesh infrastructure messages: heartbeats, discovery, election, ACK."""
    
    @property
    def stream_id(self) -> str:
        return "mesh_control"
    
    def matches(self, raw_data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Match mesh control messages."""
        try:
            if isinstance(raw_data, bytes):
                data = json.loads(raw_data.decode('utf-8'))
            elif isinstance(raw_data, dict):
                data = raw_data
            else:
                return False
            
            msg_type = data.get("type", data.get("msg_type", ""))
            control_types = {"heartbeat", "discovery", "election", "ack", "mesh", 
                           "ping", "pong", "join", "leave", "topology"}
            return msg_type in control_types
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            return False
    
    async def forward(self, message: Any, from_peer: str, metadata: Optional[Dict] = None) -> Optional[Any]:
        """Forward mesh control messages."""
        log.debug(f"Mesh control stream forwarding from {from_peer}")
        return None


class FileTransferStream(MessageStream):
    """File transfer messages: chunk delivery, acknowledgments."""
    
    @property
    def stream_id(self) -> str:
        return "file_transfer"
    
    def matches(self, raw_data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Match file transfer messages."""
        try:
            if isinstance(raw_data, bytes):
                data = json.loads(raw_data.decode('utf-8'))
            elif isinstance(raw_data, dict):
                data = raw_data
            else:
                return False
            
            msg_type = data.get("type", data.get("msg_type", ""))
            return msg_type == "file" or data.get("file_transfer", False)
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            return False
    
    async def forward(self, message: Any, from_peer: str, metadata: Optional[Dict] = None) -> Optional[Any]:
        """Forward file transfer messages."""
        log.debug(f"File transfer stream forwarding from {from_peer}")
        return None


class DashboardStream(MessageStream):
    """Dashboard/web-originated messages."""
    
    @property
    def stream_id(self) -> str:
        return "dashboard"
    
    def matches(self, raw_data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Match dashboard-originated messages."""
        try:
            if isinstance(raw_data, bytes):
                data = json.loads(raw_data.decode('utf-8'))
            elif isinstance(raw_data, dict):
                data = raw_data
            else:
                return False
            
            payload = data.get("payload", {})
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    return False
            return isinstance(payload, dict) and payload.get("source") == "web_dashboard"
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            return False
    
    async def forward(self, message: Any, from_peer: str, metadata: Optional[Dict] = None) -> Optional[Any]:
        """Forward dashboard messages."""
        log.debug(f"Dashboard stream forwarding from {from_peer}")
        return None


# ─── Multiplexer ─────────────────────────────────────────────────

class StreamMultiplexer:
    """Content-based message multiplexer inspired by AXL's Stream interface.
    
    Routes incoming messages to the appropriate stream based on content
    matching, not transport type. New streams are registered and automatically
    tried in order. Unmatched messages fall through to a default handler.
    
    Usage:
        mux = StreamMultiplexer()
        mux.register(A2AStream())
        mux.register(MeshControlStream())
        mux.register(FileTransferStream())
        
        # In receive loop:
        stream = mux.match(raw_data, metadata)
        if stream:
            await stream.forward(message, from_peer, metadata)
    """
    
    def __init__(self):
        self._streams: Dict[str, MessageStream] = {}
        self._stream_order: List[str] = []  # Priority order
        self._default_handler: Optional[Callable] = None
        self._stats = {
            "routed": 0,
            "unmatched": 0,
            "errors": 0,
            "by_stream": {},
        }
        log.info("StreamMultiplexer initialized")
    
    def register(self, stream: MessageStream, priority: int = 0):
        """Register a message stream.
        
        Args:
            stream: MessageStream implementation
            priority: Lower number = higher priority (matched first)
        """
        self._streams[stream.stream_id] = stream
        # Insert at priority position
        if priority == 0:
            self._stream_order.append(stream.stream_id)
        else:
            # Insert before first stream with lower or equal priority
            inserted = False
            for i, sid in enumerate(self._stream_order):
                if self._streams.get(sid, None) is not None:
                    inserted = True
                    self._stream_order.insert(i, stream.stream_id)
                    break
            if not inserted:
                self._stream_order.append(stream.stream_id)
        
        self._stats["by_stream"][stream.stream_id] = 0
        log.info(f"Registered stream: {stream.stream_id} (total: {len(self._streams)})")
    
    def unregister(self, stream_id: str):
        """Unregister a stream by ID."""
        if stream_id in self._streams:
            del self._streams[stream_id]
            self._stream_order.remove(stream_id)
            log.info(f"Unregistered stream: {stream_id}")
    
    def set_default_handler(self, handler: Callable):
        """Set handler for messages that don't match any stream."""
        self._default_handler = handler
    
    def match(self, raw_data: bytes, metadata: Optional[Dict] = None) -> Optional[MessageStream]:
        """Find the appropriate stream for a message.
        
        Tries each registered stream in priority order.
        Returns the first matching stream, or None if no match.
        """
        for stream_id in self._stream_order:
            stream = self._streams.get(stream_id)
            if stream and stream.matches(raw_data, metadata):
                self._stats["routed"] += 1
                self._stats["by_stream"][stream_id] = self._stats["by_stream"].get(stream_id, 0) + 1
                return stream
        
        self._stats["unmatched"] += 1
        log.debug(f"No stream matched for message (first 80 bytes: {raw_data[:80]})")
        return None
    
    async def route(self, message: Any, raw_data: bytes, from_peer: str, 
                    metadata: Optional[Dict] = None) -> Optional[Any]:
        """Route a message to the appropriate stream and forward it.
        
        If no stream matches, calls the default handler if set.
        
        Args:
            message: Parsed A2AMessage object
            raw_data: Raw bytes of the message (for pattern matching)
            from_peer: Source peer identifier
            metadata: Optional metadata dict
            
        Returns:
            Stream forward result, or default handler result, or None
        """
        stream = self.match(raw_data, metadata)
        
        if stream:
            try:
                result = await stream.forward(message, from_peer, metadata)
                return result
            except Exception as e:
                self._stats["errors"] += 1
                log.error(f"Stream {stream.stream_id} forward error: {e}")
                return None
        
        # No stream matched — call default handler
        if self._default_handler:
            try:
                if asyncio.iscoroutinefunction(self._default_handler):
                    return await self._default_handler(message, from_peer, metadata)
                else:
                    return self._default_handler(message, from_peer, metadata)
            except Exception as e:
                self._stats["errors"] += 1
                log.error(f"Default handler error: {e}")
                return None
        
        return None
    
    def get_stats(self) -> dict:
        """Return multiplexer statistics."""
        return {
            **self._stats,
            "streams": list(self._stream_order),
            "total_streams": len(self._streams),
        }


# ─── Convenience: Default Multiplexer ─────────────────────────────

def create_default_mux() -> StreamMultiplexer:
    """Create a multiplexer with all built-in streams registered."""
    mux = StreamMultiplexer()
    # Priority order: mesh_control first (fastest to match), then a2a, then file, then dashboard
    mux.register(MeshControlStream(), priority=1)   # Heartbeats, discovery, ACKs
    mux.register(A2AStream(), priority=2)           # Agent-to-agent messages
    mux.register(FileTransferStream(), priority=3)   # File transfers
    mux.register(DashboardStream(), priority=4)     # Dashboard-originated messages
    return mux