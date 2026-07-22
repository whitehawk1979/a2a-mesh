"""A2A Mesh Binary Framing — Length-prefixed binary protocol with version header.

Frame format (v1):
    [1-byte version][4-byte BE length][payload]

Frame format (v0, legacy — backward compatible):
    [4-byte BE length][payload]

The version byte allows future protocol extensions without breaking compatibility.
Version 0 (legacy) frames are auto-detected by checking if the first byte
is a valid JSON start character ({, [) or msgpack header.

Version history:
    v0: Legacy format — [4-byte length][payload] (backward compatible)
    v1: Versioned format — [0x01][4-byte length][payload]
"""

import struct
import logging

log = logging.getLogger("a2a_mesh.framing")

# Protocol version
FRAME_VERSION = 1

# Version byte for v1 frames
V1_MARKER = 0x01

# Maximum message size: 10MB
MAX_MESSAGE_SIZE = 10 * 1024 * 1024

# v0 legacy: first byte is '{' (0x7B) or '[' (0x5B) for JSON,
# or 0x80-0x9F for msgpack fixarray/fixmap
V0_JSON_MARKERS = {0x7B, 0x5B}  # { or [
V0_MSGPACK_MARKERS = set(range(0x80, 0xA0))  # fixmap
V0_MSGPACK_ARRAY_MARKERS = set(range(0x90, 0xA0))  # fixarray


def encode_frame(payload: bytes, version: int = FRAME_VERSION) -> bytes:
    """Encode a payload into a versioned binary frame.
    
    Args:
        payload: The message bytes (msgpack or JSON)
        version: Protocol version (default: 1)
    
    Returns:
        Framed bytes: [1-byte version][4-byte length][payload]
    """
    if len(payload) > MAX_MESSAGE_SIZE:
        raise ValueError(f"Payload too large: {len(payload)} bytes "
                        f"(max {MAX_MESSAGE_SIZE})")
    
    if version >= 1:
        # v1+: [version byte][4-byte length][payload]
        version_byte = bytes([version])
        length_prefix = struct.pack('>I', len(payload))
        return version_byte + length_prefix + payload
    else:
        # v0 legacy: [4-byte length][payload]
        length_prefix = struct.pack('>I', len(payload))
        return length_prefix + payload


def decode_frame(data: bytes) -> tuple:
    """Decode a versioned binary frame.
    
    Args:
        data: Raw bytes received from stream (must be complete frame)
    
    Returns:
        (version, payload) tuple
    
    Raises:
        ValueError: If frame is invalid
    """
    if not data or len(data) < 5:
        raise ValueError(f"Frame too short: {len(data)} bytes")
    
    first_byte = data[0]
    
    if first_byte == V1_MARKER:
        # v1 frame: [0x01][4-byte length][payload]
        if len(data) < 5:
            raise ValueError(f"v1 frame header incomplete: {len(data)} bytes")
        length = struct.unpack('>I', data[1:5])[0]
        if length > MAX_MESSAGE_SIZE:
            raise ValueError(f"Payload too large: {length} bytes")
        payload = data[5:]
        if len(payload) != length:
            raise ValueError(f"Payload length mismatch: expected {length}, "
                           f"got {len(payload)}")
        return (1, payload)
    else:
        # v0 legacy frame: [4-byte length][payload]
        # The first 4 bytes are the length (big-endian)
        length = struct.unpack('>I', data[0:4])[0]
        if length > MAX_MESSAGE_SIZE:
            raise ValueError(f"Payload too large: {length} bytes")
        payload = data[4:]
        if len(payload) != length:
            raise ValueError(f"Payload length mismatch: expected {length}, "
                           f"got {len(payload)}")
        return (0, payload)


async def read_frame(reader) -> tuple:
    """Read a versioned frame from an async stream reader.
    
    Auto-detects v0 (legacy) and v1 (versioned) frames.
    
    Args:
        reader: asyncio.StreamReader
    
    Returns:
        (version, payload) tuple
    
    Raises:
        ValueError: If frame is invalid or too large
    """
    # Read first byte to detect version
    first_byte = await reader.readexactly(1)
    version_byte = first_byte[0]
    
    if version_byte == V1_MARKER:
        # v1 frame: [0x01][4-byte length][payload]
        length_data = await reader.readexactly(4)
        length = struct.unpack('>I', length_data)[0]
        
        if length > MAX_MESSAGE_SIZE:
            raise ValueError(f"Payload too large: {length} bytes")
        
        payload = await reader.readexactly(length)
        return (1, payload)
    
    else:
        # v0 legacy: first_byte is part of 4-byte length prefix
        # Read remaining 3 bytes of length
        remaining_length = await reader.readexactly(3)
        length_data = first_byte + remaining_length
        length = struct.unpack('>I', length_data)[0]
        
        if length > MAX_MESSAGE_SIZE:
            raise ValueError(f"Payload too large: {length} bytes")
        
        payload = await reader.readexactly(length)
        return (0, payload)


def frame_info(data: bytes) -> dict:
    """Get frame info without fully decoding.
    
    Returns dict with version, length, and payload_size.
    """
    if not data or len(data) < 5:
        return {"version": "unknown", "length": 0, "payload_size": 0}
    
    first_byte = data[0]
    
    if first_byte == V1_MARKER:
        length = struct.unpack('>I', data[1:5])[0] if len(data) >= 5 else 0
        return {
            "version": 1,
            "length": length,
            "payload_size": len(data) - 5,
            "frame_size": len(data),
        }
    else:
        length = struct.unpack('>I', data[0:4])[0] if len(data) >= 4 else 0
        return {
            "version": 0,
            "length": length,
            "payload_size": len(data) - 4,
            "frame_size": len(data),
        }