"""A2A Mesh Binary Framing — Length-prefixed binary protocol with version header.

Frame format (v2 — authenticated):
    [0x02][4-byte BE length][4-byte BE timestamp][8-byte nonce]
    [4-byte BE HMAC_length][HMAC_bytes][payload]

Frame format (v1):
    [0x01][4-byte BE length][payload]

Frame format (v0, legacy — backward compatible):
    [4-byte BE length][payload]

The version byte allows future protocol extensions without breaking compatibility.
Version 0 (legacy) frames are auto-detected by checking if the first byte
is a valid JSON start character ({, [) or msgpack header.

Version negotiation:
    During P2P handshake (first heartbeat), peers exchange their supported
    frame version in the heartbeat payload. If both support v2, HMAC auth
    is used for all subsequent frames. If either peer only supports v1,
    frames are sent without authentication.

Version history:
    v0: Legacy format — [4-byte length][payload] (backward compatible)
    v1: Versioned format — [0x01][4-byte length][payload]
    v2: Authenticated format — [0x02][4-byte length][4-byte timestamp]
        [8-byte nonce][4-byte HMAC_length][HMAC_bytes][payload]
"""

import hashlib
import hmac
import os
import struct
import time
import logging

log = logging.getLogger("a2a_mesh.framing")

# Protocol version (latest supported version)
FRAME_VERSION = 1

# Version byte markers
V1_MARKER = 0x01
V2_MARKER = 0x02

# Maximum message size: 10MB
MAX_MESSAGE_SIZE = 10 * 1024 * 1024

# Maximum HMAC size (prevent DoS via absurd HMAC_length)
MAX_HMAC_SIZE = 512

# v0 legacy: first byte is '{' (0x7B) or '[' (0x5B) for JSON,
# or 0x80-0x9F for msgpack fixarray/fixmap
V0_JSON_MARKERS = {0x7B, 0x5B}  # { or [
V0_MSGPACK_MARKERS = set(range(0x80, 0xA0))  # fixmap
V0_MSGPACK_ARRAY_MARKERS = set(range(0x90, 0xA0))  # fixarray


def compute_hmac(key: bytes, timestamp: int, nonce: bytes, payload: bytes) -> bytes:
    """Compute HMAC-SHA256 for v2 frame authentication.

    The HMAC covers timestamp || nonce || payload so that any tampering
    with the header or payload is detected.

    Args:
        key: Shared secret key for HMAC computation.
        timestamp: 4-byte big-endian timestamp (seconds since epoch).
        nonce: 8-byte random nonce.
        payload: Frame payload bytes.

    Returns:
        32-byte HMAC-SHA256 digest.
    """
    ts_bytes = struct.pack('>I', timestamp)
    msg = ts_bytes + nonce + payload
    return hmac.new(key, msg, hashlib.sha256).digest()


def encode_frame(payload: bytes, version: int = FRAME_VERSION,
                 hmac_key: bytes | None = None) -> bytes:
    """Encode a payload into a versioned binary frame.

    Args:
        payload: The message bytes (msgpack or JSON)
        version: Protocol version (0, 1, or 2).
            v0: legacy [4-byte length][payload]
            v1: [0x01][4-byte length][payload]
            v2: [0x02][4-byte length][4-byte timestamp][8-byte nonce]
                [4-byte HMAC_length][HMAC_bytes][payload]
        hmac_key: Required for v2 frames. Shared secret for HMAC auth.

    Returns:
        Framed bytes.

    Raises:
        ValueError: If payload is too large, or v2 is requested without hmac_key.
    """
    if len(payload) > MAX_MESSAGE_SIZE:
        raise ValueError(f"Payload too large: {len(payload)} bytes "
                        f"(max {MAX_MESSAGE_SIZE})")

    if version == 2:
        if hmac_key is None:
            raise ValueError("v2 frames require an hmac_key for authentication")
        timestamp = int(time.time())
        # Clamp timestamp to 4-byte unsigned range
        if timestamp < 0 or timestamp > 0xFFFFFFFF:
            raise ValueError(f"Timestamp out of 4-byte range: {timestamp}")
        nonce = os.urandom(8)
        mac = compute_hmac(hmac_key, timestamp, nonce, payload)
        # Inner content after the 5-byte header: timestamp + nonce + hmac_len + hmac + payload
        inner = (struct.pack('>I', timestamp) + nonce +
                 struct.pack('>I', len(mac)) + mac + payload)
        version_byte = bytes([V2_MARKER])
        length_prefix = struct.pack('>I', len(inner))
        return version_byte + length_prefix + inner

    elif version >= 1:
        # v1+: [version byte][4-byte length][payload]
        version_byte = bytes([version])
        length_prefix = struct.pack('>I', len(payload))
        return version_byte + length_prefix + payload
    else:
        # v0 legacy: [4-byte length][payload]
        length_prefix = struct.pack('>I', len(payload))
        return length_prefix + payload


def decode_frame(data: bytes, hmac_key: bytes | None = None) -> tuple:
    """Decode a versioned binary frame.

    Args:
        data: Raw bytes received from stream (must be complete frame)
        hmac_key: Required to verify v2 frames. Shared secret for HMAC auth.

    Returns:
        (version, payload) tuple. For v2 frames the payload is the original
        application payload (auth header is stripped and verified).

    Raises:
        ValueError: If frame is invalid or HMAC verification fails.
    """
    if not data or len(data) < 5:
        raise ValueError(f"Frame too short: {len(data)} bytes")

    first_byte = data[0]

    if first_byte == V2_MARKER:
        # v2 frame: [0x02][4-byte length][4-byte timestamp][8-byte nonce]
        #           [4-byte HMAC_length][HMAC_bytes][payload]
        if len(data) < 5:
            raise ValueError(f"v2 frame header incomplete: {len(data)} bytes")
        length = struct.unpack('>I', data[1:5])[0]
        if length > MAX_MESSAGE_SIZE:
            raise ValueError(f"Payload too large: {length} bytes")
        inner = data[5:]
        if len(inner) != length:
            raise ValueError(f"Inner length mismatch: expected {length}, "
                           f"got {len(inner)}")

        # Minimum inner size: timestamp(4) + nonce(8) + hmac_len(4) + hmac(1) = 17
        if len(inner) < 17:
            raise ValueError(f"v2 auth header too short: {len(inner)} bytes "
                           f"(need at least 17)")

        offset = 0
        timestamp = struct.unpack('>I', inner[offset:offset+4])[0]
        offset += 4
        nonce = inner[offset:offset+8]
        offset += 8
        hmac_length = struct.unpack('>I', inner[offset:offset+4])[0]
        offset += 4

        if hmac_length > MAX_HMAC_SIZE:
            raise ValueError(f"HMAC too large: {hmac_length} bytes "
                           f"(max {MAX_HMAC_SIZE})")

        mac = inner[offset:offset+hmac_length]
        offset += hmac_length
        payload = inner[offset:]

        # Verify HMAC
        if hmac_key is None:
            raise ValueError("v2 frame requires hmac_key for verification")
        expected_mac = compute_hmac(hmac_key, timestamp, nonce, payload)
        if not hmac.compare_digest(mac, expected_mac):
            raise ValueError("v2 HMAC verification failed")

        return (2, payload)

    elif first_byte == V1_MARKER:
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


async def read_frame(reader, hmac_key: bytes | None = None) -> tuple:
    """Read a versioned frame from an async stream reader.

    Auto-detects v0 (legacy), v1 (versioned), and v2 (authenticated) frames.

    Args:
        reader: asyncio.StreamReader
        hmac_key: Required to verify v2 frames. Shared secret for HMAC auth.

    Returns:
        (version, payload) tuple. For v2 frames the auth header is stripped
        and verified, returning only the application payload.

    Raises:
        ValueError: If frame is invalid, too large, or HMAC verification fails.
    """
    # Read first byte to detect version
    first_byte = await reader.readexactly(1)
    version_byte = first_byte[0]

    # ── HTTP probe detection: drop non-mesh connections early ──
    # Common HTTP methods: GET(47), POST(50), PUT(50), HEAD(48), OPTIONS(4F), CONNECT(43)
    # These are port scanners/probes hitting the P2P port with HTTP requests
    HTTP_PROBE_MARKERS = {0x47, 0x50, 0x48, 0x43}  # G, P, H, C (HTTP method starts)
    if version_byte in HTTP_PROBE_MARKERS and version_byte not in (V1_MARKER, V2_MARKER):
        # Read a few more bytes to confirm it's an HTTP probe
        try:
            peek = await reader.read(8)  # Read enough to identify HTTP method
            probe_data = first_byte + peek
            probe_str = probe_data.decode('ascii', errors='replace')[:16]
        except Exception:
            probe_str = first_byte.decode('ascii', errors='replace')
        raise ValueError(f"HTTP probe rejected on P2P port: {probe_str!r}")

    if version_byte == V2_MARKER:
        # v2 frame: [0x02][4-byte length][inner: timestamp + nonce + hmac_len + hmac + payload]
        length_data = await reader.readexactly(4)
        length = struct.unpack('>I', length_data)[0]

        if length > MAX_MESSAGE_SIZE:
            raise ValueError(f"Payload too large: {length} bytes")

        inner = await reader.readexactly(length)

        # Minimum inner size: timestamp(4) + nonce(8) + hmac_len(4) + hmac(1) = 17
        if len(inner) < 17:
            raise ValueError(f"v2 auth header too short: {len(inner)} bytes")

        offset = 0
        timestamp = struct.unpack('>I', inner[offset:offset+4])[0]
        offset += 4
        nonce = inner[offset:offset+8]
        offset += 8
        hmac_length = struct.unpack('>I', inner[offset:offset+4])[0]
        offset += 4

        if hmac_length > MAX_HMAC_SIZE:
            raise ValueError(f"HMAC too large: {hmac_length} bytes (max {MAX_HMAC_SIZE})")

        mac = inner[offset:offset+hmac_length]
        offset += hmac_length
        payload = inner[offset:]

        # Verify HMAC
        if hmac_key is None:
            raise ValueError("v2 frame requires hmac_key for verification")
        expected_mac = compute_hmac(hmac_key, timestamp, nonce, payload)
        if not hmac.compare_digest(mac, expected_mac):
            raise ValueError("v2 HMAC verification failed")

        return (2, payload)

    elif version_byte == V1_MARKER:
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
    For v2 frames, also includes timestamp, nonce, and hmac_length.
    """
    if not data or len(data) < 5:
        return {"version": "unknown", "length": 0, "payload_size": 0}

    first_byte = data[0]

    if first_byte == V2_MARKER:
        if len(data) < 5:
            return {"version": 2, "length": 0, "payload_size": 0}
        length = struct.unpack('>I', data[1:5])[0]
        inner = data[5:]
        info = {
            "version": 2,
            "length": length,
            "frame_size": len(data),
        }
        # Parse auth header if we have enough data
        if len(inner) >= 4:
            info["timestamp"] = struct.unpack('>I', inner[0:4])[0]
        if len(inner) >= 12:
            info["nonce"] = inner[4:12]
        if len(inner) >= 16:
            hmac_len = struct.unpack('>I', inner[12:16])[0]
            info["hmac_length"] = hmac_len
            # payload starts after timestamp(4)+nonce(8)+hmac_len(4)+hmac_bytes
            payload_offset = 16 + hmac_len
            info["payload_size"] = max(0, len(inner) - payload_offset)
        else:
            info["payload_size"] = 0
        return info

    elif first_byte == V1_MARKER:
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