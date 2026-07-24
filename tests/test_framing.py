"""Tests for core.framing — Binary frame encoding/decoding."""

import struct
import pytest
import asyncio

from a2a_mesh.core.framing import (
    encode_frame, decode_frame, read_frame, frame_info,
    FRAME_VERSION, V1_MARKER, V2_MARKER, MAX_MESSAGE_SIZE,
    compute_hmac,
)


# ── Helper: Mock async reader ──

class MockReader:
    """Async StreamReader mock that yields bytes from a buffer."""
    def __init__(self, data):
        self._data = data
        self._pos = 0

    async def readexactly(self, n):
        chunk = self._data[self._pos:self._pos + n]
        self._pos += n
        if len(chunk) < n:
            raise asyncio.IncompleteReadError(chunk, n)
        return chunk

    async def read(self, n):
        chunk = self._data[self._pos:self._pos + n]
        self._pos += len(chunk)
        return chunk


# ═══════════════════════════════════════════════════════════
# V0 & V1 Tests (original, backward-compat)
# ═══════════════════════════════════════════════════════════

class TestEncodeFrame:
    def test_encode_v1_frame(self):
        payload = b"Hello, World!"
        frame = encode_frame(payload, version=1)
        # v1: [1-byte version][4-byte BE length][payload]
        assert frame[0] == V1_MARKER
        length = struct.unpack('>I', frame[1:5])[0]
        assert length == len(payload)
        assert frame[5:] == payload

    def test_encode_v0_frame(self):
        payload = b"Legacy frame"
        frame = encode_frame(payload, version=0)
        # v0: [4-byte BE length][payload]
        length = struct.unpack('>I', frame[0:4])[0]
        assert length == len(payload)
        assert frame[4:] == payload

    def test_encode_default_version_is_v1(self):
        payload = b"default version"
        frame = encode_frame(payload)
        assert frame[0] == V1_MARKER

    def test_encode_empty_payload(self):
        payload = b""
        frame = encode_frame(payload)
        assert frame[0] == V1_MARKER
        length = struct.unpack('>I', frame[1:5])[0]
        assert length == 0
        assert len(frame) == 5  # version + length only

    def test_encode_oversized_payload_raises(self):
        payload = b"x" * (MAX_MESSAGE_SIZE + 1)
        with pytest.raises(ValueError, match="too large"):
            encode_frame(payload)

    def test_encode_roundtrip_v1(self):
        payload = b"Test payload with special chars: \x00\x01\xff"
        frame = encode_frame(payload, version=1)
        version, decoded = decode_frame(frame)
        assert version == 1
        assert decoded == payload

    def test_encode_roundtrip_v0(self):
        payload = b"Legacy roundtrip"
        frame = encode_frame(payload, version=0)
        version, decoded = decode_frame(frame)
        assert version == 0
        assert decoded == payload


class TestDecodeFrame:
    def test_decode_v1_frame(self):
        payload = b"test data"
        version_byte = bytes([V1_MARKER])
        length_prefix = struct.pack('>I', len(payload))
        frame = version_byte + length_prefix + payload
        version, decoded = decode_frame(frame)
        assert version == 1
        assert decoded == payload

    def test_decode_v0_frame(self):
        payload = b"legacy data"
        length_prefix = struct.pack('>I', len(payload))
        frame = length_prefix + payload
        version, decoded = decode_frame(frame)
        assert version == 0
        assert decoded == payload

    def test_decode_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            decode_frame(b"")

    def test_decode_3_bytes_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            decode_frame(b"\x01\x00\x00")

    def test_decode_length_mismatch_raises(self):
        # v1 frame: version byte + wrong length
        frame = bytes([V1_MARKER]) + struct.pack('>I', 100) + b"short"
        with pytest.raises(ValueError, match="mismatch"):
            decode_frame(frame)

    def test_decode_payload_too_large_raises(self):
        # v1 frame with claimed huge length
        frame = bytes([V1_MARKER]) + struct.pack('>I', MAX_MESSAGE_SIZE + 1) + b"x"
        with pytest.raises(ValueError, match="too large"):
            decode_frame(frame)


class TestFrameInfo:
    def test_frame_info_v1(self):
        payload = b"info test"
        frame = encode_frame(payload, version=1)
        info = frame_info(frame)
        assert info["version"] == 1
        assert info["length"] == len(payload)
        assert info["payload_size"] == len(payload)

    def test_frame_info_v0(self):
        payload = b"info legacy"
        frame = encode_frame(payload, version=0)
        info = frame_info(frame)
        assert info["version"] == 0
        assert info["length"] == len(payload)

    def test_frame_info_empty(self):
        info = frame_info(b"")
        assert info["version"] == "unknown"
        assert info["length"] == 0

    def test_frame_info_too_short(self):
        info = frame_info(b"\x01\x00")
        # 2 bytes < 5 byte minimum → treated as unknown
        assert info["version"] == "unknown"
        assert info["length"] == 0


class TestReadFrame:
    @pytest.mark.asyncio
    async def test_read_v1_frame(self):
        payload = b"async v1 test"
        frame = encode_frame(payload, version=1)
        reader = MockReader(frame)
        version, decoded = await read_frame(reader)
        assert version == 1
        assert decoded == payload

    @pytest.mark.asyncio
    async def test_read_v0_frame(self):
        payload = b"async v0 test"
        frame = encode_frame(payload, version=0)
        reader = MockReader(frame)
        version, decoded = await read_frame(reader)
        assert version == 0
        assert decoded == payload


# ═══════════════════════════════════════════════════════════
# V2 Tests (authenticated frame format)
# ═══════════════════════════════════════════════════════════

class TestV2Encode:
    def test_encode_v2_basic(self):
        """v2 frame: [0x02][4-byte length][4-byte ts][8-byte nonce][4-byte hmac_len][hmac][payload]"""
        key = b"secret-key-12345"
        payload = b"authenticated payload"
        frame = encode_frame(payload, version=2, hmac_key=key)

        assert frame[0] == V2_MARKER
        length = struct.unpack('>I', frame[1:5])[0]
        inner = frame[5:]
        assert len(inner) == length

        # Parse inner: timestamp(4) + nonce(8) + hmac_len(4) + hmac(32) + payload
        ts = struct.unpack('>I', inner[0:4])[0]
        nonce = inner[4:12]
        hmac_len = struct.unpack('>I', inner[12:16])[0]
        mac = inner[16:16+hmac_len]
        decoded_payload = inner[16+hmac_len:]

        assert hmac_len == 32  # SHA-256 = 32 bytes
        assert decoded_payload == payload
        assert ts > 0

        # Verify HMAC independently
        expected_mac = compute_hmac(key, ts, nonce, payload)
        assert mac == expected_mac

    def test_encode_v2_empty_payload(self):
        """v2 frame with empty payload should still include auth header."""
        key = b"key"
        payload = b""
        frame = encode_frame(payload, version=2, hmac_key=key)

        assert frame[0] == V2_MARKER
        version, decoded = decode_frame(frame, hmac_key=key)
        assert version == 2
        assert decoded == b""

    def test_encode_v2_without_key_raises(self):
        """v2 encoding without hmac_key should raise ValueError."""
        with pytest.raises(ValueError, match="hmac_key"):
            encode_frame(b"data", version=2)

    def test_encode_v2_oversized_payload_raises(self):
        """v2 frame should respect MAX_MESSAGE_SIZE."""
        key = b"key"
        payload = b"x" * (MAX_MESSAGE_SIZE + 1)
        with pytest.raises(ValueError, match="too large"):
            encode_frame(payload, version=2, hmac_key=key)

    def test_encode_v2_preserves_binary_payload(self):
        """v2 roundtrip should preserve arbitrary binary data."""
        key = b"\x00\xff\x01\x02"
        payload = bytes(range(256))  # all byte values
        frame = encode_frame(payload, version=2, hmac_key=key)
        version, decoded = decode_frame(frame, hmac_key=key)
        assert version == 2
        assert decoded == payload


class TestV2Decode:
    def test_decode_v2_roundtrip(self):
        """encode then decode should recover original payload."""
        key = b"shared-secret-key"
        payload = b"roundtrip test data"
        frame = encode_frame(payload, version=2, hmac_key=key)
        version, decoded = decode_frame(frame, hmac_key=key)
        assert version == 2
        assert decoded == payload

    def test_decode_v2_without_key_raises(self):
        """Decoding v2 frame without hmac_key should raise."""
        key = b"key"
        frame = encode_frame(b"test", version=2, hmac_key=key)
        with pytest.raises(ValueError, match="hmac_key"):
            decode_frame(frame)

    def test_decode_v2_wrong_key_raises(self):
        """Decoding v2 frame with wrong key should fail HMAC."""
        key = b"correct-key"
        wrong_key = b"wrong-key"
        frame = encode_frame(b"secret data", version=2, hmac_key=key)
        with pytest.raises(ValueError, match="HMAC verification failed"):
            decode_frame(frame, hmac_key=wrong_key)

    def test_decode_v2_tampered_payload_raises(self):
        """Tampering with the payload should cause HMAC failure."""
        key = b"key"
        payload = b"original"
        frame = encode_frame(payload, version=2, hmac_key=key)
        # Flip a bit in the payload section
        tampered = bytearray(frame)
        tampered[-1] ^= 0x01  # flip last byte
        with pytest.raises(ValueError, match="HMAC verification failed"):
            decode_frame(bytes(tampered), hmac_key=key)

    def test_decode_v2_tampered_timestamp_raises(self):
        """Tampering with the timestamp should cause HMAC failure."""
        key = b"key"
        payload = b"original"
        frame = bytearray(encode_frame(payload, version=2, hmac_key=key))
        # Flip a bit in the timestamp (bytes 5-8)
        frame[5] ^= 0x01
        with pytest.raises(ValueError, match="HMAC verification failed"):
            decode_frame(bytes(frame), hmac_key=key)

    def test_decode_v2_tampered_nonce_raises(self):
        """Tampering with the nonce should cause HMAC failure."""
        key = b"key"
        payload = b"original"
        frame = bytearray(encode_frame(payload, version=2, hmac_key=key))
        # Flip a bit in the nonce (bytes 9-16)
        frame[10] ^= 0x01
        with pytest.raises(ValueError, match="HMAC verification failed"):
            decode_frame(bytes(frame), hmac_key=key)

    def test_decode_v2_truncated_inner_raises(self):
        """A v2 frame with truncated auth header should raise."""
        key = b"key"
        payload = b"test"
        frame = bytearray(encode_frame(payload, version=2, hmac_key=key))
        # Truncate to just the 5-byte header + 10 bytes (less than 17 minimum inner)
        truncated = frame[:15]
        with pytest.raises(ValueError, match="too short|incomplete|mismatch"):
            decode_frame(truncated, hmac_key=key)

    def test_decode_v2_different_keys_produce_different_hmacs(self):
        """Two different keys should produce different HMACs for the same payload."""
        import time
        payload = b"same payload"
        # Force same timestamp and nonce for comparison
        ts = int(time.time())
        nonce = b"\x00" * 8
        mac1 = compute_hmac(b"key1", ts, nonce, payload)
        mac2 = compute_hmac(b"key2", ts, nonce, payload)
        assert mac1 != mac2


class TestV2ReadFrame:
    @pytest.mark.asyncio
    async def test_read_v2_roundtrip(self):
        """Async read of v2 frame should recover original payload."""
        key = b"async-secret"
        payload = b"async v2 test"
        frame = encode_frame(payload, version=2, hmac_key=key)
        reader = MockReader(frame)
        version, decoded = await read_frame(reader, hmac_key=key)
        assert version == 2
        assert decoded == payload

    @pytest.mark.asyncio
    async def test_read_v2_without_key_raises(self):
        """Async read of v2 frame without hmac_key should raise."""
        key = b"key"
        payload = b"test"
        frame = encode_frame(payload, version=2, hmac_key=key)
        reader = MockReader(frame)
        with pytest.raises(ValueError, match="hmac_key"):
            await read_frame(reader)

    @pytest.mark.asyncio
    async def test_read_v2_wrong_key_raises(self):
        """Async read of v2 frame with wrong key should fail HMAC."""
        key = b"correct"
        frame = encode_frame(b"data", version=2, hmac_key=key)
        reader = MockReader(frame)
        with pytest.raises(ValueError, match="HMAC verification failed"):
            await read_frame(reader, hmac_key=b"wrong")


class TestV2FrameInfo:
    def test_frame_info_v2(self):
        """frame_info should parse v2 frame metadata."""
        key = b"key"
        payload = b"info v2 test"
        frame = encode_frame(payload, version=2, hmac_key=key)
        info = frame_info(frame)
        assert info["version"] == 2
        assert info["length"] > 0
        assert "timestamp" in info
        assert "nonce" in info
        assert "hmac_length" in info
        assert info["hmac_length"] == 32  # SHA-256
        assert info["payload_size"] == len(payload)

    def test_frame_info_v2_frame_size(self):
        """frame_info should report correct frame_size for v2."""
        key = b"key"
        payload = b"x" * 100
        frame = encode_frame(payload, version=2, hmac_key=key)
        info = frame_info(frame)
        assert info["frame_size"] == len(frame)
        assert info["payload_size"] == len(payload)


class TestV2BackwardCompat:
    """Ensure v0 and v1 still work after adding v2."""

    def test_v1_still_works(self):
        payload = b"v1 still works"
        frame = encode_frame(payload, version=1)
        version, decoded = decode_frame(frame)
        assert version == 1
        assert decoded == payload

    def test_v0_still_works(self):
        payload = b"v0 still works"
        frame = encode_frame(payload, version=0)
        version, decoded = decode_frame(frame)
        assert version == 0
        assert decoded == payload

    def test_default_version_still_v1(self):
        """FRAME_VERSION should still be 1 — v2 requires explicit opt-in."""
        assert FRAME_VERSION == 1
        payload = b"default"
        frame = encode_frame(payload)
        assert frame[0] == V1_MARKER

    @pytest.mark.asyncio
    async def test_async_read_v1_still_works(self):
        payload = b"async v1 compat"
        frame = encode_frame(payload, version=1)
        reader = MockReader(frame)
        version, decoded = await read_frame(reader)
        assert version == 1
        assert decoded == payload

    @pytest.mark.asyncio
    async def test_async_read_v0_still_works(self):
        payload = b"async v0 compat"
        frame = encode_frame(payload, version=0)
        reader = MockReader(frame)
        version, decoded = await read_frame(reader)
        assert version == 0
        assert decoded == payload

    def test_v2_marker_is_0x02(self):
        assert V2_MARKER == 0x02

    def test_v1_marker_unchanged(self):
        assert V1_MARKER == 0x01