"""Tests for core.framing — Binary frame encoding/decoding."""

import struct
import pytest

from a2a_mesh.core.framing import (
    encode_frame, decode_frame, read_frame, frame_info,
    FRAME_VERSION, V1_MARKER, MAX_MESSAGE_SIZE,
)


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

        class MockReader:
            def __init__(self, data):
                self._data = data
                self._pos = 0
            async def readexactly(self, n):
                chunk = self._data[self._pos:self._pos + n]
                self._pos += n
                if len(chunk) < n:
                    raise asyncio.IncompleteReadError(chunk, n)
                return chunk

        reader = MockReader(frame)
        version, decoded = await read_frame(reader)
        assert version == 1
        assert decoded == payload

    @pytest.mark.asyncio
    async def test_read_v0_frame(self):
        payload = b"async v0 test"
        frame = encode_frame(payload, version=0)

        class MockReader:
            def __init__(self, data):
                self._data = data
                self._pos = 0
            async def readexactly(self, n):
                chunk = self._data[self._pos:self._pos + n]
                self._pos += n
                if len(chunk) < n:
                    raise asyncio.IncompleteReadError(chunk, n)
                return chunk

        reader = MockReader(frame)
        version, decoded = await read_frame(reader)
        assert version == 0
        assert decoded == payload


import asyncio
