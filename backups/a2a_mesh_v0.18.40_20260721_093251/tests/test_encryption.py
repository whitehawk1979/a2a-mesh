"""Test core.encryption — Ed25519 signing, verification"""
import pytest
from a2a_mesh.core.encryption import MeshEncryption, HAS_NACL


class TestEncryption:
    """Test Ed25519 signing and verification."""

    def test_keypair_generation(self):
        sk, pk = MeshEncryption.generate_keypair()
        assert sk is not None
        assert pk is not None
        assert isinstance(sk, str)
        assert isinstance(pk, str)

    @pytest.mark.skipif(not HAS_NACL, reason="PyNaCl not installed")
    def test_sign_and_verify(self):
        sk, pk = MeshEncryption.generate_keypair()
        enc = MeshEncryption(signing_key_hex=sk)
        message = "Hello, A2A Mesh!"
        signature = enc.sign_message(message)
        assert signature is not None
        assert isinstance(signature, str)

    @pytest.mark.skipif(not HAS_NACL, reason="PyNaCl not installed")
    def test_sign_and_verify_roundtrip(self):
        sk, pk = MeshEncryption.generate_keypair()
        enc = MeshEncryption(signing_key_hex=sk)
        message = "Hello, A2A Mesh!"
        signature = enc.sign_message(message)
        # verify_message is an instance method on the same object
        result = enc.verify_message(message, signature, pk)
        assert result is True

    @pytest.mark.skipif(not HAS_NACL, reason="PyNaCl not installed")
    def test_tampered_message(self):
        sk, pk = MeshEncryption.generate_keypair()
        enc = MeshEncryption(signing_key_hex=sk)
        message = "Original message"
        signature = enc.sign_message(message)
        tampered = "Tampered message"
        result = enc.verify_message(tampered, signature, pk)
        assert result is False

    @pytest.mark.skipif(not HAS_NACL, reason="PyNaCl not installed")
    def test_empty_message(self):
        sk, pk = MeshEncryption.generate_keypair()
        enc = MeshEncryption(signing_key_hex=sk)
        message = ""
        signature = enc.sign_message(message)
        result = enc.verify_message(message, signature, pk)
        assert result is True

    def test_has_nacl_flag(self):
        assert isinstance(HAS_NACL, bool)