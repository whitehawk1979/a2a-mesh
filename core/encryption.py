import logging
"""A2A Mesh Encryption — Ed25519 signing + NaCl encryption for mesh messages."""

import os
import hashlib
from typing import Optional, Tuple

log = logging.getLogger("a2a_mesh.encryption")


try:
    from nacl.signing import SigningKey, VerifyKey
    from nacl.encoding import HexEncoder
    from nacl.public import PrivateKey, SealedBox, PublicKey
    from nacl.secret import SecretBox
    HAS_NACL = True
except ImportError:
    HAS_NACL = False


class MeshEncryption:
    """Handles Ed25519 signing and NaCl encryption for mesh messages."""

    def __init__(self, signing_key_hex: Optional[str] = None):
        if not HAS_NACL:
            raise ImportError("pynacl not installed. Run: pip install pynacl")

        if signing_key_hex:
            self.signing_key = SigningKey(signing_key_hex, encoder=HexEncoder)
        else:
            self.signing_key = SigningKey.generate()

        self.verify_key = self.signing_key.verify_key
        self.signing_key_hex = self.signing_key.encode(encoder=HexEncoder).decode()
        self.verify_key_hex = self.verify_key.encode(encoder=HexEncoder).decode()

        # NaCl encryption key (derived from signing key for convenience)
        self._private_key = PrivateKey(self.signing_key.encode()[:32])

    def sign_message(self, content: str) -> str:
        """Sign a message string with Ed25519. Returns hex signature."""
        signed = self.signing_key.sign(content.encode('utf-8'))
        return signed.signature.hex()

    def verify_message(self, content: str, signature_hex: str,
                       public_key_hex: str) -> bool:
        """Verify an Ed25519 signature."""
        try:
            verify_key = VerifyKey(public_key_hex, encoder=HexEncoder)
            signature = bytes.fromhex(signature_hex)
            verify_key.verify(content.encode('utf-8'), signature)
            return True
        except Exception:
            return False

    def encrypt_for_peer(self, plaintext: bytes,
                         peer_public_key_hex: str) -> bytes:
        """Encrypt a message for a specific peer using NaCl sealed box."""
        try:
            peer_public = PublicKey(
                bytes.fromhex(peer_public_key_hex)[:32]
            )
            box = SealedBox(self._private_key)
            # For sealed box, we need the peer's public key
            # But SealedBox uses OUR private key + THEIR public key
            # Let's use the correct approach
            from nacl.public import SealedBox as SB
            peer_pub = PublicKey(bytes.fromhex(peer_public_key_hex)[:32])
            box = SB(self._private_key)  # This doesn't work as expected
            # Actually: SealedBox encrypts with recipient's public key
            box = SB(peer_pub)
            return box.encrypt(plaintext)
        except Exception as e:
            raise ValueError(f"Encryption failed: {e}")

    def decrypt_from_peer(self, ciphertext: bytes) -> bytes:
        """Decrypt a sealed box message from a peer."""
        box = SealedBox(self._private_key)
        return box.decrypt(ciphertext)

    @staticmethod
    def generate_keypair() -> Tuple[str, str]:
        """Generate a new Ed25519 keypair. Returns (private_key_hex, public_key_hex)."""
        sk = SigningKey.generate()
        private_hex = sk.encode(encoder=HexEncoder).decode()
        public_hex = sk.verify_key.encode(encoder=HexEncoder).decode()
        return private_hex, public_hex

    @staticmethod
    def derive_keypair_from_seed(seed: str) -> Tuple[str, str]:
        """Derive a deterministic keypair from a seed string."""
        seed_bytes = hashlib.sha256(seed.encode()).digest()
        sk = SigningKey(seed_bytes)
        private_hex = sk.encode(encoder=HexEncoder).decode()
        public_hex = sk.verify_key.encode(encoder=HexEncoder).decode()
        return private_hex, public_hex