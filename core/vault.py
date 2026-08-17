"""
Vault — encrypted credential storage for A2A Mesh.

Inspired by Marveen's vault:
  - AES-256-GCM encrypted secrets at rest
  - Key derived via scrypt from master password
  - Atomic writes (rename for crash safety)
  - Entries: id, label, type, encrypted value

For A2A Mesh:
  - Store API keys, passwords, tokens encrypted
  - Key from env var or keychain
  - CRUD API for dashboard
"""

import os
import json
import time
import hashlib
import secrets
import logging
from pathlib import Path

log = logging.getLogger("vault")

VAULT_DIR = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data")
VAULT_FILE = os.path.join(VAULT_DIR, "vault.json")
VAULT_KEY_FILE = os.path.join(VAULT_DIR, ".vault-key")
ALGORITHM = "aes-256-gcm"

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


def _get_or_create_key():
    """Get or create the master key."""
    try:
        with open(VAULT_KEY_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        key = secrets.token_hex(32)
        os.makedirs(VAULT_DIR, exist_ok=True)
        with open(VAULT_KEY_FILE, "w") as f:
            f.write(key)
        os.chmod(VAULT_KEY_FILE, 0o600)
        return key


def _derive_key(password):
    """Derive a 256-bit key from password using scrypt-like derivation."""
    return hashlib.scrypt(
        password.encode(),
        salt=b"a2a-mesh-vault-salt",
        n=16384,
        r=8,
        p=1,
        dklen=32,
    )


def _encrypt(plaintext, key_bytes):
    """Encrypt with AES-256-GCM."""
    if not HAS_CRYPTO:
        return {"_plain": plaintext}  # Fallback: store plain (not recommended)
    nonce = secrets.token_bytes(12)
    aesgcm = AESGCM(key_bytes)
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode(), None)
    return {
        "nonce": nonce.hex(),
        "ciphertext": ciphertext.hex(),
    }


def _decrypt(encrypted, key_bytes):
    """Decrypt with AES-256-GCM."""
    if "_plain" in encrypted:
        return encrypted["_plain"]
    if not HAS_CRYPTO:
        return None
    nonce = bytes.fromhex(encrypted["nonce"])
    ciphertext = bytes.fromhex(encrypted["ciphertext"])
    aesgcm = AESGCM(key_bytes)
    return aesgcm.decrypt(nonce, ciphertext, None).decode()


def store_secret(label, secret, secret_type="generic"):
    """Store a secret in the vault."""
    key = _get_or_create_key()
    key_bytes = _derive_key(key)
    
    vault = _load_vault()
    entry_id = hashlib.sha256(f"{label}:{time.time()}".encode()).hexdigest()[:12]
    
    vault["entries"].append({
        "id": entry_id,
        "label": label,
        "type": secret_type,
        "encrypted": _encrypt(secret, key_bytes),
        "created_at": time.time(),
    })
    
    _save_vault(vault)
    return {"id": entry_id, "label": label, "stored": True}


def retrieve_secret(entry_id):
    """Retrieve a secret from the vault."""
    key = _get_or_create_key()
    key_bytes = _derive_key(key)
    
    vault = _load_vault()
    for entry in vault["entries"]:
        if entry["id"] == entry_id:
            return _decrypt(entry["encrypted"], key_bytes)
    return None


def list_secrets():
    """List all vault entries (without revealing secrets)."""
    vault = _load_vault()
    return [
        {
            "id": e["id"],
            "label": e["label"],
            "type": e["type"],
            "created_at": e["created_at"],
        }
        for e in vault["entries"]
    ]


def delete_secret(entry_id):
    """Delete a secret from the vault."""
    vault = _load_vault()
    before = len(vault["entries"])
    vault["entries"] = [e for e in vault["entries"] if e["id"] != entry_id]
    _save_vault(vault)
    return len(vault["entries"]) < before


def _load_vault():
    try:
        with open(VAULT_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"entries": []}


def _save_vault(vault):
    os.makedirs(VAULT_DIR, exist_ok=True)
    tmp = VAULT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(vault, f, indent=2)
    os.rename(tmp, VAULT_FILE)
    os.chmod(VAULT_FILE, 0o600)


def get_vault_status():
    """Get vault status."""
    vault = _load_vault()
    return {
        "initialized": os.path.exists(VAULT_FILE),
        "encrypted": HAS_CRYPTO,
        "entry_count": len(vault["entries"]),
        "entries": [e["label"] for e in vault["entries"]],
    }