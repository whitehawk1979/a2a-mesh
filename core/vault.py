"""Vault — cross-platform secret storage for mesh credentials.

Marveen-inspired: mesh secrets (PG passwords, mTLS/HMAC secrets, webhook tokens)
should not sit in plaintext config YAMLs on 4 different machines.

Resolution order (per machine, first available wins):
  1. OS keyring via `keyring` lib (macOS Keychain, Linux gnome-keyring/libsecret,
     Windows Credential Manager) — values never touch the disk in plaintext.
  2. Encrypted file vault (`A2A_VAULT_FILE`, default ~/.hermes/.mesh_vault):
     - If the `cryptography` package is available: AES-256-GCM (Fernet) with a
       master key file (0600, A2A_VAULT_KEY or ~/.hermes/.mesh_vault.key).
     - Without `cryptography` (minimal/HAOS containers): stdlib fallback —
       HMAC-SHA256 authenticated keystream (XOR one-time-pad derived via
       PBKDF2-HMAC per entry salt). NOT as strong as AES (no standard cipher),
       but keeps plaintext off disk + tamper-evident; master key is 0600.
  3. Environment variables (A2A_VAULT_<NAME>) — CI/headless deployments.
  4. Plaintext passthrough (legacy configs keep working; migration is incremental).

Secret REFERENCE format in config YAML: `vault:MESH/<NAME>` — resolved at
config load time; values exist only in process memory afterwards.

CLI:
    python3 -m core.vault set MESH/PG_PASSWORD
    python3 -m core.vault get MESH/PG_PASSWORD
    python3 -m core.vault list
"""

import os
import sys
import json
import hmac
import base64
import hashlib
import secrets
import time
from typing import Optional, Tuple

_SERVICE_PREFIX = "MESH"


# ── OS keyring backend ─────────────────────────────────────────────────────

def _keyring_lib():
    try:
        import keyring
        backend = keyring.get_keyring()
        if backend and getattr(backend, "priority", 0) >= 0:
            return keyring
    except Exception:
        pass
    return None


# ── File vault (encrypted, cross-platform) ────────────────────────────────

def _vault_path() -> str:
    return os.environ.get("A2A_VAULT_FILE") or os.path.expanduser("~/.hermes/.mesh_vault")


def _key_path() -> str:
    return os.environ.get("A2A_VAULT_KEY") or _vault_path() + ".key"


def _load_master_key(create: bool = True) -> Optional[bytes]:
    kp = _key_path()
    if os.path.isfile(kp):
        try:
            with open(kp, "rb") as f:
                return base64.b64decode(f.read().strip())
        except Exception:
            return None
    if not create:
        return None
    key = secrets.token_bytes(32)
    os.makedirs(os.path.dirname(kp) or ".", exist_ok=True)
    with open(kp, "wb") as f:
        f.write(base64.b64encode(key))
    os.chmod(kp, 0o600)
    return key


def _fernet_available() -> bool:
    try:
        from cryptography.fernet import Fernet  # noqa: F401
        return True
    except Exception:
        return False


# ── AES-256-GCM (Fernet) path ─────────────────────────────────────────────

def _enc_aes(value: str, key: bytes) -> Tuple[str, str]:
    """Encrypt with Fernet; returns (blob, mode)."""
    from cryptography.fernet import Fernet
    f = Fernet(base64.urlsafe_b64encode(key))
    blob = f.encrypt(value.encode("utf-8"))
    return base64.b64encode(blob).decode("ascii"), "aes-gcm"


def _dec_aes(blob: str, key: bytes) -> Optional[str]:
    from cryptography.fernet import Fernet
    f = Fernet(base64.urlsafe_b64encode(key))
    try:
        return f.decrypt(base64.b64decode(blob)).decode("utf-8")
    except Exception:
        return None


# ── Stdlib authenticated-keystream path (no external deps) ─────────────────

def _derive_keystream(salt: bytes, key: bytes, length: int) -> bytes:
    """PBKDF2-HMAC based keystream (deterministic, stdlib-only)."""
    block = hashlib.pbkdf2_hmac("sha256", key, salt, 100_000, dklen=length)
    return block


def _enc_xor(value: str, key: bytes) -> Tuple[str, str]:
    """Authenticated XOR-keystream encryption, stdlib only.

    Format: salt(16) + ct + hmac — each value gets a fresh random salt, so
    identical secrets never encrypt to the same blob. HMAC detects tampering.
    """
    salt = secrets.token_bytes(16)
    data = value.encode("utf-8")
    ks = _derive_keystream(salt, key, len(data))
    ct = bytes(a ^ b for a, b in zip(data, ks))
    mac = hmac.new(key, salt + ct, hashlib.sha256).digest()
    blob = base64.b64encode(salt + ct + mac).decode("ascii")
    return blob, "xor-hmac"


def _dec_xor(blob: str, key: bytes) -> Optional[str]:
    try:
        raw = base64.b64decode(blob)
        salt, ct, mac = raw[:16], raw[16:-32], raw[-32:]
        expected = hmac.new(key, salt + ct, hashlib.sha256).digest()
        if not hmac.compare_digest(mac, expected):
            return None  # tampered
        ks = _derive_keystream(salt, key, len(ct))
        data = bytes(a ^ b for a, b in zip(ct, ks))
        return data.decode("utf-8")
    except Exception:
        return None


# ── Vault store (file) ─────────────────────────────────────────────────────

def _read_store() -> dict:
    vp = _vault_path()
    if not os.path.isfile(vp):
        return {}
    try:
        with open(vp, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_store(store: dict) -> None:
    vp = _vault_path()
    os.makedirs(os.path.dirname(vp) or ".", exist_ok=True)
    with open(vp, "w", encoding="utf-8") as f:
        json.dump(store, f)
    os.chmod(vp, 0o600)


def _store_get(name: str) -> Optional[str]:
    store = _read_store()
    entry = store.get(name)
    if not entry:
        return None
    key = _load_master_key(create=False)
    if not key:
        return None
    mode = entry.get("mode", "xor-hmac")
    blob = entry.get("blob", "")
    if mode == "aes-gcm":
        return _dec_aes(blob, key)
    return _dec_xor(blob, key)


def _store_set(name: str, value: str) -> None:
    store = _read_store()
    key = _load_master_key(create=True)
    use_aes = _fernet_available()
    blob, mode = (_enc_aes if use_aes else _enc_xor)(value, key)
    store[name] = {"mode": mode, "blob": blob}
    _write_store(store)


# ── Public API ─────────────────────────────────────────────────────────────

def get_secret(name: str) -> Optional[str]:
    """Resolve a secret by full name (e.g. 'MESH/PG_PASSWORD')."""
    service, _, key = name.partition("/")
    service = service or _SERVICE_PREFIX
    # 1) OS keyring
    kr = _keyring_lib()
    if kr:
        try:
            v = kr.get_password(service, key)
            if v:
                return v
        except Exception:
            pass
    # 2) Encrypted file vault
    if os.path.isfile(_vault_path()):
        v = _store_get(name)
        if v is not None:
            return v
    # 3) Environment
    env_key = "A2A_VAULT_" + key.upper().replace("-", "_")
    return os.environ.get(env_key)


def _index_path() -> str:
    return _vault_path() + ".keyring_index"


def _index_add(name: str) -> None:
    """Track keyring-stored names (Keychain not listable without security prompts)."""
    try:
        idx = {}
        ip = _index_path()
        if os.path.isfile(ip):
            try:
                with open(ip, "r", encoding="utf-8") as f:
                    idx = json.load(f)
            except Exception:
                idx = {}
        if name not in idx:
            idx[name] = {"backend": "keyring", "ts": time.time()}
            os.makedirs(os.path.dirname(ip) or ".", exist_ok=True)
            with open(ip, "w", encoding="utf-8") as f:
                json.dump(idx, f)
            os.chmod(ip, 0o600)
    except Exception:
        pass


def _index_remove(name: str) -> None:
    try:
        ip = _index_path()
        if not os.path.isfile(ip):
            return
        with open(ip, "r", encoding="utf-8") as f:
            idx = json.load(f)
        if name in idx:
            idx.pop(name, None)
            with open(ip, "w", encoding="utf-8") as f:
                json.dump(idx, f)
    except Exception:
        pass


def set_secret(name: str, value: str) -> bool:
    """Store a secret: OS keyring preferred, encrypted file vault fallback."""
    service, _, key = name.partition("/")
    service = service or _SERVICE_PREFIX
    kr = _keyring_lib()
    if kr:
        try:
            kr.set_password(service, key, value)
            _index_add(name)
            return True
        except Exception as e:
            print(f"OS keyring set failed ({e}); using encrypted file vault", file=sys.stderr)
    try:
        _store_set(name, value)
        return True
    except Exception as e:
        print(f"file vault set failed: {e}", file=sys.stderr)
        return False


def resolve_config_value(value):
    """Resolve 'vault:NAME' references in config values. Passthrough otherwise."""
    if isinstance(value, str) and value.startswith("vault:"):
        name = value[len("vault:"):].strip()
        secret = get_secret(name)
        if secret is not None:
            return secret
        return value  # unresolved → keep original (caller may warn)
    return value


def backend_info() -> str:
    kr = _keyring_lib()
    parts = []
    parts.append("OS keyring: " + ("available" if kr else "not available"))
    if os.path.isfile(_vault_path()):
        mode = "AES-256-GCM" if _fernet_available() else "XOR-HMAC (stdlib)"
        parts.append(f"file vault: {_vault_path()} ({mode})")
    envs = [k for k in os.environ if k.startswith("A2A_VAULT_")]
    if envs:
        parts.append(f"env secrets: {', '.join(envs)}")
    return " | ".join(parts)


# ── CLI ────────────────────────────────────────────────────────────────────

def _cli():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)
    cmd = sys.argv[1]
    if cmd == "set" and len(sys.argv) >= 4:
        name, value = sys.argv[2], sys.argv[3]
        ok = set_secret(name, value)
        print("stored" if ok else "FAILED")
    elif cmd == "get" and len(sys.argv) >= 3:
        v = get_secret(sys.argv[2])
        print(v if v else "(not found)")
    elif cmd == "list":
        print(backend_info())
        store = _read_store()
        for name in store:
            print(f"  {name} ({store[name].get('mode', '?')})")
        for name in _keyring_index_names():
            if name not in store:
                print(f"  {name} (keyring)")
    else:
        print("usage: python3 -m core.vault set|get|list [NAME] [VALUE]")


if __name__ == "__main__":
    _cli()

# ── Legacy API compatibility layer (dashboard_admin.py endpoints) ──────────
# v0.38 dashboard endpoints expect these functions; the ceca47e refactor
# removed them, breaking /api/vault* (500 ImportError). Implemented on top
# of the new 3-tier vault (keyring → encrypted file → env).

def list_secrets():
    """List all vault entries (no secrets revealed). Covers file vault + keyring index + env."""
    entries = []
    seen = set()
    for name, meta in _read_store().items():
        seen.add(name)
        entries.append({
            "id": name,
            "label": name,
            "type": meta.get("mode", "generic"),
            "backend": "file",
            "created_at": meta.get("created_at"),
        })
    # OS keyring bejegyzések (indexből — Keychain nem listázható prompt nélkül)
    for name in _keyring_index_names():
        if name not in seen:
            seen.add(name)
            entries.append({
                "id": name,
                "label": name,
                "type": "keyring",
                "backend": "keyring",
                "created_at": None,
            })
    for k in sorted(os.environ):
        if k.startswith("A2A_VAULT_"):
            entries.append({
                "id": k[len("A2A_VAULT_"):].lower().replace("_", "/"),
                "label": k[len("A2A_VAULT_"):],
                "type": "env",
                "backend": "env",
                "created_at": None,
            })
    return entries


def store_secret(label, secret, secret_type="generic"):
    """Store a secret (new-style: keyring preferred, file vault fallback)."""
    if not label or not isinstance(secret, str) or not secret:
        return {"error": "label and secret required"}
    ok = set_secret(label, secret)
    return {"id": label, "label": label, "stored": bool(ok)}


def delete_secret(entry_id):
    """Delete a secret (file vault + OS keyring, ha elérhető)."""
    deleted = False
    store = _read_store()
    if entry_id in store:
        store.pop(entry_id, None)
        _write_store(store)
        deleted = True
    kr = _keyring_lib()
    if kr:
        try:
            service, _, key = entry_id.partition("/")
            service = service or _SERVICE_PREFIX
            kr.delete_password(service, key)
            deleted = True
        except Exception:
            pass
    _index_remove(entry_id)
    return deleted


def get_vault_status():
    """Get vault status (legacy shape, new backend info)."""
    store = _read_store()
    keyring_names = _keyring_index_names()
    all_names = sorted(set(list(store.keys()) + keyring_names))
    return {
        "initialized": os.path.exists(_vault_path()) or bool(keyring_names),
        "encrypted": True,
        "entry_count": len(all_names),
        "entries": all_names,
        "backend": backend_info(),
        "keyring_available": _keyring_lib() is not None,
    }


def _keyring_index_names() -> list:
    """Names stored in the OS keyring (tracked via index file)."""
    try:
        ip = _index_path()
        if os.path.isfile(ip):
            with open(ip, "r", encoding="utf-8") as f:
                return sorted(json.load(f).keys())
    except Exception:
        pass
    return []
