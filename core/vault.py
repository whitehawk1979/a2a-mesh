"""Vault — OS-keychain-backed secret storage for mesh credentials.

Marveen-inspired (vault.md: AES-256-GCM vault with OS key store): mesh secrets
(PG passwords, mTLS/HMAC secrets, webhook tokens) should not sit in plaintext
config YAMLs on 4 different machines.

Design (deterministic, no LLM):
  - OS keyring via `keyring` lib: macOS Keychain / Linux gnome-keyring(libsecret)
  - Secret REFERENCE format in config: `vault:MESH/<NAME>` — config loader
    resolves these at startup, values exist only in process memory.
  - Fallback: if no OS backend (e.g. headless HAOS container), a
    `$A2A_VAULT_FILE` (0600, host-local) file vault is used; if that is absent
    too, the raw value passes through (legacy plaintext still works —
    migration is incremental, nothing breaks).

Usage in mesh_config.yaml:
    pg_notify:
      password: vault:MESH/PG_PASSWORD      # resolved from keychain
    webhook_secret: vault:MESH/WEBHOOK_SECRET

Setup (one-time per machine):
    python3 -m core.vault set MESH/PG_PASSWORD
    python3 -m core.vault set MESH/WEBHOOK_SECRET
    python3 -m core.vault list
"""

import os
import sys
from typing import Optional

_SERVICE_PREFIX = "MESH"


def _get_backend():
    try:
        import keyring
        backend = keyring.get_keyring()
        # A "fail" backend means no OS keyring available
        if backend and not getattr(backend, "priority", 0) < 0:
            return ("os", keyring)
        return ("none", None)
    except Exception:
        return ("none", None)


def _vault_file() -> Optional[str]:
    p = os.environ.get("A2A_VAULT_FILE")
    if p and os.path.isfile(p):
        return p
    default = os.path.expanduser("~/.hermes/.mesh_vault")
    if os.path.isfile(default):
        return default
    return None


def get_secret(name: str) -> Optional[str]:
    """Resolve a secret by NAME (e.g. 'MESH/PG_PASSWORD')."""
    service, _, key = name.partition("/")
    service = service or _SERVICE_PREFIX
    kind, keyring = _get_backend()
    if kind == "os":
        try:
            v = keyring.get_password(service, key)
            if v:
                return v
        except Exception:
            pass
    vf = _vault_file()
    if vf:
        try:
            with open(vf, "r", encoding="utf-8") as f:
                for line in f:
                    k, _, val = line.rstrip("\n").partition("=")
                    if k.strip() == f"{service}/{key}":
                        return val or None
        except OSError:
            pass
    # Legacy: raw env override
    env_key = "A2A_VAULT_" + key.upper().replace("-", "_")
    return os.environ.get(env_key)


def set_secret(name: str, value: str) -> bool:
    service, _, key = name.partition("/")
    service = service or _SERVICE_PREFIX
    kind, keyring = _get_backend()
    if kind == "os":
        try:
            keyring.set_password(service, key, value)
            return True
        except Exception as e:
            print(f"OS keyring set failed ({e}); falling back to file vault", file=sys.stderr)
    vf = os.environ.get("A2A_VAULT_FILE") or os.path.expanduser("~/.hermes/.mesh_vault")
    os.makedirs(os.path.dirname(vf), exist_ok=True)
    entries = {}
    if os.path.isfile(vf):
        with open(vf, "r", encoding="utf-8") as f:
            for line in f:
                k, _, val = line.rstrip("\n").partition("=")
                entries[k.strip()] = val
    entries[f"{service}/{key}"] = value
    with open(vf, "w", encoding="utf-8") as f:
        for k, val in entries.items():
            f.write(f"{k}={val}\n")
    os.chmod(vf, 0o600)
    return True


def resolve_config_value(value):
    """Resolve 'vault:NAME' references in config values. Passthrough otherwise."""
    if isinstance(value, str) and value.startswith("vault:"):
        name = value[len("vault:"):].strip()
        secret = get_secret(name)
        if secret is not None:
            return secret
        # Unresolved reference — return original (caller logs a warning)
        return value
    return value


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
        kind, _ = _get_backend()
        vf = _vault_file()
        print(f"backend: {'OS keyring' if kind == 'os' else 'file/env fallback'}")
        if vf:
            print(f"file vault: {vf}")
            with open(vf, "r", encoding="utf-8") as f:
                for line in f:
                    k = line.split("=", 1)[0]
                    print(f"  {k}")
    else:
        print("usage: python3 -m core.vault set|get|list [NAME] [VALUE]")


if __name__ == "__main__":
    _cli()