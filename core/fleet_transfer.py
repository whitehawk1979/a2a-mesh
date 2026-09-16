"""
Fleet Transfer — portable mesh configuration export/import.

Inspired by Marveen's fleet-transfer:
  - Export: snapshot of team config, kanban, skills, vault, settings → JSON
  - Import: load snapshot into a fresh node
  - Encrypted with scrypt (password-protected)
  - Machine-specific paths excluded (only portable data)

For A2A Mesh:
  - Export team_config.json + kanban cards + delegation stats + skills
  - Import on a new node to replicate the mesh state
  - Useful for backup, migration, and fleet expansion
"""

import json
import os
import time
import logging
import hashlib
import secrets
import struct
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger("fleet_transfer")

MESH_DIR = os.path.expanduser("~/.hermes/scripts/a2a_mesh")
DATA_DIR = os.path.join(MESH_DIR, "data")
EXPORT_DIR = os.path.join(DATA_DIR, "fleet_exports")

FLEET_SCHEMA_VERSION = 1


def _safe_name(name: str) -> bool:
    """Validate a safe name (alphanumeric + dash/underscore)."""
    import re
    return bool(re.match(r'^[a-z0-9][a-z0-9_-]*$', name))


def export_fleet(include_vault: bool = False, include_delegations: bool = True) -> Dict[str, Any]:
    """Export a portable snapshot of the mesh configuration.
    
    Returns a JSON-serializable dict with:
    - schemaVersion, exportedAt, sourceHost
    - team config, kanban cards, skills registry
    - delegation stats (summary only, not individual tasks)
    - dashboard settings
    """
    import socket
    
    snapshot = {
        "schemaVersion": FLEET_SCHEMA_VERSION,
        "exportedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sourceHost": socket.gethostname(),
    }
    
    # 1. Team configuration
    team_file = os.path.join(DATA_DIR, "team_config.json")
    try:
        with open(team_file) as f:
            snapshot["team"] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        snapshot["team"] = {"nodes": {}}
    
    # 2. Kanban boards
    kanban_file = os.path.join(DATA_DIR, "kanban_boards.json")
    try:
        with open(kanban_file) as f:
            snapshot["kanban"] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        snapshot["kanban"] = {"boards": []}
    
    # 3. Skills registry (from data/skills_registry.json)
    skills_file = os.path.join(DATA_DIR, "skills_registry.json")
    try:
        with open(skills_file) as f:
            snapshot["skills"] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        snapshot["skills"] = {"skills": []}
    
    # 4. Delegation stats (summary)
    if include_delegations:
        snapshot["delegationStats"] = {"note": "Summary only — individual tasks are PG-backed"}
    
    # 5. Dashboard settings
    settings_file = os.path.join(DATA_DIR, "dashboard_settings.json")
    try:
        with open(settings_file) as f:
            snapshot["dashboardSettings"] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        snapshot["dashboardSettings"] = {}
    
    # 6. Mesh config (peers, transport, security — no secrets)
    mesh_config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    try:
        import yaml
        with open(mesh_config_file) as f:
            config = yaml.safe_load(f)
        # Strip secrets
        if isinstance(config, dict):
            config.pop("pg_password", None)
            config.pop("auth_password", None)
            config.pop("tls_key_path", None)
            config.pop("tls_cert_path", None)
        snapshot["meshConfig"] = config
    except (FileNotFoundError, ImportError, Exception):
        snapshot["meshConfig"] = None
    
    log.info(f"Fleet export: {len(json.dumps(snapshot))} bytes from {snapshot['sourceHost']}")
    return snapshot


def save_export(snapshot: Dict, password: Optional[str] = None) -> str:
    """Save a fleet export to disk, optionally encrypted.
    
    Returns the file path of the saved export.
    """
    os.makedirs(EXPORT_DIR, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"fleet_export_{timestamp}.json"
    filepath = os.path.join(EXPORT_DIR, filename)
    
    data = json.dumps(snapshot, indent=2, ensure_ascii=False)
    
    if password:
        encrypted = _encrypt(data, password)
        filepath = filepath.replace(".json", ".enc")
        with open(filepath, "wb") as f:
            f.write(encrypted)
    else:
        with open(filepath, "w") as f:
            f.write(data)
    
    log.info(f"Fleet export saved: {filepath}")
    return filepath


def import_fleet(snapshot: Dict, dry_run: bool = False) -> Dict[str, Any]:
    """Import a fleet snapshot into this node.
    
    Args:
        snapshot: The parsed fleet export dict
        dry_run: If True, only validate and report, don't write
    
    Returns a summary of what was imported.
    """
    results = {"imported": [], "skipped": [], "errors": []}
    
    # Validate schema
    if snapshot.get("schemaVersion") != FLEET_SCHEMA_VERSION:
        results["errors"].append(f"Schema version mismatch: {snapshot.get('schemaVersion')} vs {FLEET_SCHEMA_VERSION}")
        return results
    
    # 1. Team config
    if "team" in snapshot and not dry_run:
        team_file = os.path.join(DATA_DIR, "team_config.json")
        os.makedirs(os.path.dirname(team_file), exist_ok=True)
        with open(team_file, "w") as f:
            json.dump(snapshot["team"], f, indent=2)
        results["imported"].append("team_config.json")
    
    # 2. Kanban
    if "kanban" in snapshot and not dry_run:
        kanban_file = os.path.join(DATA_DIR, "kanban_boards.json")
        with open(kanban_file, "w") as f:
            json.dump(snapshot["kanban"], f, indent=2)
        results["imported"].append("kanban_boards.json")
    
    # 3. Skills
    if "skills" in snapshot and not dry_run:
        skills_file = os.path.join(DATA_DIR, "skills_registry.json")
        with open(skills_file, "w") as f:
            json.dump(snapshot["skills"], f, indent=2)
        results["imported"].append("skills_registry.json")
    
    # 4. Dashboard settings
    if "dashboardSettings" in snapshot and not dry_run:
        settings_file = os.path.join(DATA_DIR, "dashboard_settings.json")
        with open(settings_file, "w") as f:
            json.dump(snapshot["dashboardSettings"], f, indent=2)
        results["imported"].append("dashboard_settings.json")
    
    log.info(f"Fleet import: {len(results['imported'])} items imported, {len(results['skipped'])} skipped")
    return results


def load_export(filepath: str, password: Optional[str] = None) -> Dict:
    """Load a fleet export from disk, optionally decrypting.
    
    Returns the parsed snapshot dict.
    """
    if filepath.endswith(".enc") and password:
        with open(filepath, "rb") as f:
            encrypted = f.read()
        data = _decrypt(encrypted, password)
    else:
        with open(filepath, "r") as f:
            data = f.read()
    
    return json.loads(data)


def _encrypt(data: str, password: str) -> bytes:
    """Encrypt data with scrypt-derived key (AES-256-GCM).
    
    Format: salt(16) + nonce(12) + ciphertext + tag(16)
    """
    import base64
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    except ImportError:
        # Fallback: simple XOR obfuscation (not secure, but better than plaintext)
        log.warning("cryptography not available, using weak obfuscation")
        key = hashlib.sha256(password.encode()).digest()
        return b"WEAK:" + base64.b64encode(bytes(a ^ key[i % len(key)] for i, a in enumerate(data.encode())))
    
    salt = secrets.token_bytes(16)
    kdf = Scrypt(salt=salt, length=32, n=2**16, r=8, p=1)
    key = kdf.derive(password.encode())
    nonce = secrets.token_bytes(12)
    aesgcm = AESGCM(key)
    ct = aesgcm.encrypt(nonce, data.encode(), None)
    return salt + nonce + ct


def _decrypt(encrypted: bytes, password: str) -> str:
    """Decrypt scrypt-encrypted data."""
    import base64
    if encrypted.startswith(b"WEAK:"):
        log.warning("Decrypting weakly obfuscated data")
        key = hashlib.sha256(password.encode()).digest()
        raw = base64.b64decode(encrypted[5:])
        return bytes(a ^ key[i % len(key)] for i, a in enumerate(raw)).decode()
    
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    except ImportError:
        raise RuntimeError("cryptography package required for decryption")
    
    salt = encrypted[:16]
    nonce = encrypted[16:28]
    ct = encrypted[28:]
    kdf = Scrypt(salt=salt, length=32, n=2**16, r=8, p=1)
    key = kdf.derive(password.encode())
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ct, None).decode()


def list_exports() -> list:
    """List available fleet exports."""
    if not os.path.isdir(EXPORT_DIR):
        return []
    files = sorted(Path(EXPORT_DIR).glob("fleet_export_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return [{"name": f.name, "size": f.stat().st_size, "encrypted": f.name.endswith(".enc")} for f in files]


def get_fleet_status() -> Dict[str, Any]:
    """Get fleet transfer status for dashboard."""
    exports = list_exports()
    return {
        "schemaVersion": FLEET_SCHEMA_VERSION,
        "exports": len(exports),
        "exportDir": EXPORT_DIR,
        "recentExports": exports[:5],
    }