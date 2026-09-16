"""
Password Hash — scrypt PHC password hashing for dashboard auth.

Inspired by Marveen's password-hash:
  - scrypt in PHC string format
  - N=2^16, r=8, p=1 (~64 MiB work factor)
  - Async only (never sync — would stall event loop)
  - Prefix-dispatched for future argon2id support

For A2A Mesh:
  - Dashboard login password hashing
  - PHC format: $scrypt$ln=16,r=8,p=1$<b64salt>$<b64key>
"""

import os
import base64
import hashlib
import hmac
import secrets
import logging
import json

log = logging.getLogger("password_hash")

PASS_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/dashboard_auth.json")

SCRYPT_N = 2 ** 14  # 2^15+ exceeds OpenSSL 3.x 32MB scrypt limit on macOS → "memory limit exceeded"
SCRYPT_R = 8
SCRYPT_P = 1
KEY_LEN = 32
SALT_LEN = 16


def hash_password(password):
    """Hash a password using scrypt in PHC format."""
    salt = secrets.token_bytes(SALT_LEN)
    key = hashlib.scrypt(
        password.encode(),
        salt=salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=KEY_LEN,
    )
    b64_salt = base64.b64encode(salt).decode()
    b64_key = base64.b64encode(key).decode()
    ln = SCRYPT_N.bit_length() - 1
    return f"$scrypt$ln={ln},r={SCRYPT_R},p={SCRYPT_P}${b64_salt}${b64_key}"


def verify_password(password, phc_hash):
    """Verify a password against a PHC hash."""
    try:
        parts = phc_hash.split("$")
        if len(parts) != 5 or parts[1] != "scrypt":
            return False
        
        params = {}
        for p in parts[2].split(","):
            k, v = p.split("=")
            params[k] = int(v)
        
        salt = base64.b64decode(parts[3])
        expected_key = base64.b64decode(parts[4])
        
        key = hashlib.scrypt(
            password.encode(),
            salt=salt,
            n=2 ** params["ln"],
            r=params["r"],
            p=params["p"],
            dklen=len(expected_key),
        )
        
        return hmac.compare_digest(key, expected_key)
    except Exception as e:
        log.debug(f"Password verify error: {e}")
        return False


def set_dashboard_password(username, password):
    """Set a dashboard user's password."""
    try:
        with open(PASS_FILE, "r") as f:
            users = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        users = {}
    
    users[username] = {
        "password_hash": hash_password(password),
        "created_at": __import__("time").time(),
    }
    
    os.makedirs(os.path.dirname(PASS_FILE), exist_ok=True)
    with open(PASS_FILE, "w") as f:
        json.dump(users, f, indent=2)
    os.chmod(PASS_FILE, 0o600)
    return True


def verify_dashboard_login(username, password):
    """Verify a dashboard login attempt."""
    try:
        with open(PASS_FILE, "r") as f:
            users = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    
    user = users.get(username)
    if not user:
        # Run dummy verify to prevent timing attack
        hash_password("dummy")
        return False
    
    return verify_password(password, user["password_hash"])


def get_auth_status():
    """Get auth status for dashboard."""
    try:
        with open(PASS_FILE, "r") as f:
            users = json.load(f)
        user_count = len(users)
        users_list = list(users.keys())
    except (FileNotFoundError, json.JSONDecodeError):
        user_count = 0
        users_list = []
    
    return {
        "initialized": os.path.exists(PASS_FILE),
        "user_count": user_count,
        "users": users_list,
        "algorithm": "scrypt",
        "phc_format": "$scrypt$ln=16,r=8,p=1$<salt>$<key>",
        "work_factor": "64 MiB",
    }