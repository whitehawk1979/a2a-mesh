"""A2A Mesh Dashboard Authentication — User registration, login, session management.

SQLite-backed user store with bcrypt password hashing and JWT tokens.
Owner role can manage users; regular users can only use the dashboard.
"""
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("a2a_mesh.auth")

# Try to import bcrypt; fall back to hashlib if not available
try:
    import bcrypt
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False

DB_PATH = os.path.expanduser("~/.hermes/mesh_users.db")

# JWT-like token signing secret (generated once, stored in file)
SECRET_PATH = os.path.expanduser("~/.hermes/mesh_auth_secret")


def _get_secret() -> str:
    """Get or generate the signing secret for tokens."""
    if os.path.exists(SECRET_PATH):
        with open(SECRET_PATH, "r") as f:
            return f.read().strip()
    secret = uuid.uuid4().hex + uuid.uuid4().hex
    os.makedirs(os.path.dirname(SECRET_PATH), exist_ok=True)
    with open(SECRET_PATH, "w") as f:
        f.write(secret)
    return secret


SIGNING_SECRET = _get_secret()


@dataclass
class DashboardUser:
    """A dashboard user."""
    user_id: str
    username: str
    display_name: str
    role: str  # "owner" or "user"
    created_at: float = field(default_factory=time.time)
    last_login: float = 0.0
    is_active: bool = True

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "display_name": self.display_name,
            "role": self.role,
            "created_at": self.created_at,
            "last_login": self.last_login,
            "is_active": self.is_active,
        }


class AuthManager:
    """SQLite-backed user authentication for the dashboard.

    Roles:
        - owner: Full access, can manage users, view all data
        - user: Dashboard access, can send messages, view agents

    Features:
        - Password hashing (bcrypt if available, sha256+salt otherwise)
        - JWT-like token authentication
        - Session management with expiry
        - Rate limiting on login attempts
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._rate_limits: dict = {}  # username -> [timestamp, ...]
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                created_at REAL NOT NULL,
                last_login REAL DEFAULT 0,
                is_active INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")
        conn.commit()

        # Create default owner if no users exist
        cur = conn.execute("SELECT COUNT(*) FROM users")
        if cur.fetchone()[0] == 0:
            self.register_user("zsolt", "Lakatos Miklós Zsolt", "mesh2026", role="owner")
            log.info("Default owner user 'zsolt' created")

        conn.close()

    def _hash_password(self, password: str, salt: Optional[str] = None) -> tuple:
        """Hash a password with salt. Returns (hash, salt)."""
        if salt is None:
            salt = uuid.uuid4().hex[:16]

        if HAS_BCRYPT:
            hashed = bcrypt.hashpw((password + salt).encode(), bcrypt.gensalt(12)).decode()
        else:
            # Fallback: HMAC-SHA256
            hashed = hmac.new(
                SIGNING_SECRET.encode(),
                (password + salt).encode(),
                hashlib.sha256
            ).hexdigest()

        return hashed, salt

    def _verify_password(self, password: str, stored_hash: str, salt: str) -> bool:
        """Verify a password against stored hash and salt."""
        if HAS_BCRYPT and stored_hash.startswith("$2"):
            try:
                return bcrypt.checkpw((password + salt).encode(), stored_hash.encode())
            except Exception:
                pass

        # Fallback verification
        computed = hmac.new(
            SIGNING_SECRET.encode(),
            (password + salt).encode(),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(computed, stored_hash)

    def register_user(self, username: str, display_name: str, password: str, role: str = "user") -> Optional[DashboardUser]:
        """Register a new user. Returns the user object or None if username taken."""
        if len(username) < 2 or len(username) > 30:
            raise ValueError("Username must be 2-30 characters")
        if len(password) < 6:
            raise ValueError("Password must be at least 6 characters")
        if role not in ("owner", "user"):
            raise ValueError("Role must be 'owner' or 'user'")

        password_hash, salt = self._hash_password(password)
        user_id = uuid.uuid4().hex[:12]

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "INSERT INTO users (user_id, username, display_name, password_hash, salt, role, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_id, username.lower(), display_name, password_hash, salt, role, time.time())
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return None  # Username taken

        conn.close()
        return DashboardUser(
            user_id=user_id,
            username=username.lower(),
            display_name=display_name,
            role=role,
            created_at=time.time(),
        )

    def login(self, username: str, password: str) -> Optional[dict]:
        """Authenticate a user. Returns {user, token} or None."""
        # Rate limiting: max 5 attempts per minute
        now = time.time()
        attempts = self._rate_limits.get(username.lower(), [])
        attempts = [t for t in attempts if now - t < 60]
        if len(attempts) >= 5:
            raise ValueError("Too many login attempts. Try again in 1 minute.")
        attempts.append(now)
        self._rate_limits[username.lower()] = attempts

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT * FROM users WHERE username = ? AND is_active = 1",
            (username.lower(),)
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            return None

        if not self._verify_password(password, row["password_hash"], row["salt"]):
            return None

        # Generate token
        token = self._generate_token(row["user_id"])

        # Update last login
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE users SET last_login = ? WHERE user_id = ?", (time.time(), row["user_id"]))
        conn.commit()
        conn.close()

        user = DashboardUser(
            user_id=row["user_id"],
            username=row["username"],
            display_name=row["display_name"],
            role=row["role"],
            created_at=row["created_at"],
            last_login=time.time(),
            is_active=bool(row["is_active"]),
        )

        return {
            "user": user,
            "token": token,
        }

    def _generate_token(self, user_id: str, expiry_hours: int = 24) -> str:
        """Generate a JWT-like token."""
        expires = time.time() + (expiry_hours * 3600)
        payload = {
            "user_id": user_id,
            "exp": expires,
            "jti": uuid.uuid4().hex[:8],
        }
        payload_json = json.dumps(payload, sort_keys=True)
        signature = hmac.new(SIGNING_SECRET.encode(), payload_json.encode(), hashlib.sha256).hexdigest()
        token = f"{payload_json}:{signature}"

        # Store session
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (signature, user_id, time.time(), expires)
        )
        conn.commit()
        conn.close()

        return token

    def verify_token(self, token: str) -> Optional[DashboardUser]:
        """Verify a token and return the user. Returns None if invalid/expired."""
        if ":" not in token:
            return None

        payload_json, signature = token.rsplit(":", 1)
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            return None

        # Check expiry
        if payload.get("exp", 0) < time.time():
            return None

        # Verify signature
        expected = hmac.new(SIGNING_SECRET.encode(), payload_json.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None

        # Check session exists
        conn = sqlite3.connect(self.db_path)
        cur = conn.execute(
            "SELECT user_id FROM sessions WHERE token = ? AND expires_at > ?",
            (signature, time.time())
        )
        session = cur.fetchone()
        conn.close()

        if not session:
            return None

        # Get user
        return self.get_user(payload["user_id"])

    def logout(self, token: str):
        """Invalidate a session token."""
        if ":" not in token:
            return
        _, signature = token.rsplit(":", 1)
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM sessions WHERE token = ?", (signature,))
        conn.commit()
        conn.close()

    def get_user(self, user_id: str) -> Optional[DashboardUser]:
        """Get a user by ID."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        conn.close()

        if not row:
            return None

        return DashboardUser(
            user_id=row["user_id"],
            username=row["username"],
            display_name=row["display_name"],
            role=row["role"],
            created_at=row["created_at"],
            last_login=row["last_login"] or 0,
            is_active=bool(row["is_active"]),
        )

    def list_users(self) -> list:
        """List all users (owner only)."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM users ORDER BY created_at")
        rows = cur.fetchall()
        conn.close()

        return [DashboardUser(
            user_id=r["user_id"],
            username=r["username"],
            display_name=r["display_name"],
            role=r["role"],
            created_at=r["created_at"],
            last_login=r["last_login"] or 0,
            is_active=bool(r["is_active"]),
        ) for r in rows]

    def update_user(self, user_id: str, **kwargs) -> bool:
        """Update user fields. Returns True if successful."""
        allowed = {"display_name", "role", "is_active"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [user_id]

        conn = sqlite3.connect(self.db_path)
        conn.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
        conn.commit()
        conn.close()
        return True

    def change_password(self, user_id: str, new_password: str) -> bool:
        """Change a user's password."""
        if len(new_password) < 6:
            raise ValueError("Password must be at least 6 characters")

        password_hash, salt = self._hash_password(new_password)
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "UPDATE users SET password_hash = ?, salt = ? WHERE user_id = ?",
            (password_hash, salt, user_id)
        )
        conn.commit()
        conn.close()
        return True

    def delete_user(self, user_id: str) -> bool:
        """Deactivate a user (soft delete)."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
        return True

    def cleanup_sessions(self):
        """Remove expired sessions."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (time.time(),))
        conn.commit()
        conn.close()


# ─── Node Authentication (peer-to-peer) ───
# These classes handle peer node authentication (open, whitelist, TOFU modes)
# which is separate from dashboard user authentication above.

class AuthMode:
    """Node authentication modes for peer connections."""
    OPEN = "open"
    WHITELIST = "whitelist"
    TRUST_ON_FIRST_USE = "tofu"


@dataclass
class AuthConfig:
    """Configuration for node authentication."""
    mode: str = AuthMode.OPEN
    whitelist: set = field(default_factory=set)
    trusted_keys: dict = field(default_factory=dict)  # node_name -> public_key
    trust_center: str = ""  # Coordinator node name


@dataclass
class JoinRequest:
    """A node join request."""
    node_name: str
    node_role: str = "end_device"
    public_key: str = ""
    timestamp: float = field(default_factory=time.time)
    nonce: str = field(default_factory=lambda: uuid.uuid4().hex[:16])


class NodeAuthenticator:
    """Authenticates peer node join requests based on configured mode."""

    def __init__(self, config: AuthConfig = None):
        self.config = config or AuthConfig()
        self._trusted_keys: dict = dict(config.trusted_keys) if config else {}

    def authenticate_join(self, request: JoinRequest) -> tuple:
        """Authenticate a join request. Returns (accepted: bool, reason: str)."""
        if self.config.mode == AuthMode.OPEN:
            return True, "open_mode"

        if self.config.mode == AuthMode.WHITELIST:
            if request.node_name in self.config.whitelist:
                return True, "whitelisted"
            return False, f"node '{request.node_name}' not in whitelist"

        if self.config.mode == AuthMode.TRUST_ON_FIRST_USE:
            if request.node_name in self._trusted_keys:
                if self._trusted_keys[request.node_name] == request.public_key:
                    return True, "known_key"
                return False, f"key mismatch for '{request.node_name}'"
            # First use — trust the key
            self._trusted_keys[request.node_name] = request.public_key
            return True, "trust_on_first_use"

        return False, f"unknown auth mode: {self.config.mode}"