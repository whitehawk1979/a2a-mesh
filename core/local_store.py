"""A2A Mesh Local Store — SQLite fallback for offline/decentralized operation.

When PG is unavailable, messages are stored locally in SQLite.
When PG comes back online, messages are synced (flushed) to PG.

Features:
- Local message queue for offline operation
- Automatic sync when PG recovers
- File metadata cache for P2P file transfers
- Steer directive tracking (local mirror of PG data)
"""
import json
import logging
import sqlite3
import time
import os
from typing import List, Optional, Dict, Any
from pathlib import Path

log = logging.getLogger("a2a_mesh.local_store")


class LocalStore:
    """SQLite-based local store for decentralized mesh operation.

    Falls back to local storage when PG is unavailable,
    syncs back when PG recovers.
    """

    def __init__(self, node_name: str, db_path: Optional[str] = None):
        self.node_name = node_name
        self.db_path = db_path or os.path.expanduser(f"~/.hermes/scripts/a2a_mesh/local_store_{node_name}.db")
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_db()

    def _ensure_db(self):
        """Create database and tables if they don't exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS outbound_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id TEXT UNIQUE NOT NULL,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                msg_type TEXT NOT NULL,
                priority INTEGER DEFAULT 5,
                payload TEXT NOT NULL,
                routing_mode TEXT DEFAULT 'hybrid',
                status TEXT DEFAULT 'pending',
                created_at REAL NOT NULL,
                retry_count INTEGER DEFAULT 0,
                last_retry REAL,
                pg_synced INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_outbound_status ON outbound_queue(status, priority);
            CREATE INDEX IF NOT EXISTS idx_outbound_pg ON outbound_queue(pg_synced);

            CREATE TABLE IF NOT EXISTS inbound_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id TEXT UNIQUE NOT NULL,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                msg_type TEXT NOT NULL,
                priority INTEGER DEFAULT 5,
                payload TEXT NOT NULL,
                from_transport TEXT,
                processed INTEGER DEFAULT 0,
                created_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_inbound_processed ON inbound_messages(processed);

            CREATE TABLE IF NOT EXISTS file_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT UNIQUE NOT NULL,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                transfer_method TEXT NOT NULL,  -- 'minio', 'p2p', 'pg_base64'
                chunk_index INTEGER DEFAULT 0,
                total_chunks INTEGER DEFAULT 1,
                chunk_data BLOB,
                status TEXT DEFAULT 'pending',  -- pending, transferring, complete, failed
                created_at REAL NOT NULL,
                completed_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_file_status ON file_transfers(status, recipient);

            CREATE TABLE IF NOT EXISTS steer_cache (
                id TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                action TEXT NOT NULL,
                params TEXT,
                priority INTEGER DEFAULT 5,
                status TEXT DEFAULT 'pending',
                result TEXT,
                created_at REAL NOT NULL,
                updated_at REAL
            );

            CREATE TABLE IF NOT EXISTS peer_status (
                node_name TEXT PRIMARY KEY,
                role TEXT,
                address TEXT,
                last_seen REAL,
                pg_available INTEGER DEFAULT 0,
                minio_available INTEGER DEFAULT 0,
                p2p_available INTEGER DEFAULT 0
            );
        """)
        self._conn.commit()
        log.info(f"Local store initialized: {self.db_path}")

    # ─── Outbound Queue ───────────────────────────────────────────

    def enqueue_outbound(self, msg_id: str, sender: str, recipient: str,
                         msg_type: str, priority: int, payload: str,
                         routing_mode: str = "hybrid") -> bool:
        """Queue a message for sending (used when PG is down)."""
        try:
            self._conn.execute(
                """INSERT OR IGNORE INTO outbound_queue
                   (msg_id, sender, recipient, msg_type, priority, payload, routing_mode, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                (msg_id, sender, recipient, msg_type, priority, payload, routing_mode, time.time())
            )
            self._conn.commit()
            return True
        except Exception as e:
            log.error(f"Failed to enqueue outbound {msg_id[:8]}: {e}")
            return False

    def get_pending_outbound(self, limit: int = 50) -> List[Dict]:
        """Get pending outbound messages ordered by priority (P10 first)."""
        rows = self._conn.execute(
            """SELECT * FROM outbound_queue
               WHERE status = 'pending' AND pg_synced = 0
               ORDER BY -priority, created_at ASC LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_outbound_sent(self, msg_id: str):
        """Mark outbound message as sent."""
        self._conn.execute(
            "UPDATE outbound_queue SET status = 'sent' WHERE msg_id = ?",
            (msg_id,)
        )
        self._conn.commit()

    def mark_outbound_pg_synced(self, msg_id: str):
        """Mark outbound message as synced to PG."""
        self._conn.execute(
            "UPDATE outbound_queue SET pg_synced = 1 WHERE msg_id = ?",
            (msg_id,)
        )
        self._conn.commit()

    def increment_retry(self, msg_id: str):
        """Increment retry count for an outbound message."""
        self._conn.execute(
            """UPDATE outbound_queue
               SET retry_count = retry_count + 1, last_retry = ?
               WHERE msg_id = ?""",
            (time.time(), msg_id)
        )
        self._conn.commit()

    def cleanup_outbound(self, max_age_hours: int = 1):
        """Remove old outbound messages (synced and pending).
        
        Synced messages are cleaned after max_age_hours.
        Pending messages older than max_age_hours are also cleaned — 
        if they haven't been delivered in that time, they're stale.
        Heartbeat/ACK messages are never enqueued (filtered in router),
        so this only affects real message data.
        """
        cutoff = time.time() - (max_age_hours * 3600)
        # Clean synced messages
        synced = self._conn.execute(
            "DELETE FROM outbound_queue WHERE pg_synced = 1 AND created_at < ?",
            (cutoff,)
        ).rowcount
        # Clean stale pending messages (older than max_age_hours)
        # These are messages that were never synced — likely broadcast heartbeats
        # or messages that already arrived via another transport
        pending = self._conn.execute(
            "DELETE FROM outbound_queue WHERE pg_synced = 0 AND created_at < ?",
            (cutoff,)
        ).rowcount
        self._conn.commit()
        if synced > 0 or pending > 0:
            log.info(f"Cleanup: removed {synced} synced, {pending} stale pending outbound messages")
        return synced + pending

    # ─── Inbound Messages ─────────────────────────────────────────

    def store_inbound(self, msg_id: str, sender: str, recipient: str,
                      msg_type: str, priority: int, payload: str,
                      from_transport: str = "unknown") -> bool:
        """Store a received message locally."""
        try:
            self._conn.execute(
                """INSERT OR IGNORE INTO inbound_messages
                   (msg_id, sender, recipient, msg_type, priority, payload, from_transport, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (msg_id, sender, recipient, msg_type, priority, payload, from_transport, time.time())
            )
            self._conn.commit()
            return True
        except Exception as e:
            log.error(f"Failed to store inbound {msg_id[:8]}: {e}")
            return False

    def get_unprocessed_inbound(self, limit: int = 50) -> List[Dict]:
        """Get unprocessed inbound messages."""
        rows = self._conn.execute(
            """SELECT * FROM inbound_messages
               WHERE processed = 0
               ORDER BY -priority, created_at ASC LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_inbound_processed(self, msg_id: str):
        """Mark inbound message as processed."""
        self._conn.execute(
            "UPDATE inbound_messages SET processed = 1 WHERE msg_id = ?",
            (msg_id,)
        )
        self._conn.commit()

    # ─── File Transfers ────────────────────────────────────────────

    def store_file_chunk(self, file_id: str, filename: str, size: int,
                         sender: str, recipient: str, transfer_method: str,
                         chunk_index: int = 0, total_chunks: int = 1,
                         chunk_data: bytes = b'') -> bool:
        """Store a file chunk for P2P transfer."""
        try:
            self._conn.execute(
                """INSERT OR REPLACE INTO file_transfers
                   (file_id, filename, size, sender, recipient, transfer_method,
                    chunk_index, total_chunks, chunk_data, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                (file_id, filename, size, sender, recipient, transfer_method,
                 chunk_index, total_chunks, chunk_data, time.time())
            )
            self._conn.commit()
            return True
        except Exception as e:
            log.error(f"Failed to store file chunk {file_id[:8]}: {e}")
            return False

    def get_pending_file_chunks(self, recipient: str = "", limit: int = 50) -> List[Dict]:
        """Get pending file chunks for a recipient."""
        if recipient:
            rows = self._conn.execute(
                """SELECT * FROM file_transfers
                   WHERE status = 'pending' AND recipient = ?
                   ORDER BY chunk_index ASC LIMIT ?""",
                (recipient, limit)
            ).fetchall()
        else:
            rows = self._conn.execute(
                """SELECT * FROM file_transfers
                   WHERE status = 'pending'
                   ORDER BY created_at ASC LIMIT ?""",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_file_complete(self, file_id: str):
        """Mark file transfer as complete."""
        self._conn.execute(
            "UPDATE file_transfers SET status = 'complete', completed_at = ? WHERE file_id = ?",
            (time.time(), file_id)
        )
        self._conn.commit()

    # ─── Steer Cache ──────────────────────────────────────────────

    def cache_steer(self, steer_id: str, sender: str, action: str,
                    params: str, priority: int, status: str = "pending"):
        """Cache a steer directive locally."""
        self._conn.execute(
            """INSERT OR REPLACE INTO steer_cache
               (id, sender, action, params, priority, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (steer_id, sender, action, params, priority, status, time.time(), time.time())
        )
        self._conn.commit()

    def update_steer_status(self, steer_id: str, status: str, result: str = None):
        """Update steer directive status."""
        self._conn.execute(
            """UPDATE steer_cache SET status = ?, result = ?, updated_at = ?
               WHERE id = ?""",
            (status, result, time.time(), steer_id)
        )
        self._conn.commit()

    def get_active_steers(self) -> List[Dict]:
        """Get active (non-completed) steer directives."""
        rows = self._conn.execute(
            """SELECT * FROM steer_cache
               WHERE status IN ('pending', 'executing')
               ORDER BY -priority, created_at ASC"""
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── Peer Status ──────────────────────────────────────────────

    def update_peer_status(self, node_name: str, role: str = "", address: str = "",
                           pg_available: bool = False, minio_available: bool = False,
                           p2p_available: bool = False):
        """Update peer availability status."""
        self._conn.execute(
            """INSERT OR REPLACE INTO peer_status
               (node_name, role, address, last_seen, pg_available, minio_available, p2p_available)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (node_name, role, address, time.time(),
             int(pg_available), int(minio_available), int(p2p_available))
        )
        self._conn.commit()

    def get_peer_status(self, node_name: str) -> Optional[Dict]:
        """Get a peer's availability status."""
        row = self._conn.execute(
            "SELECT * FROM peer_status WHERE node_name = ?",
            (node_name,)
        ).fetchone()
        return dict(row) if row else None

    def get_available_peers(self, require_p2p: bool = True) -> List[Dict]:
        """Get peers that are available (seen in last 5 minutes)."""
        cutoff = time.time() - 300  # 5 minutes
        query = "SELECT * FROM peer_status WHERE last_seen > ?"
        params = [cutoff]
        if require_p2p:
            query += " AND p2p_available = 1"
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    # ─── Sync ─────────────────────────────────────────────────────

    def get_unsynced_count(self) -> Dict[str, int]:
        """Get count of unsynced messages."""
        outbound = self._conn.execute(
            "SELECT COUNT(*) FROM outbound_queue WHERE pg_synced = 0"
        ).fetchone()[0]
        inbound = self._conn.execute(
            "SELECT COUNT(*) FROM inbound_messages WHERE processed = 0"
        ).fetchone()[0]
        files = self._conn.execute(
            "SELECT COUNT(*) FROM file_transfers WHERE status = 'pending'"
        ).fetchone()[0]
        return {"unsynced_outbound": outbound, "unprocessed_inbound": inbound, "pending_files": files}

    def get_stats(self) -> Dict[str, Any]:
        """Get local store statistics."""
        return {
            "outbound_pending": self._conn.execute(
                "SELECT COUNT(*) FROM outbound_queue WHERE status = 'pending'"
            ).fetchone()[0],
            "outbound_synced": self._conn.execute(
                "SELECT COUNT(*) FROM outbound_queue WHERE pg_synced = 1"
            ).fetchone()[0],
            "inbound_unprocessed": self._conn.execute(
                "SELECT COUNT(*) FROM inbound_messages WHERE processed = 0"
            ).fetchone()[0],
            "inbound_total": self._conn.execute(
                "SELECT COUNT(*) FROM inbound_messages"
            ).fetchone()[0],
            "files_pending": self._conn.execute(
                "SELECT COUNT(*) FROM file_transfers WHERE status = 'pending'"
            ).fetchone()[0],
            "files_complete": self._conn.execute(
                "SELECT COUNT(*) FROM file_transfers WHERE status = 'complete'"
            ).fetchone()[0],
            "active_steers": self._conn.execute(
                "SELECT COUNT(*) FROM steer_cache WHERE status IN ('pending', 'executing')"
            ).fetchone()[0],
            "peers": self._conn.execute(
                "SELECT COUNT(*) FROM peer_status WHERE last_seen > ?",
                (time.time() - 300,)
            ).fetchone()[0],
        }

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            log.info("Local store closed")