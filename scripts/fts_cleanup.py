#!/usr/bin/env python3
"""Weekly FTS cleanup — vacuums state.db and drops ONLY dead FTS shadow tables.

Root-cause fix (2026-09-02): the old script dropped ALL messages_fts* tables
INCLUDING messages_fts_trigram, which hermes uses for live session-transcript
appends. Hermes recreates the tables lazily on next search, but until then every
append_message fails with "no such table: main.messages_fts_trigram" and session
transcripts are lost (retried forever). The trigram FTS is actively used and must
be preserved; only the unused legacy messages_fts tables are dropped.

Safe behavior:
- Keeps messages_fts_trigram (active substring search) + its triggers.
- Drops the legacy messages_fts vtable FIRST, then any leftover shadow
  tables/views/triggers (non-trigram), then VACUUMs.
- WAL checkpoint before vacuum keeps session-storage writes healthy.
- busy_timeout=120s: must run when the gateway is idle (e.g. Sun 3am cron).

Drop-order fix (2026-09-15, found on Runa): the legacy messages_fts VIRTUAL
table must be dropped FIRST, while its shadow tables are still intact.
SQLite re-parses vtable schemas after every schema change; with the shadows
already gone the FTS5 constructor fails ("vtable constructor failed:
messages_fts") and the whole transaction rolls back. The old
iterate-and-drop loop worked only when sqlite_master row order happened to
list the vtable first (Nova); on Runa the shadows came first -> rollback.
Explicit vtable-first order makes it deterministic and idempotent.
"""
import sqlite3, os, sys

db_path = os.path.expanduser("~/.hermes/state.db")
if not os.path.exists(db_path):
    print("state.db not found")
    sys.exit(0)

conn = sqlite3.connect(db_path, timeout=120)
conn.execute("PRAGMA busy_timeout=120000")
cur = conn.cursor()

# 1. WAL checkpoint first (64MB->0 keeps session-storage writes healthy)
print("WAL checkpoint...")
cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")

# 2. Drop orphan triggers on legacy messages_fts (NOT the trigram ones)
for (t,) in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'messages_fts%' "
    "AND name NOT LIKE 'messages_fts_trigram%'"
).fetchall():
    cur.execute(f"DROP TRIGGER IF EXISTS {t}")
    print(f"Dropped trigger: {t}")

# 3. Drop legacy FTS views (not trigram)
for (t,) in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='view' AND name LIKE 'messages_fts%' "
    "AND name NOT LIKE 'messages_fts_trigram%'"
).fetchall():
    cur.execute(f"DROP VIEW IF EXISTS {t}")
    print(f"Dropped view: {t}")

# 3. Drop the legacy vtable FIRST — shadow tables must still exist for the
#    FTS5 constructor to succeed. DROP vtable auto-removes its shadow tables.
#    FALLBACK: if the vtable is already corrupt (constructor fails even with
#    intact shadows — observed on Runa 2026-09-15), do sqlite_master surgery
#    via writable_schema: delete the vtable + shadow rows directly, then let
#    VACUUM rebuild the file without them. Standard fts5 recovery technique.
#    A full .db backup MUST exist before this path (see header).
try:
    cur.execute("DROP TABLE IF EXISTS messages_fts")
    print("Dropped vtable: messages_fts")
except sqlite3.DatabaseError as e:
    print(f"vtable drop failed ({e}) -> writable_schema surgery")
    cur.execute("PRAGMA writable_schema=ON")
    rows = cur.execute(
        "SELECT type, name FROM sqlite_master WHERE name LIKE 'messages_fts%' "
        "AND name NOT LIKE 'messages_fts_trigram%'"
    ).fetchall()
    for obj_type, name in rows:
        cur.execute("DELETE FROM sqlite_master WHERE type=? AND name=?", (obj_type, name))
        print(f"sqlite_master surgery: removed {obj_type} {name}")
    cur.execute("PRAGMA writable_schema=OFF")
    cur.execute("PRAGMA integrity_check(1)")
    ic = cur.fetchone()
    print(f"integrity_check: {ic[0] if ic else '?'}")

# 4. Drop any leftover legacy shadow tables + views (non-trigram),
#    keep messages_fts_trigram* intact — actively used by hermes_state_search.
for (t,) in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='view' AND name LIKE 'messages_fts%' "
    "AND name NOT LIKE 'messages_fts_trigram%'"
).fetchall():
    cur.execute(f"DROP VIEW IF EXISTS {t}")
    print(f"Dropped view: {t}")
for (t,) in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'messages_fts%' "
    "AND name NOT LIKE 'messages_fts_trigram%'"
).fetchall():
    cur.execute(f"DROP TABLE IF EXISTS {t}")
    print(f"Dropped leftover table: {t}")

conn.commit()

# 5. VACUUM
print("VACUUM...")
cur.execute("VACUUM")
conn.close()

size = os.path.getsize(db_path) / 1024 / 1024
print(f"state.db: {size:.1f}MB")