"""R0 Honcho-Mesh bridge (v0.46.0): deterministic, 0-LLM context injection.

Debate consensus (2026-09-21, mesh /debate):
  - Snapshot-based: the mesh NEVER reads the live Honcho schema at wake time.
    A materialized snapshot table (fixed schema) is written hourly by Nova (Mac,
    where the Honcho PG lives) and read via a 10-min TTL local cache.
  - PG-down resilience: wake-time read returns an EMPTY block on any error —
    the wake prompt is never blocked by Honcho PG unavailability.
  - Write-time redaction: peer-allowlist + regex deny-list applied when the
    snapshot is WRITTEN, so every reader gets clean data (morzsa condition).

Design (Zsolt principle: core mesh deterministic, LLM optional layer):
  - Writer: nova only (honcho_dsn from config) — pulls peer cards +
    recent representations from the local Honcho PG, redacts, upserts into
    the shared mesh PG (mesh.honcho_context_snapshot).
  - Reader: any node at wake time — SELECT from the shared PG snapshot table
    (never the Honcho DB), 10-min TTL in-process cache, empty-on-error.

Snapshot schema (fixed, in the shared mesh PG):
  CREATE TABLE IF NOT EXISTS mesh.honcho_context_snapshot (
      peer_name      TEXT PRIMARY KEY,          -- honcho peer (observed)
      card_json      JSONB NOT NULL,            -- redacted peer card
      contexts_json  JSONB NOT NULL,            -- recent redacted representations
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
  );
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("a2a_mesh.honcho_bridge")

# ─── Redaction (write-time) ────────────────────────────────────────────────

# Peer allowlist: only these observed peers may enter the snapshot (family
# context stays out of every agent's prompt/logs — morzsa condition, SOUL rule).
DEFAULT_PEER_ALLOWLIST = [
    "zsolt",        # primary user
    "nova",         # the assistant itself
]

# Regex deny-list: matched (case-insensitive) values are dropped from card /
# representation fields before the snapshot is written.
DEFAULT_REDACT_PATTERNS = [
    r"\b(?:\d{1,3}\.){3}\d{1,3}\b",                         # IPv4 (dots only — dates keep intact)
    r"\bpass(?:word)?\b",                                  # password mentions
    r"\b(?:titok|jelszo|secret|token|api[_ -]?key)\b",     # secrets
    r"Hajnalka",                                           # family: wife name
    r"Kata\b",                                             # family: dog name
    r"\b(?:8912|7796)\d{6,}\b",                            # Telegram IDs (family)
]

# Cap on representation text length kept per peer.
MAX_REPR_CHARS = 1200
MAX_REPRS_PER_PEER = 3


def redact_value(value: Any, patterns: List[str]) -> Any:
    """Recursively redact strings inside a JSON-ish structure (write-time)."""
    if isinstance(value, str):
        out = value
        for pat in patterns:
            try:
                out = re.sub(pat, "[REDACTED]", out, flags=re.IGNORECASE)
            except re.error:
                continue
        return out
    if isinstance(value, list):
        return [redact_value(v, patterns) for v in value]
    if isinstance(value, dict):
        return {k: redact_value(v, patterns) for k, v in value.items()}
    return value


def redact_text(text: str, patterns: List[str]) -> str:
    """Redact a flat string (representation content)."""
    out = text or ""
    for pat in patterns:
        try:
            out = re.sub(pat, "[REDACTED]", out, flags=re.IGNORECASE)
        except re.error:
            continue
    return out


# ─── Writer (nova only) ───────────────────────────────────────────────────

HONCHO_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS mesh.honcho_context_snapshot (
    peer_name     TEXT PRIMARY KEY,
    card_json     JSONB NOT NULL,
    contexts_json JSONB NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


async def write_honcho_snapshot(
    honcho_pool: Any,
    mesh_pool: Any,
    peer_allowlist: Optional[List[str]] = None,
    redact_patterns: Optional[List[str]] = None,
) -> Dict[str, int]:
    """Pull peer cards + recent representations from the local Honcho PG,
    redact, and upsert into the shared mesh PG snapshot table.

    Deterministic: pure SQL + regex, zero LLM calls. Errors never raise into
    the caller loop — they are logged and a {written: 0} returned.
    """
    allowlist = [p.lower() for p in (peer_allowlist or DEFAULT_PEER_ALLOWLIST)]
    patterns = redact_patterns or DEFAULT_REDACT_PATTERNS
    written = 0
    try:
        # 1. Ensure the snapshot table exists in the shared PG.
        async with mesh_pool.acquire() as mconn:
            await mconn.execute(HONCHO_SNAPSHOT_DDL)
            # Prune: only keep the allowlisted peers' snapshots — a peer
            # removed from the allowlist disappears from the next snapshot.
            if allowlist:
                placeholders = ",".join(f"${i+1}" for i in range(len(allowlist)))
                await mconn.execute(
                    f"DELETE FROM mesh.honcho_context_snapshot WHERE peer_name <> ALL(ARRAY[{placeholders}]::text[])",
                    *allowlist,
                )

        # 2. Pull peer cards from Honcho (peers table: configuration holds the card).
        cards: Dict[str, dict] = {}
        async with honcho_pool.acquire() as hconn:
            rows = await hconn.fetch(
                """
                SELECT name, configuration, metadata, internal_metadata
                FROM peers
                WHERE lower(name) = ANY($1::text[])
                """,
                allowlist,
            )
            for r in rows:
                def _j(v) -> dict:
                    if v is None:
                        return {}
                    if isinstance(v, dict):
                        return v
                    try:
                        parsed = json.loads(v)
                        return parsed if isinstance(parsed, dict) else {}
                    except Exception:
                        return {}
                card = {
                    "name": r["name"],
                    "configuration": redact_value(_j(r["configuration"]), patterns),
                    "metadata": redact_value(_j(r["metadata"]), patterns),
                }
                cards[r["name"].lower()] = card

            # 3. Recent representations per allowed peer (documents table,
            #    observer != observed => derived knowledge about that peer).
            reprs: Dict[str, list] = {p: [] for p in allowlist}
            rep_rows = await hconn.fetch(
                """
                SELECT observed, content, created_at, times_derived
                FROM documents
                WHERE lower(observed) = ANY($1::text[])
                  AND length(content) > 20
                ORDER BY created_at DESC
                """,
                allowlist,
            )
            for r in rep_rows:
                key = r["observed"].lower()
                if key not in reprs:
                    continue
                if len(reprs[key]) >= MAX_REPRS_PER_PEER:
                    continue
                txt = redact_text(r["content"][:MAX_REPR_CHARS], patterns)
                if txt.strip():
                    entry = {"content": txt, "created_at": r["created_at"].isoformat() if hasattr(r["created_at"], "isoformat") else str(r["created_at"]), "times_derived": r["times_derived"]}
                    reprs[key].append(entry)

        # 4. Upsert into the shared mesh PG.
        async with mesh_pool.acquire() as mconn:
            for peer, card in cards.items():
                contexts = reprs.get(peer, [])
                await mconn.execute(
                    """
                    INSERT INTO mesh.honcho_context_snapshot (peer_name, card_json, contexts_json, updated_at)
                    VALUES ($1, $2::jsonb, $3::jsonb, now())
                    ON CONFLICT (peer_name) DO UPDATE
                    SET card_json = EXCLUDED.card_json,
                        contexts_json = EXCLUDED.contexts_json,
                        updated_at = now()
                    """,
                    peer,
                    json.dumps(card, ensure_ascii=False),
                    json.dumps(contexts, ensure_ascii=False),
                )
                written += 1
        log.info(f"🔄 Honcho snapshot written: {written} peers")
        return {"written": written}
    except Exception as e:
        log.debug(f"Honcho snapshot write failed (non-fatal): {e}")
        return {"written": 0}


# ─── Reader (any node, wake-time) ─────────────────────────────────────────

_CACHE_TTL = 600  # 10 minutes (debate consensus)
_cache: Dict[str, tuple] = {}  # peer_name -> (fetched_at, payload)


async def get_honcho_context(
    mesh_pool: Any,
    peer_name: str,
    chat_username: Optional[str] = None,
    max_chars: int = 1500,
) -> Optional[str]:
    """Return a formatted, REDACTED context block for a peer from the shared
    PG snapshot table (never the Honcho DB).

    - 10-minute TTL in-process cache.
    - PG-down or table-missing => empty string (never blocks the wake).
    - peer_name: the honcho peer whose card injects into THIS wake prompt.
      Falls back to chat_username (the human the agent is talking to).
    """
    peer_key = (peer_name or chat_username or "").lower()
    if not peer_key:
        return None
    now = time.monotonic()
    cached = _cache.get(peer_key)
    if cached and (now - cached[0]) < _CACHE_TTL:
        payload = cached[1]
    else:
        payload = None
        try:
            if mesh_pool is not None and hasattr(mesh_pool, "acquire"):
                async with mesh_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT card_json, contexts_json, updated_at
                        FROM mesh.honcho_context_snapshot
                        WHERE peer_name = $1
                        """,
                        peer_key,
                    )
                if row:
                    payload = {
                        "card": row["card_json"] if isinstance(row["card_json"], dict) else _safe_json(row["card_json"]),
                        "contexts": row["contexts_json"] if isinstance(row["contexts_json"], dict) else _safe_json(row["contexts_json"]),
                        "updated_at": str(row["updated_at"])[:19],
                    }
        except Exception as e:
            log.debug(f"Honcho context read failed (non-fatal): {e}")
            payload = None
        if payload is None:
            # Negative caching: remember the miss briefly to avoid hammering
            # the shared PG on every wake when the table is empty/missing.
            _cache[peer_key] = (now, None)
            return None
        _cache[peer_key] = (now, payload)

    ctx = format_honcho_context(payload, max_chars=max_chars)
    return ctx or None


def _safe_json(v) -> Any:
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return {}


def format_honcho_context(payload: dict, max_chars: int = 1500) -> str:
    """Format a snapshot row into a compact prompt block."""
    card = (payload or {}).get("card") or {}
    contexts = (payload or {}).get("contexts") or []
    if not card and not contexts:
        return ""
    parts: List[str] = []
    name = card.get("name", "peer")
    # Card: compact key=value rendering of the configuration/metadata
    card_lines = []
    for section in ("configuration", "metadata"):
        section_data = card.get(section) or {}
        if not isinstance(section_data, dict):
            continue
        for k, v in section_data.items():
            if isinstance(v, (str, int, float, bool)):
                s = str(v).strip()
                if s and s not in ("{}", "[REDACTED]"):
                    card_lines.append(f"{k}: {s[:120]}")
            elif isinstance(v, list) and v:
                joined = "; ".join(str(x)[:60] for x in v[:4])
                card_lines.append(f"{k}: {joined}")
            elif isinstance(v, dict) and v:
                joined = "; ".join(f"{k2}={str(v2)[:40]}" for k2, v2 in list(v.items())[:4])
                card_lines.append(f"{k}: {joined}")
    if card_lines:
        parts.append("🪪 Peer card: " + name + " — " + " | ".join(card_lines[:8]))
    for i, c in enumerate(contexts[:MAX_REPRS_PER_PEER], 1):
        content = (c.get("content") or "").strip()
        if content:
            parts.append(f"🧩 Repr {i}: {content[:300]}")
    block = "\n".join(parts)
    if len(block) > max_chars:
        block = block[: max_chars - 1] + "…"
    return block