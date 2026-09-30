"""Wake 2.0 — determinisztikus wake könyvtár (dedup, coalescing, delivered-ack, watchdog).

Nova fejlesztés, agent-ötlet alapján (morzsa: wake-ack + dedup; runa: coalescing;
morzsa: watchdog; tor: capability-registry alap). Teljesen LLM-mentes.

Használati mód (hívó oldal, pl. dashboard_chat.py):
    wl = await get_wake_log(pool)
    ok, wake_id, status = await wl.send_wake(
        target_agent='tor', target_host='100.74.221.46', health_port=8650,
        prompt=..., sender='zsolt', message_id=msg_uuid,
        payload={...},  # a /api/wake-agent POST body
    )
    status: 'sent' | 'coalesced' | 'failed'  (coalesced = 60s-en belül már volt wake)

Fogadó oldal (dashboard_agents._api_wake_agent feldolgozás elején):
    await mark_delivered(pool, wake_id)   # delivered-ack visszacsatolás

Watchdog (periodikus, node.py heartbeat blokkban vagy külön task):
    await run_wake_watchdog(pool, node)  # 5p delivered-at nélkül → retry (max 3)
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid as _uuid
from typing import Any, Dict, Optional, Tuple

import aiohttp

log = logging.getLogger("a2a_mesh.wake")

# ── Konstansok ────────────────────────────────────────────────────────────────
COALESCE_WINDOW_S = 60          # ezen belül wake-ek összevonódnak (runa ötlet)
DELIVER_TIMEOUT_S = 300         # 5 perc delivered-ack nélkül → watchdog retry
WATCHDOG_INTERVAL_S = 60        # watchdog futási gyakoriság
MAX_ATTEMPTS = 3                # watchdog újrapróbálkozásai
WAKE_HTTP_TIMEOUT_S = 120       # /api/wake-agent POST timeout
WATCHDOG_WAKE_URL_PATH = "/api/wake-agent"


async def get_wake_log(pool) -> "WakeLog":
    """WakeLog instance (pool: asyncpg pool a mesh PG-re)."""
    return WakeLog(pool)


def _hash_prompt(prompt: str) -> str:
    return hashlib.sha256((prompt or "").encode("utf-8")).hexdigest()[:32]


class WakeLog:
    def __init__(self, pool):
        self.pool = pool

    async def _ensure_table(self):
        """Idempotens tábla-ellenőrzés (ha a migráció nem futott le, létrehozza)."""
        try:
            await self.pool.execute("""
                CREATE TABLE IF NOT EXISTS mesh.wake_log (
                    wake_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    target_agent TEXT NOT NULL,
                    target_host TEXT,
                    wake_type TEXT NOT NULL DEFAULT 'agent',
                    prompt_hash TEXT,
                    prompt TEXT,
                    message_id TEXT,
                    sender TEXT,
                    status TEXT NOT NULL DEFAULT 'sent',
                    attempts INT NOT NULL DEFAULT 1,
                    max_attempts INT NOT NULL DEFAULT 3,
                    sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    delivered_at TIMESTAMPTZ,
                    first_ack_at TIMESTAMPTZ,
                    last_error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )""")
            await self.pool.execute("""
                CREATE INDEX IF NOT EXISTS idx_wake_log_target_recent
                ON mesh.wake_log (target_agent, sent_at DESC)""")
            await self.pool.execute("""
                CREATE INDEX IF NOT EXISTS idx_wake_log_status_timeout
                ON mesh.wake_log (status, sent_at) WHERE status = 'sent'""")
        except Exception as e:
            log.debug(f"wake_log tábla ensure sikertelen: {e}")

    async def send_wake(
        self,
        target_agent: str,
        target_host: str,
        health_port: int,
        prompt: str,
        sender: str,
        message_id: str = "",
        wake_type: str = "agent",
        payload: Optional[Dict[str, Any]] = None,
        mesh_secret: str = "mesh-wake-secret-2026",
        coalesce_window_s: int = COALESCE_WINDOW_S,
    ) -> Tuple[bool, Optional[str], str]:
        """Wake kérés loggolva + dedup-olva + elküldve.

        Returns (ok, wake_id, status):
          ok=True, status='sent'      — elküldve (HTTP 200/202)
          ok=True, status='coalesced' — 60s-en belül volt már wake erre a targetre,
                                         NEM küldtünk újat; a hívónak jelezni kell,
                                         hogy az üzenet a meglévő wake-hoz csatlakozik
          ok=False, status='failed'   — HTTP hiba (logolva, retry a watchdogra)
        """
        await self._ensure_table()

        ph = _hash_prompt(prompt)

        # ── Dedup/coalescing: van-e aktív wake ugyanarra a targetre? ──
        row = await self.pool.fetchrow("""
            SELECT wake_id, status, sent_at, attempts
            FROM mesh.wake_log
            WHERE target_agent = $1
              AND sent_at > now() - make_interval(secs => $2)
              AND status IN ('sent', 'delivered')
            ORDER BY sent_at DESC
            LIMIT 1
        """, target_agent, coalesce_window_s)
        if row:
            await self.pool.execute("""
                UPDATE mesh.wake_log
                SET updated_at = now(),
                    prompt_hash = $2
                WHERE wake_id = $1
            """, row["wake_id"], ph)
            log.info(f"🔁 Wake coalesced → {target_agent} (aktív wake {row['wake_id']} < {coalesce_window_s}s)")
            return (True, row["wake_id"], "coalesced")

        # ── Új wake sor beszúrása (teljes prompt tárolása a watchdog retry-hoz) ──
        wake_id = str(_uuid.uuid4())
        await self.pool.execute("""
            INSERT INTO mesh.wake_log
                (wake_id, target_agent, target_host, wake_type, prompt_hash,
                 message_id, sender, status, attempts, prompt)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'sent', 1, $8)
        """, _uuid.UUID(wake_id), target_agent, target_host, wake_type, ph,
             message_id or None, sender, prompt[:2000])

        # ── HTTP POST a /api/wake-agent endpointra ──
        # (SSH-alapú wake, pl. MCP DM-wake: health_port=0 → csak log, küldés a hívóban)
        if not health_port:
            return (True, wake_id, "sent")
        _pl = dict(payload or {})
        _pl["wake_id"] = wake_id  # fogadó oldali delivered-ack visszacsatoláshoz
        ok, err = await _post_wake(
            target_host, health_port, prompt, _pl,
            agent_name=target_agent, sender=sender, message_id=message_id,
            mesh_secret=mesh_secret,
        )
        if not ok:
            # Nem írjuk felül a 'delivered' státuszt — a fogadó esetleg már ackolta,
            # csak a lassú HTTP-válasz miatt timeoutolt a küldő oldalon.
            await self.pool.execute("""
                UPDATE mesh.wake_log
                SET status=CASE WHEN status='delivered' THEN 'delivered' ELSE 'failed' END,
                    last_error=$2, updated_at=now()
                WHERE wake_id = $1
            """, _uuid.UUID(wake_id), err)
            return (False, wake_id, "failed")
        return (True, wake_id, "sent")


async def _post_wake(
    host: str,
    port: int,
    prompt: str,
    payload: Dict[str, Any],
    agent_name: str,
    sender: str,
    message_id: str,
    mesh_secret: str,
) -> Tuple[bool, Optional[str]]:
    """HTTP POST /api/wake-agent. Returns (ok, error)."""
    url = f"http://{host}:{port}{WATCHDOG_WAKE_URL_PATH}"
    body = dict(payload)
    body.setdefault("prompt", prompt)
    body.setdefault("agent_name", agent_name)
    body.setdefault("sender", sender)
    body.setdefault("mesh_secret", mesh_secret)
    if message_id:
        body.setdefault("chat_msg_uuid", message_id)
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                url, json=body,
                timeout=aiohttp.ClientTimeout(total=WAKE_HTTP_TIMEOUT_S),
            ) as resp:
                if resp.status in (200, 202):
                    return (True, None)
                text = await resp.text()
                # 429 = busy/queued — nem hiba, a fogadó oldalon queue-zódik
                if resp.status == 429:
                    return (True, None)
                return (False, f"HTTP {resp.status}: {text[:200]}")
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")


async def mark_delivered(pool, wake_id: str):
    """Fogadó oldal: a wake feldolgozása megkezdődött — delivered-ack.

    A /api/wake-agent handler hívja a feldolgozás ELEJÉN. A watchdog így tudja,
    hogy a wake megérkezett és dolgozik.
    """
    try:
        await pool.execute("""
            UPDATE mesh.wake_log
            SET status='delivered',
                delivered_at=COALESCE(delivered_at, now()),
                first_ack_at=COALESCE(first_ack_at, now()),
                last_error=NULL,
                updated_at=now()
            WHERE wake_id = $1
        """, _uuid.UUID(wake_id))
        log.info(f"✅ Wake delivered-ack: {wake_id[:8]}")
    except Exception as e:
        log.debug(f"mark_delivered failed: {e}")


async def run_wake_watchdog(pool, node) -> int:
    """Watchdog: 5 perc 'sent' státuszú wake → retry (max MAX_ATTEMPTS).

    Determinisztikus: a node a heartbeat-tal párhuzamosan futtathatja 60s-enként.
    Returns: retry-özött wake-ek száma.
    """
    retried = 0
    try:
        rows = await pool.fetch("""
            SELECT wake_id, target_agent, target_host, prompt_hash, prompt, message_id,
                   sender, wake_type, attempts, max_attempts
            FROM mesh.wake_log
            WHERE status = 'sent'
              AND sent_at < now() - make_interval(secs => $1)
              AND attempts < max_attempts
            ORDER BY sent_at ASC
            LIMIT 10
        """, DELIVER_TIMEOUT_S)
        if not rows:
            return 0
        for row in rows:
            health_port = _get_peer_health_port(node, row["target_agent"])
            if not row["target_host"] or not health_port:
                log.warning(f"⏰ Wake watchdog: {row['target_agent']} host/port ismeretlen — retry skipped")
                continue
            # Retry a TÁROLT prompttal (korábban üresen ment → 400 a fogadónál);
            # a wake_id marad az eredeti — a fogadó ugyanazt a sort ackolja.
            ok, err = await _post_wake(
                row["target_host"], health_port,
                prompt=row["prompt"] or "",
                payload={"wake_id": str(row["wake_id"]),
                         "message_id": row["message_id"] or "",
                         "__wake_retry__": True},
                agent_name=row["target_agent"], sender=row["sender"] or "watchdog",
                message_id=row["message_id"] or "",
                mesh_secret="mesh-wake-secret-2026",
            )
            attempts = row["attempts"] + 1
            if attempts >= row["max_attempts"]:
                new_status = "timeout" if not ok else "sent"
            else:
                new_status = "sent" if ok else "failed"
            await pool.execute("""
                UPDATE mesh.wake_log
                SET attempts=$2, status=$3, last_error=$4, sent_at=now(), updated_at=now()
                WHERE wake_id = $1
            """, row["wake_id"], attempts, new_status, err)
            retried += 1
            log.info(f"⏰ Wake watchdog retry {attempts}/{row['max_attempts']} → {row['target_agent']} ({'ok' if ok else err})")
    except Exception as e:
        log.warning(f"run_wake_watchdog failed: {e}")
    return retried


def _get_peer_health_port(node, target_agent: str) -> Optional[int]:
    """Peer health_port kikeresése (topológia/fallback tábla)."""
    try:
        FALLBACK = {
            "morzsa": ("192.168.1.30", 8650),
            "runa": ("192.168.1.100", 8650),
            "nova": ("192.168.1.8", 8650),
            "tor": ("100.74.221.46", 8650),
            "mano": ("192.168.1.43", 8650),
        }
        host, port = FALLBACK.get(target_agent, (None, 8650))
        return port
    except Exception:
        return None