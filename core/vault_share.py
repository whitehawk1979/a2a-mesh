"""Vault Share — per-agent vault hozzáférés + titkos megosztás a mesh P2P hálózaton.

Architektúra (Zsolt kérés, 2026-09-02):
  - Minden agent saját vaultot birtokol (core/vault.py: keyring → encrypted file → env).
  - A dashboard (owner bejelentkezéssel) BArmelyik node vaultját listázhatja,
    kezelheti, és tételt másolhat node-ok közt (share).
  - Agentek egymás közt is kérhetnek titkot: vault_request/vault_response P2P
    üzenetek, mTLS-csatornán (a mesh összes P2P forgalma TLS 1.3).

P2P protokoll (deterministic, no-LLM):
  vault_request:   {action: list|get|ping, name?: str}
  vault_response:  {request_id, ok, action, entries?/value?/vault_status?}

Biztonság:
  - A titok csak a kérő node memóriájában létezik, soha nem íródik le automatikusan.
  - A megosztás (share) a dashboard-on keresztül explicit owner-jóváhagyású
    folyamat: az owner kijelöli a tételt és a cél node-okat.
  - P2P csatorna mTLS + HMAC (mesh standard), így node→node titkosküldés
    nem hagyja a mesh-t.

UI (dashboard.js Vault oldal):
  - Agent-váltó: [Helyi] [morzsa] [runa] [tor] — a /api/vault/remote/{node} API-n
    keresztül a kijelölt node vault listája töltődik be.
  - Minden tételnél „Megosztás →" gomb: cél node választás → vault_share API.
"""

import os
import json
import time
import uuid
import asyncio
import logging
from typing import Optional, Dict, List

from . import vault as vault_mod

log = logging.getLogger("vault_share")

# ── Request/response registry (per node, in-memory) ────────────────────────

_pending: Dict[str, asyncio.Future] = {}
_TIMEOUT = 25.0  # seconds — P2P válaszvárás; ssh_tunnel fallback útvonal lassabb lehet restart után


def _new_request_id() -> str:
    return uuid.uuid4().hex[:12]


async def request_from_peer(router, peer: str, action: str, name: Optional[str] = None,
                            timeout: float = _TIMEOUT) -> Optional[dict]:
    """Send vault_request to a peer and await vault_response.

    Returns the response payload dict, or None on timeout/transport failure.
    """
    from ..node import A2AMessage  # late import: avoid circulars

    rid = _new_request_id()
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    _pending[rid] = fut
    payload = {"request_id": rid, "action": action}
    if name:
        payload["name"] = name

    msg = A2AMessage(
        sender=getattr(router, "node_name", "?"),
        recipient=peer,
        type="vault_request",
        payload=payload,
    )
    try:
        await router.send(msg)
    except Exception as e:
        _pending.pop(rid, None)
        log.warning(f"vault_request send failed to {peer}: {e}")
        return None

    try:
        return await asyncio.wait_for(fut, timeout)
    except asyncio.TimeoutError:
        log.warning(f"vault_request timeout from {peer} (action={action})")
        return None
    finally:
        _pending.pop(rid, None)


def handle_vault_request(payload: dict) -> dict:
    """Build a vault_response payload for an incoming vault_request.

    NEVER sends secrets anywhere by itself — response goes back to the
    authenticated mTLS peer that asked.
    """
    action = (payload or {}).get("action", "")
    rid = (payload or {}).get("request_id", "")

    if action == "list":
        entries = vault_mod.list_secrets()
        return {"request_id": rid, "ok": True, "action": "list", "entries": entries}
    if action == "status":
        return {"request_id": rid, "ok": True, "action": "status", "vault_status": vault_mod.get_vault_status()}
    if action == "delete":
        name = (payload or {}).get("name", "")
        if not name:
            return {"request_id": rid, "ok": False, "error": "name required"}
        deleted = vault_mod.delete_secret(name)
        return {"request_id": rid, "ok": bool(deleted), "action": "delete",
                "name": name, "deleted": bool(deleted)}
    if action == "get":
        name = (payload or {}).get("name", "")
        if not name:
            return {"request_id": rid, "ok": False, "error": "name required"}
        value = vault_mod.get_secret(name)
        if value is None:
            return {"request_id": rid, "ok": False, "error": "not found"}
        return {"request_id": rid, "ok": True, "action": "get", "name": name, "value": value}
    return {"request_id": rid, "ok": False, "error": f"unknown action: {action}"}


def handle_vault_response(payload: dict) -> None:
    """Resolve a pending request future with the incoming vault_response."""
    rid = (payload or {}).get("request_id", "")
    fut = _pending.get(rid)
    if fut and not fut.done():
        fut.set_result(payload)
    else:
        log.debug(f"vault_response for unknown/expired request {rid}")


# ── Share (owner-initiated, dashboard-driven) ──────────────────────────────

async def share_to_peer(router, peer: str, name: str, include_secret: bool = False, timeout: float = _TIMEOUT) -> dict:
    """Owner-driven share: fetch entry from LOCAL vault, push to peer vault.

    The peer receives vault_request-style payload via vault_share msg type
    with action='put' — the peer stores it into its own vault (encrypted).
    """
    from ..node import A2AMessage

    # Get the secret from local vault
    value = vault_mod.get_secret(name)
    if value is None:
        return {"ok": False, "error": f"lokális vaultban nincs '{name}'"}

    rid = _new_request_id()
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    _pending[rid] = fut

    msg = A2AMessage(
        sender=getattr(router, "node_name", "?"),
        recipient=peer,
        type="vault_share",
        payload={"request_id": rid, "action": "put", "name": name, "value": value},
    )
    try:
        await router.send(msg)
    except Exception as e:
        _pending.pop(rid, None)
        return {"ok": False, "error": f"send failed: {e}"}

    try:
        resp = await asyncio.wait_for(fut, timeout)
        return resp if isinstance(resp, dict) else {"ok": False, "error": "invalid response"}
    except asyncio.TimeoutError:
        return {"ok": False, "error": f"timeout — {peer} nem válaszolt"}
    finally:
        _pending.pop(rid, None)


def handle_vault_share(payload: dict) -> dict:
    """Incoming vault_share (action=put): store the secret into LOCAL vault."""
    action = (payload or {}).get("action", "")
    rid = (payload or {}).get("request_id", "")
    if action != "put":
        return {"request_id": rid, "ok": False, "error": f"unknown action: {action}"}
    name = (payload or {}).get("name", "")
    value = (payload or {}).get("value", "")
    if not name or not value:
        return {"request_id": rid, "ok": False, "error": "name and value required"}
    ok = vault_mod.set_secret(name, value)
    return {"request_id": rid, "ok": bool(ok), "stored": name}


async def store_to_peer(router, peer: str, name: str, value: str, timeout: float = _TIMEOUT) -> dict:
    """Dashboard-driven direct store: push a secret directly to a peer vault."""
    from ..node import A2AMessage

    rid = _new_request_id()
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    _pending[rid] = fut

    msg = A2AMessage(
        sender=getattr(router, "node_name", "?"),
        recipient=peer,
        type="vault_share",
        payload={"request_id": rid, "action": "put", "name": name, "value": value},
    )
    try:
        await router.send(msg)
    except Exception as e:
        _pending.pop(rid, None)
        log.warning(f"vault_share (put) send failed to {peer}: {e}")
        return {"ok": False, "error": f"send failed: {e}"}

    try:
        resp = await asyncio.wait_for(fut, timeout)
        return resp if isinstance(resp, dict) else {"ok": False, "error": "invalid response"}
    except asyncio.TimeoutError:
        log.warning(f"vault_share (put) timeout to {peer}")
        return {"ok": False, "error": f"timeout — {peer} nem válaszolt"}
    finally:
        _pending.pop(rid, None)


# ── Dashboard helpers (local + remote aggregation) ─────────────────────────

async def get_remote_vault_list(router, peers: List[str], timeout: float = _TIMEOUT) -> dict:
    """Fetch vault entry lists from all peers in parallel. Returns {peer: entries|error}."""
    results: Dict[str, dict] = {}
    tasks = []
    for p in peers:
        tasks.append(request_from_peer(router, p, "list", timeout=timeout))
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    for p, r in zip(peers, responses):
        if isinstance(r, Exception) or r is None:
            results[p] = {"error": "unreachable/timeout"}
        elif r.get("ok"):
            results[p] = {"entries": r.get("entries", [])}
        else:
            results[p] = {"error": r.get("error", "unknown")}
    return results


async def get_remote_vault_status(router, peers: List[str], timeout: float = _TIMEOUT) -> dict:
    """Fetch vault status from all peers in parallel."""
    results: Dict[str, dict] = {}
    tasks = [request_from_peer(router, p, "status", timeout=timeout) for p in peers]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    for p, r in zip(peers, responses):
        if isinstance(r, Exception) or r is None:
            results[p] = {"error": "unreachable/timeout"}
        elif r.get("ok"):
            results[p] = {"vault_status": r.get("vault_status", {})}
        else:
            results[p] = {"error": r.get("error", "unknown")}
    return results


async def get_remote_secret(router, peer: str, name: str, timeout: float = _TIMEOUT) -> dict:
    """Owner dashboard: fetch a single secret from a peer vault."""
    return await request_from_peer(router, peer, "get", name=name, timeout=timeout) or {"ok": False, "error": "timeout"}