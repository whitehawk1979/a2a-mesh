"""Ötletláda Coordinator Review — az elfogadott ötletek beépítési felülvizsgálata.

Folyamat (Zsolt kérés, 2026-09-02):
  1. Agentek (bármelyik node) P2P-n ötletet küldenek: msg_type='idea_submit'
  2. A node beírja a mesh.mesh_ideas táblába (status='idea', source_type='agent')
  3. A COORDINATOR (nova, short_addr=0x1E54... valójában role=router legalacsonyabb addr)
     hetente/naponta átnézi a beérkezett ötleteket:
       - LLM-mel értékeli: implementálható-e, kockázatok, erőforrás-igény
       - ha BEÉPÍTHETŐ: Telegram-üzenetet küld a tulajdonosnak (Zsolt),
         hogy "jelezzen: be lehet-e építeni"
       - ha NEM: reasoning-vel együtt rejected/idea marad
  4. A review-eredmény az ötlet comments-ébe kerül (source_type='coordinator_review')

A review NEM dönt — a tulajdonos dönt. A coordinator csak felülvizsgál + jelez.
"""

import asyncio
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("a2a_mesh.idea_review")

OWNER_TELEGRAM = "7796035659"  # Zsolt — tulajdonos
REVIEW_INTERVAL = 6 * 3600  # 6 óránként review-kör
MIN_VOTES_FOR_REVIEW = 0  # minden ötletet megnéz, ami legalább ennyi szavazatot kapott


# ── P2P: agent → ötletláda ─────────────────────────────────────────────────

def parse_idea_submit(payload: dict) -> Optional[dict]:
    """Bejövő idea_submit üzenet validálása és normalizálása."""
    if not isinstance(payload, dict):
        return None
    title = (payload.get("title") or "").strip()
    if not title:
        return None
    return {
        "title": title[:300],
        "description": (payload.get("description") or "").strip()[:4000],
        "category": (payload.get("category") or "agent").strip()[:50],
        "priority": (payload.get("priority") or "medium").strip()[:20],
        "submitted_by": (payload.get("submitted_by") or "agent").strip()[:100],
        "tags": payload.get("tags") if isinstance(payload.get("tags"), list) else [],
    }


async def store_agent_idea(pg_pool, idea: dict) -> Optional[str]:
    """Ötlet mentése a mesh.mesh_ideas táblába. Visszaadja az idea_id-t."""
    import uuid
    if not pg_pool:
        return None
    idea_id = "idea_" + uuid.uuid4().hex[:12]
    try:
        await pg_pool.execute(
            """INSERT INTO mesh.mesh_ideas
               (idea_id, title, description, category, priority, source_type, submitted_by, tags)
               VALUES ($1, $2, $3, $4, $5, 'agent', $6, $7::text[])""",
            idea_id, idea["title"], idea["description"], idea["category"],
            idea["priority"], idea["submitted_by"], idea["tags"],
        )
        log.info(f"💡 Agent-ötlet elmentve: {idea['title'][:60]} (from {idea['submitted_by']})")
        return idea_id
    except Exception as e:
        log.warning(f"Agent-ötlet mentése sikertelen: {e}")
        return None


# ── Coordinator review ────────────────────────────────────────────────────

def _is_coordinator(node) -> bool:
    """Ez a node-e a mesh coordinator? (election modul szerint)"""
    try:
        el = getattr(node, "election", None)
        if el and hasattr(el, "get_status"):
            st = el.get_status()
            coord = (st.get("coordinator") or {}).get("node_name")
            return bool(coord and coord == getattr(node, "node_name", ""))
    except Exception:
        pass
    return False


async def review_ideas_with_llm(node, pg_pool) -> list:
    """A coordinator átnézi a még nem review-olt ötleteket.

    Minden ötlethez:
      1. LLM-elemzés: implementálhatóság, kockázat, erőforrás
      2. Eredmény → idea comment (coordinator_review)
      3. Ha buildable → Telegram jelzés a tulajdonosnak
    """
    if not pg_pool:
        return []
    # Még nem review-olt, nem-zárt ötletek
    try:
        rows = await pg_pool.fetch(
            """SELECT idea_id, title, description, category, priority, submitted_by, upvotes, downvotes
               FROM mesh.mesh_ideas
               WHERE status = 'idea'
                 AND idea_id NOT IN (
                   SELECT idea_id FROM mesh.mesh_idea_comments
                   WHERE author = 'coordinator_review'
                 )
               ORDER BY upvotes DESC, created_at ASC LIMIT 10"""
        )
    except Exception as e:
        # A comment-tábla esetleg nem létezik — létrehozzuk
        try:
            await pg_pool.execute(
                "CREATE TABLE IF NOT EXISTS mesh.mesh_idea_comments ("
                "id SERIAL PRIMARY KEY, idea_id VARCHAR(64) NOT NULL, "
                "author VARCHAR(100) NOT NULL, comment TEXT NOT NULL, "
                "created_at TIMESTAMPTZ DEFAULT NOW())"
            )
            rows = await pg_pool.fetch(
                """SELECT idea_id, title, description, category, priority, submitted_by, upvotes, downvotes
                   FROM mesh.mesh_ideas WHERE status = 'idea'
                   ORDER BY upvotes DESC, created_at ASC LIMIT 10"""
            )
        except Exception as e2:
            log.warning(f"idea review query failed: {e2}")
            return []

    results = []
    for r in rows:
        try:
            verdict = await _llm_review_idea(node, r)
            comment_text = _format_review_comment(verdict, r)
            await pg_pool.execute(
                "INSERT INTO mesh.mesh_idea_comments (idea_id, author, comment) VALUES ($1, $2, $3)",
                r["idea_id"], "coordinator_review", comment_text,
            )
            results.append({"idea_id": r["idea_id"], "verdict": verdict})
            # Buildable → tulajdonos értesítése Telegramon
            if verdict.get("buildable"):
                _notify_owner(r, verdict)
        except Exception as e:
            log.warning(f"Idea review failed for {r.get('idea_id')}: {e}")
    return results


async def _llm_review_idea(node, idea_row) -> dict:
    """LLM-alapú implementálhatósági elemzés egy ötlethez.

    A mesh saját LLM-hívását használja (_task_llm_research), ami a node
    PRIMER/gemma4/qwen2.5 útvonalon fut.
    """
    prompt = f"""Te egy AI-agent-mesh (A2A Mesh) koordinátora vagy. Felülvizsgáld az alábbi ötletet, hogy BEÉPÍTHETŐ-E a mesh rendszerbe.

Ötlet: {idea_row['title']}
Leírás: {idea_row['description'] or '(nincs leírás)'}
Kategória: {idea_row['category']} | Prioritás: {idea_row['priority']}
Beküldte: {idea_row['submitted_by']}

Értékeld:
1. buildable: implementálható-e a mesh-ben (true/false)
2. effort: becslés (kicsi/közepes/nagy)
3. risk: kockázat (alacsony/közepes/magas) — mi romolhat el
4. deps: mire épül (mely meglévő rendszerekhez nyúl hozzá)
5. summary: 1-2 mondat értékelés

Válaszolj SZIGORÚAN ebben a JSON formában:
{{"buildable": true/false, "effort": "...", "risk": "...", "deps": "...", "summary": "..."}}"""

    now = datetime.now(timezone.utc)
    result = None
    try:
        result = await node._task_llm_research(
            node.node_name, now,
            "Ötlet-review",
            prompt,
        )
    except Exception as e:
        log.warning(f"LLM review hívás sikertelen: {e}")

    raw = ""
    if isinstance(result, dict):
        raw = str(result.get("result", "") or result.get("output", "") or "")
    # JSON kinyerése a válaszból
    try:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            verdict = json.loads(raw[start:end + 1])
            if isinstance(verdict, dict) and "buildable" in verdict:
                return verdict
    except Exception:
        pass
    # Fallback: konzervatív — buildable, de "kézi átnézendő"
    return {
        "buildable": True,
        "effort": "ismeretlen",
        "risk": "közepes (LLM review nem elérhető — kézi átnézés javasolt)",
        "deps": "—",
        "summary": "LLM review nem sikerült; kézi felülvizsgálat javasolt.",
    }


def _format_review_comment(verdict: dict, idea_row) -> str:
    buildable = "✅ BEÉPÍTHETŐ" if verdict.get("buildable") else "❌ Nem ajánlott beépíteni"
    return (
        f"🛡️ Koordinátor-review ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC)\n"
        f"{buildable}\n"
        f"Becsült munka: {verdict.get('effort', '?')}\n"
        f"Kockázat: {verdict.get('risk', '?')}\n"
        f"Függőségek: {verdict.get('deps', '?')}\n"
        f"Összegzés: {verdict.get('summary', '?')}"
    )


def _notify_owner(idea_row, verdict: dict) -> None:
    """Telegram-üzenet a tulajdonosnak (Zsolt): az ötlet beépíthető, döntésre vár."""
    msg = (
        f"💡 ÖTLET-BEÉPÍTÉS JELZÉS\n"
        f"Ötlet: {idea_row['title'][:80]}\n"
        f"Koordinátor-review: ✅ beépíthető\n"
        f"Munka: {verdict.get('effort', '?')} | Kockázat: {verdict.get('risk', '?')}\n"
        f"Összegzés: {str(verdict.get('summary', ''))[:200]}\n"
        f"Beküldte: {idea_row['submitted_by']}\n"
        f"Dashboard: Ötletláda → Elfogadás gomb a döntéshez."
    )
    try:
        subprocess.Popen(
            ["hermes", "send", "--telegram", OWNER_TELEGRAM, msg],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        log.info(f"📤 Tulajdonos értesítve: buildable idea {idea_row['idea_id']}")
    except Exception as e:
        log.warning(f"Telegram értesítés sikertelen: {e}")


# ── Review loop indítása a coordinatoron ───────────────────────────────────

async def start_review_loop(node) -> Optional[asyncio.Task]:
    """Háttér-loop: REVIEW_INTERVAL-enként review-kör, CSAK a coordinatoron."""
    if not _is_coordinator(node):
        log.debug("idea_review: nem coordinator — loop nem indul")
        return None

    async def _loop():
        # induláskor 2 perc várakozás (node stabilizálás)
        await asyncio.sleep(120)
        while True:
            try:
                if not _is_coordinator(node):
                    await asyncio.sleep(REVIEW_INTERVAL)
                    continue
                pg_pool = getattr(node, "_pg_pool", None)
                results = await review_ideas_with_llm(node, pg_pool)
                if results:
                    log.info(f"🛡️ Idea review kör: {len(results)} ötlet felülvizsgálva")
            except Exception as e:
                log.warning(f"idea review loop error: {e}")
            await asyncio.sleep(REVIEW_INTERVAL)

    task = asyncio.create_task(_loop())
    log.info("🛡️ Coordinator idea-review loop elindult (interval: 6h)")
    return task