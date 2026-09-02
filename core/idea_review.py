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

AUTO_APPROVE_SCORE = 2   # +2 → azonnali automatikus elfogadás + megvalósítás
AUTO_REJECT_SCORE = -2    # -2 → azonnali automatikus elutasítás

# ── Idő-alapú érés (age-based promotion) ────────────────────────────────────
# Az ötlet nem ragadhat örökre egy +1-es szavazaton. A review-loop minden körben
# (6h) ellenőrzi a korokat és determinisztikus idő-szabályokat alkalmaz:
AGE_APPROVE_HOURS = 48    # 48h után score >= +1 → auto-elfogadás
AGE_REVIEW_HOURS = 48     # 48h után score == 0  → koordinátor-review dönt
AGE_REJECT_HOURS = 72     # 72h után score <= -1 → auto-elutasítás


def make_implement_fn(node, pg_pool):
    """Standard megvalósítás-logika (delegáció + in_progress) P2P/review-útvonalhoz.

    A dashboard HTTP-útvonala a saját mixin-helperét használja (_implement_idea_internal);
    ez itt a node-oldali (P2P idea_vote + coordinator review) megfelelője."""
    async def _impl(row, idea_id):
        delegation = getattr(node, "delegation", None)
        if not delegation or not pg_pool:
            return None
        import json as _j
        desc = {
            "type": "code_generation",
            "language": "python",
            "language_hint": "python",
            "description": (
                f"A2A Mesh repó implementáció. Ötlet: {row['title']}\n\n"
                f"Kontextus: {(row['description'] or '')[:3000]}\n\n"
                "A munkakönyvtár az a2a_mesh git repó. A feladat az ötlet tényleges "
                "kód-implementációja: hozz létre vagy módosíts .py fájlokat a repóban, "
                "amik az ötlet funkcionalitását megvalósítják. Generálj futtatható, "
                "önálló Python kódot, ami a repó gyökeréből futtatható."
            ),
            "idea_id": idea_id,
            "source": "otletlada_auto",
            "repo": "a2a_mesh",
            "target": "mesh",
        }
        assigned = row["assigned_to"] or ""
        task_id = await delegation.delegate_task(
            to_agent=assigned or "any",
            subject=f"[ötletláda] {row['title']}"[:500],
            description=_j.dumps(desc),
            task_type="code_generation",
            priority=7,
            available=not assigned,
        )
        await pg_pool.execute(
            "UPDATE mesh.mesh_ideas SET status = 'in_progress', updated_at = NOW() WHERE idea_id = $1",
            idea_id,
        )
        return {"task_id": str(task_id)}
    return _impl


def _notify_ready_to_build(title: str, idea_id: str) -> None:
    """Telegram-jelzés: az ötlet elérte az elfogadási küszöböt — beépítés-jóváhagyásra vár."""
    msg = (
        f"✅ ÖTLET ELFOGADVA — beépítés-jóváhagyásra vár\n\n"
        f"Ötlet: {title[:80]}\n"
        f"A szavazás/elérés küszöbét elérte, de a beépítés CSAK a te jóváhagyásoddal indul.\n"
        f"Dashboard: Ötletláda → kártya → „🔨 Beépítés jóváhagyása\" gomb."
    )
    try:
        _hermes_bin = "/Users/zsolt/.hermes/hermes-agent/venv/bin/hermes"
        import os as _os_env
        if not _os_env.path.exists(_hermes_bin):
            _hermes_bin = _os_env.popen("command -v hermes").read().strip() or "hermes"
        subprocess.Popen(
            [_hermes_bin, "send", "--telegram", OWNER_TELEGRAM, msg],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        log.info(f"📤 Beépítés-jóváhagyásra vár: {idea_id} — Zsolt értesítve")
    except Exception as e:
        log.warning(f"Telegram értesítés sikertelen: {e}")


async def apply_vote_with_rules(pg_pool, idea_id: str, voter: str, vote: str,
                                implement_fn=None) -> dict:
    """Szavazat rögzítése + determinisztikus továbbléptetési szabályok.

    Szabályok (csak 'idea' státuszúnál):
      score >= +2 → auto-elfogadás (approved — a BEÉPÍTÉS jóváhagyásra vár!)
      score <= -2 → auto-elutasítás

    A szavazat mindenki számára azonos: owner (dashboard), agentek (P2P idea_vote),
    koordinátor-review (buildable → +1, nem buildable → -1).
    A tényleges implementáció NEM automatikus: az approved ötlethez a dashboardon
    a „🔨 Beépítés jóváhagyása" gomb kell (Zsolt emberi kapuja a beépítés előtt).
    """
    if not pg_pool:
        return {"ok": False, "error": "PG unavailable"}
    row = await pg_pool.fetchrow(
        """SELECT idea_id, title, description, status, assigned_to, category, priority, voters, upvotes, downvotes
           FROM mesh.mesh_ideas WHERE idea_id = $1""",
        idea_id,
    )
    if not row:
        return {"ok": False, "error": "Idea not found"}
    voters = list(row["voters"]) if row["voters"] else []
    if voter in voters:
        return {"ok": False, "error": "Already voted", "already_voted": True,
                "upvotes": row["upvotes"], "downvotes": row["downvotes"],
                "score": row["upvotes"] - row["downvotes"]}
    voters.append(voter)
    if vote == "down":
        await pg_pool.execute(
            "UPDATE mesh.mesh_ideas SET downvotes = downvotes + 1, voters = $2, updated_at = NOW() WHERE idea_id = $1",
            idea_id, voters,
        )
    else:
        await pg_pool.execute(
            "UPDATE mesh.mesh_ideas SET upvotes = upvotes + 1, voters = $2, updated_at = NOW() WHERE idea_id = $1",
            idea_id, voters,
        )
    row2 = await pg_pool.fetchrow(
        "SELECT upvotes, downvotes, status FROM mesh.mesh_ideas WHERE idea_id = $1", idea_id)
    score = row2["upvotes"] - row2["downvotes"]
    result = {"ok": True, "upvotes": row2["upvotes"], "downvotes": row2["downvotes"],
              "score": score, "action": "none"}
    if row2["status"] == "idea":
        if score >= AUTO_APPROVE_SCORE:
            await pg_pool.execute(
                "UPDATE mesh.mesh_ideas SET status = 'approved', updated_at = NOW() WHERE idea_id = $1",
                idea_id,
            )
            result["action"] = "auto_approved"
            # ⚠️ NEM automatikus implementáció: az ötlet approved-ba kerül,
            # a beépítés a dashboard „Beépítés jóváhagyása" gombbal indul (emberi kapu).
            _notify_ready_to_build(row["title"], idea_id)
        elif score <= AUTO_REJECT_SCORE:
            await pg_pool.execute(
                "UPDATE mesh.mesh_ideas SET status = 'rejected', updated_at = NOW(), closed_at = NOW() WHERE idea_id = $1",
                idea_id,
            )
            result["action"] = "auto_rejected"
    return result


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


async def apply_age_rules(node, pg_pool) -> list:
    """Idő-alapú érés: a nyitott ötletek kora szerinti determinisztikus továbbléptetés.

    A review-loop minden körében (6h) fut, CSAK a coordinatoron:
      - 48h-nál idősebb, score >= +1  → auto-elfogadás (+ implement)
      - 48h-nál idősebb, score == 0   → LLM-review dönt (buildable → elfogadás)
      - 72h-nál idősebb, score <= -1  → auto-elutasítás

    Így a +1-es ötletek sem ragadnak be örökre: ha 2 napig senki nem ellenezte,
    az egy pozitív szavazat is elegendő az induláshoz.
    """
    if not pg_pool:
        return []
    _coord = f"coordinator:{getattr(node, 'node_name', 'coordinator')}"
    actions = []
    try:
        rows = await pg_pool.fetch(
            """SELECT idea_id, title, description, assigned_to, category, priority,
                      upvotes, downvotes, (upvotes - downvotes) AS score,
                      EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600 AS age_hours
               FROM mesh.mesh_ideas
               WHERE status = 'idea'
               ORDER BY created_at ASC LIMIT 50""",
        )
    except Exception as e:
        log.warning(f"age-rules query failed: {e}")
        return []

    impl_fn = getattr(node, "_idea_implement_fn", None)
    for r in rows:
        age = float(r["age_hours"] or 0)
        score = int(r["score"] or 0)
        idea_id = r["idea_id"]
        if age >= AGE_REJECT_HOURS and score <= -1:
            await pg_pool.execute(
                "UPDATE mesh.mesh_ideas SET status = 'rejected', updated_at = NOW(), closed_at = NOW() WHERE idea_id = $1",
                idea_id,
            )
            actions.append({"idea_id": idea_id, "action": "age_rejected", "age_h": round(age), "score": score})
            log.info(f"⏳ Age-reject: {idea_id} ({round(age)}h, score {score}) → rejected")
        elif age >= AGE_APPROVE_HOURS and score >= 1:
            await pg_pool.execute(
                "UPDATE mesh.mesh_ideas SET status = 'approved', updated_at = NOW() WHERE idea_id = $1",
                idea_id,
            )
            action = "age_approved"
            # Emberi kapu: a beépítés jóváhagyása gombbal indul, nem automatikusan
            _notify_ready_to_build(r["title"], idea_id)
            actions.append({"idea_id": idea_id, "action": action, "age_h": round(age), "score": score})
            log.info(f"⏳ Age-approve: {idea_id} ({round(age)}h, score {score}) → {action} (beépítés-jóváhagyásra vár)")
        elif age >= AGE_REVIEW_HOURS and score == 0:
            # 0 score, 48h: koordinátor LLM-review dönt — buildable → elfogadás
            try:
                verdict = await _llm_review_idea(node, r)
                await pg_pool.execute(
                    "INSERT INTO mesh.mesh_idea_comments (idea_id, author, comment) VALUES ($1, $2, $3)",
                    idea_id, "coordinator_review",
                    _format_review_comment(verdict, r) + "\n\n⏳ Idő-alapú review (48h, 0 score): a koordinátor döntött.",
                )
                if verdict.get("buildable"):
                    await pg_pool.execute(
                        "UPDATE mesh.mesh_ideas SET status = 'approved', updated_at = NOW() WHERE idea_id = $1",
                        idea_id,
                    )
                    action = "age_review_approved"
                    # Emberi kapu: beépítés jóváhagyásra vár
                    _notify_ready_to_build(r["title"], idea_id)
                    log.info(f"⏳ Age-review approve: {idea_id} (48h, score 0, buildable) → {action} (jóváhagyásra vár)")
                else:
                    await pg_pool.execute(
                        "UPDATE mesh.mesh_ideas SET status = 'rejected', updated_at = NOW(), closed_at = NOW() WHERE idea_id = $1",
                        idea_id,
                    )
                    action = "age_review_rejected"
                    log.info(f"⏳ Age-review reject: {idea_id} (48h, score 0, nem buildable)")
                actions.append({"idea_id": idea_id, "action": action, "age_h": round(age), "score": score})
            except Exception as e:
                log.warning(f"Age-review failed for {idea_id}: {e}")
    return actions


async def review_ideas_with_llm(node, pg_pool) -> list:
    """A coordinator átnézi a még nem review-olt ötleteket.

    Minden ötlethez:
      1. LLM-elemzés: implementálhatóság, kockázat, erőforrás
      2. Eredmény → idea comment (coordinator_review)
      3. Ha buildable → Telegram jelzés a tulajdonosnak
    """
    if not pg_pool:
        return []
    # Nem-zárt ötletek, amikre a koordinátor MÉG NEM SZAVAZOTT
    # (voters-alapú kizárás: a régi komment-alapú kizárás kihagyta volna azokat,
    # amikre a c46a514 ELŐTT futt review nem adott szavazatot)
    try:
        rows = await pg_pool.fetch(
            """SELECT idea_id, title, description, category, priority, submitted_by, upvotes, downvotes, voters
               FROM mesh.mesh_ideas
               WHERE status = 'idea'
                 AND NOT (%s = ANY(voters))
               ORDER BY upvotes DESC, created_at ASC LIMIT 10""",
            f"coordinator:{getattr(node, 'node_name', 'coordinator')}",
        )
    except Exception as e:
        log.warning(f"idea review query failed: {e}")
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
            # A review szavazat is: buildable → +1, nem buildable → -1.
            # A koordinátor szavazata a +2 küszöbbe számít bele — így a
            # "koordinátor szerint jó + owner/agent jóváhagyása" = azonnali indul.
            try:
                impl = getattr(node, "_idea_implement_fn", None)
                vote_result = await apply_vote_with_rules(
                    pg_pool, r["idea_id"], f"coordinator:{getattr(node, 'node_name', 'coordinator')}",
                    "up" if verdict.get("buildable") else "down",
                    implement_fn=impl,
                )
                log.info(f"🛡️ Review-vote {r['idea_id']}: {vote_result.get('action')} (score {vote_result.get('score')})")
                if verdict.get("buildable"):
                    _notify_owner(r, verdict)
            except Exception as vote_err:
                log.warning(f"Review-vote failed for {r['idea_id']}: {vote_err}")
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
        _hermes_bin = "/Users/zsolt/.hermes/hermes-agent/venv/bin/hermes"
        import os as _os_env
        if not _os_env.path.exists(_hermes_bin):
            _hermes_bin = _os_env.popen("command -v hermes").read().strip() or "hermes"
        subprocess.Popen(
            [_hermes_bin, "send", "--telegram", OWNER_TELEGRAM, msg],
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
                # 1. Idő-alapú érés: régi ötletek determinisztikus továbbléptetése
                age_actions = await apply_age_rules(node, pg_pool)
                if age_actions:
                    log.info(f"⏳ Age-rules: {len(age_actions)} ötlet idő-szabály alapján továbblépve")
                # 2. LLM-review az újaknak
                results = await review_ideas_with_llm(node, pg_pool)
                if results:
                    log.info(f"🛡️ Idea review kör: {len(results)} ötlet felülvizsgálva")
            except Exception as e:
                log.warning(f"idea review loop error: {e}")
            await asyncio.sleep(REVIEW_INTERVAL)

    task = asyncio.create_task(_loop())
    log.info("🛡️ Coordinator idea-review loop elindult (interval: 6h)")
    return task