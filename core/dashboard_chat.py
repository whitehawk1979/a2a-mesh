"""A2A Mesh Chat — Per-user DM system for dashboard ↔ agent communication.

Each dashboard user gets a personal chat identity. Messages are stored in PG
(mesh.mesh_chat_messages) and routed to agents via the mesh. Agent replies are
stored as DMs back to the user.
"""
import uuid
import json
import logging

log = logging.getLogger("mesh.chat")


async def _ensure_chat_user(pool, username, display_name, node_name):
    """Create or update a chat user in PG."""
    try:
        await pool.execute(
            """INSERT INTO mesh.mesh_chat_users (username, display_name, node_name, last_seen)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (username)
               DO UPDATE SET last_seen = NOW(), display_name = $2, node_name = $3""",
            username, display_name or username, node_name
        )
    except Exception as e:
        log.warning(f"Failed to ensure chat user: {e}")



# ═══════════════════════════════════════════════════════════════════════
# ── Telegram-style chat commands (/help, /status, /debate, /ask, /all) ──
# Intercepted in handle_chat_send BEFORE mesh routing. Command replies are
# stored as agent_reply-style system messages so they render in the chat.
# ═══════════════════════════════════════════════════════════════════════

_CHAT_COMMANDS = {
    "help": "Elérhető parancsok listája",
    "status": "Mesh és agent állapot riport",
    "debate": "Vita indítása: /debate <téma> — minden agent kifejti álláspontját",
    "ask": "Célzott kérés: /ask <agent> <kérdés> — csak az adott agent válaszol",
    "all": "Közös elemzés: /all <kérdés> — minden agent válaszol ugyanarra",
    "ideas": "Ötletgyűjtés: /ideas <téma> — minden agent javaslatot ad, [ÖTLET]-jelölve → Ötletláda",
    "vote": "Agent-szavazás: /vote [idea_id] — minden agent leadja szavazatát az ötletládában nyitott ötletekre",
    "delegate": "Delegálás: /delegate <agent|any> <tárgy> [--prio N] [--type T] [--timeout M] [--desc L] [--fanout N] [--dist] [--eligible a,b] [--depends ID] — task + Kanban kártya, eredmény visszajön a chatbe",
    "tasks": "Task lista: /tasks [nyitott|completed|failed|all] [agent] — delegációk állapota",
    "task": "Task részletek: /task <task_id> — státusz, eredmény, időpontok",
    "reassign": "Task átirányítás: /reassign <task_id> <agent> — meglévő feladat másik agentnek",
    "cancel": "Task törlés: /cancel <task_id> — meglévő delegáció érvénytelenítése",
    "clear": "Chat üzenetek törlése ebben a szobában (csak saját üzenetek)",
}


async def _process_chat_command(node, pool, username, display_name, recipient, command_text):
    """Process a / command. Returns (response_dict, True) if handled, else (None, False).
    Command messages ARE stored in PG (so the user sees what they typed), and the
    command's output is stored as a system agent_reply."""
    parts = command_text.strip().split(maxsplit=1)
    cmd = parts[0][1:].lower() if parts and parts[0].startswith("/") else ""
    args = parts[1] if len(parts) > 1 else ""

    if cmd not in _CHAT_COMMANDS:
        return None, False  # Not a known command — treat as normal message

    from aiohttp import web
    out_content = None

    if cmd == "help":
        out_content = "🤖 **Chat parancsok:**\n"
        for c, desc in _CHAT_COMMANDS.items():
            out_content += f"• `/{c}` — {desc}\n"
        out_content += "\nA parancsokat a chatbe írva használhatod (közös szoba vagy DM)."

    elif cmd == "status":
        try:
            peers = getattr(node, 'peer_discovery', None)
            rows = await pool.fetch(
                "SELECT node_name, role, status, version FROM mesh.mesh_nodes ORDER BY node_name"
            )
            out_content = "📊 **Mesh állapot:**\n"
            for r in rows:
                out_content += f"• {r['node_name']}: {r['role']} — {r['status']} (v{r['version']})\n"
        except Exception as e:
            out_content = f"⚠️ Státusz hiba: {e}"

    elif cmd == "clear":
        try:
            if recipient == "broadcast":
                await pool.execute(
                    "DELETE FROM mesh.mesh_chat_messages WHERE recipient = 'broadcast' AND username = $1",
                    username,
                )
            else:
                await pool.execute(
                    "DELETE FROM mesh.mesh_chat_messages WHERE username = $1 AND (sender = $2 OR recipient = $2)",
                    username, recipient,
                )
            out_content = "🧹 Chat törölve ebben a szobában."
        except Exception as e:
            out_content = f"⚠️ Törlés hiba: {e}"

    elif cmd == "delegate":
        # /delegate <agent|any> <tárgy> [--prio N] [--type T] [--timeout M] [--desc L]
        #          [--fanout N] [--dist] [--eligible a,b] [--depends <task_id>]
        # Deterministic delegation via DelegationManager (PG INSERT + Kanban card).
        # v0.46.11: user-kötés (context.chat_username) + fanout/dist/eligible/depends.
        import re as _re_dl, shlex as _shlex_dl
        args = (args or "").strip()
        if not args:
            out_content = ("⚠️ Használat: `/delegate <agent|any|auto> <tárgy> [opciók]`\n"
                           "• `/delegate morzsa Elemzés a hőmérséklet-logokról`\n"
                           "• `/delegate any Riport a mesh topológiáról --prio 8`\n"
                           "• `/delegate auto Gyors összegzés --timeout 20` — ⚖️ legkevésbé terhelt node kapja\n"
                           "• `/delegate any Adat-gyűjtés --fanout 3 --dist` (párhuzamos, 3 agent)\n"
                           "• `/delegate any Audit --eligible nova,runa --timeout 240`\n"
                           "• `/delegate tor Utóellenőrzés --depends <task_id>` (függőségi lánc)\n"
                           "Opciók: `--prio 1-9` (default 5) • `--type <típus>` • `--timeout <perc>` • "
                           "`--desc <leírás>` • `--fanout N` (verseny/párhuzamos) • `--dist` (mindenkinek más) • "
                           "`--eligible a,b` (csak ők claimelhetik) • `--depends <task_id>`")
        else:
            try:
                tokens = _shlex_dl.split(args)
            except ValueError:
                tokens = args.split()
            to_agent = tokens[0].strip().lstrip("@").lower() if tokens else ""
            subject_parts: list = []
            prio, task_type, timeout_m, desc = 5, "generic", 30, ""
            fanout, dist_mode, eligible, depends_on = 0, False, None, None
            i = 1
            while i < len(tokens):
                t = tokens[i]
                if t == "--prio" and i + 1 < len(tokens):
                    try: prio = max(1, min(9, int(tokens[i+1])))
                    except ValueError: pass
                    i += 2
                elif t == "--type" and i + 1 < len(tokens):
                    task_type = tokens[i+1][:40]; i += 2
                elif t == "--timeout" and i + 1 < len(tokens):
                    try: timeout_m = max(1, min(1440, int(tokens[i+1])))
                    except ValueError: pass
                    i += 2
                elif t == "--desc" and i + 1 < len(tokens):
                    desc = tokens[i+1][:2000]; i += 2
                elif t == "--fanout" and i + 1 < len(tokens):
                    try: fanout = max(0, min(10, int(tokens[i+1])))
                    except ValueError: pass
                    i += 2
                elif t == "--dist" :
                    dist_mode = True; i += 1
                elif t == "--eligible" and i + 1 < len(tokens):
                    eligible = [a.strip().lstrip("@").lower() for a in tokens[i+1].split(",") if a.strip()][:8]
                    i += 2
                elif t == "--depends" and i + 1 < len(tokens):
                    depends_on = tokens[i+1].strip(); i += 2
                else:
                    subject_parts.append(t); i += 1
            subject = " ".join(subject_parts).strip()
            if not to_agent or not subject:
                out_content = "⚠️ Használat: `/delegate <agent|any|auto> <tárgy>` — pl. `/delegate morzsa Logok elemzése`"
            elif depends_on and len(depends_on) < 8:
                out_content = "⚠️ `--depends` hibás task_id — teljes (36 karakteres) task_id-t adj meg"
            else:
                try:
                    _dl = getattr(node, "delegation", None)
                    if _dl is None and getattr(node, "pg_pool", None) is None and getattr(node, "_pg_pool", None) is None:
                        raise RuntimeError("PG pool nem elérhető")
                    if _dl is None:
                        from core.delegation import DelegationManager
                        _dl = DelegationManager(node.node_name, getattr(node, "pg_pool", None) or getattr(node, "_pg_pool", None))
                        node.delegation = _dl
                    available = (to_agent == "any")
                    # v0.46.11: user-kötés — a task a chat-felhasználóhoz kötődik,
                    # így az eredmény visszajut a közös szobába (lásd _on_delegation_result).
                    _ctx = {"chat_username": username, "origin_node": node.node_name,
                            "origin_recipient": recipient or "broadcast"}
                    task_id = await _dl.delegate_task(
                        to_agent=(to_agent if not available else "any"),
                        subject=subject,
                        description=desc or subject,
                        task_type=task_type,
                        priority=prio,
                        timeout_minutes=timeout_m,
                        available=available,
                        fan_out=fanout,
                        distribute_mode=dist_mode,
                        eligible_agents=eligible if available else None,
                        depends_on=depends_on,
                        context=_ctx,
                    )
                    _ids = task_id if isinstance(task_id, list) else [task_id]
                    _mode = ""
                    if fanout > 0:
                        _mode = f" | Fan-out: {fanout}×" + (" (distribute)" if dist_mode else " (verseny)")
                    if eligible:
                        _mode += f" | Eligible: {','.join(eligible)}"
                    if depends_on:
                        _mode += f" | Depends: {depends_on[:12]}…"
                    out_content = (f"✅ **Delegálva** → `{to_agent}`\n"
                                   f"• Tárgy: {subject[:120]}\n"
                                   f"• task_id: `{_ids[0][:18]}…`\n"
                                   f"• Prioritás: P{prio} | Típus: {task_type} | Timeout: {timeout_m} perc{_mode}\n"
                                   f"• Az eredmény automatikusan megérkezik ide: {username} felhasználónak")
                except Exception as _dl_e:
                    out_content = f"❌ Delegálás sikertelen: {_dl_e}"

    elif cmd == "tasks":
        # /tasks [nyitott|completed|failed|all] [agent] — delegációk listája
        _parts = (args or "").strip().split()
        _filter = (_parts[0].lower() if _parts else "nyitott")
        _agent = (_parts[1].strip().lstrip("@").lower() if len(_parts) > 1 else None)
        _status_map = {"nyitott": ("pending", "available", "accepted", "running"),
                       "completed": ("completed",), "failed": ("failed", "expired", "cancelled"), "all": None}
        _statuses = _status_map.get(_filter, None)
        if _statuses is None and _filter != "all":
            out_content = "⚠️ Használat: `/tasks [nyitott|completed|failed|all] [agent]`"
        else:
            try:
                if _statuses is None:
                    if _agent:
                        rows = await pool.fetch(
                            "SELECT task_id, to_agent, assigned_agent, status, priority, subject, created_at FROM shared_delegations WHERE (to_agent=$1 OR assigned_agent=$1) ORDER BY created_at DESC LIMIT 20", _agent)
                    else:
                        rows = await pool.fetch(
                            "SELECT task_id, to_agent, assigned_agent, status, priority, subject, created_at FROM shared_delegations ORDER BY created_at DESC LIMIT 20")
                else:
                    if _agent:
                        rows = await pool.fetch(
                            "SELECT task_id, to_agent, assigned_agent, status, priority, subject, created_at FROM shared_delegations WHERE status = ANY($1) AND (to_agent=$2 OR assigned_agent=$2) ORDER BY created_at DESC LIMIT 20", list(_statuses), _agent)
                    else:
                        rows = await pool.fetch(
                            "SELECT task_id, to_agent, assigned_agent, status, priority, subject, created_at FROM shared_delegations WHERE status = ANY($1) ORDER BY priority DESC, created_at DESC LIMIT 20", list(_statuses))
                if not rows:
                    out_content = f"📭 Nincs task a szűrésben ({_filter}{', ' + _agent if _agent else ''})."
                else:
                    _icon = {"pending": "🕒", "available": "🟢", "accepted": "🤝", "running": "🔄", "completed": "✅", "failed": "❌", "expired": "⏰", "cancelled": "🚫"}
                    out_content = f"📋 **Taskok ({_filter}{', ' + _agent if _agent else ''})** — {len(rows)} db:\n"
                    for r in rows:
                        _who = r['assigned_agent'] or r['to_agent']
                        out_content += f"{_icon.get(r['status'], '•')} `{str(r['task_id'])[:8]}…` P{r['priority']} {_who}: {(r['subject'] or '')[:60]}\n"
            except Exception as _t_e:
                out_content = f"❌ Task lista hiba: {_t_e}"

    elif cmd == "task":
        # /task <task_id> — részletek
        _tid = (args or "").strip()
        if not _tid:
            out_content = "⚠️ Használat: `/task <task_id>` — task_id-t a `/tasks` listából"
        else:
            try:
                if len(_tid) < 36:
                    _rows = await pool.fetch("SELECT * FROM shared_delegations WHERE task_id::text LIKE $1 ORDER BY created_at DESC LIMIT 3", f"%{_tid}%")
                else:
                    _rows = await pool.fetch("SELECT * FROM shared_delegations WHERE task_id::text = $1", _tid)
                if not _rows:
                    out_content = f"❌ Task nem található: `{_tid[:20]}`"
                else:
                    r = _rows[0]
                    _icon = {"pending": "🕒", "available": "🟢", "accepted": "🤝", "running": "🔄", "completed": "✅", "failed": "❌", "expired": "⏰", "cancelled": "🚫"}
                    out_content = (f"{_icon.get(r['status'], '•')} **Task** `{str(r['task_id'])[:12]}…`\n"
                                   f"• Tárgy: {(r['subject'] or '')[:150]}\n"
                                   f"• Állapot: {r['status']} | P{r['priority']} | {r['task_type'] or 'generic'}\n"
                                   f"• {r['from_agent']} → {r['assigned_agent'] or r['to_agent']}\n"
                                   f"• Létrehozva: {str(r['created_at'])[:19]}")
                    if r.get('progress'):
                        out_content += f"\n• Progress: {r['progress']}%"
                    if r.get('notes'):
                        _notes = (r['notes'] or '')[-400:]
                        out_content += f"\n• Megjegyzések: {_notes}"
                    if r.get('result'):
                        out_content += f"\n• **Eredmény:** {(r['result'] or '')[:600]}"
            except Exception as _tk_e:
                out_content = f"❌ Task részletek hiba: {_tk_e}"

    elif cmd == "reassign":
        # /reassign <task_id> <agent> — meglévő task átirányítása
        _parts = (args or "").strip().split()
        if len(_parts) < 2:
            out_content = "⚠️ Használat: `/reassign <task_id> <agent>` — pl. `/reassign b0f5393d-c04c-4a8a-a3e2-5681625fed1f runa`"
        else:
            _tid, _new = _parts[0].strip(), _parts[1].strip().lstrip("@").lower()
            try:
                _dl = getattr(node, "delegation", None)
                if _dl is None:
                    from core.delegation import DelegationManager
                    _dl = DelegationManager(node.node_name, getattr(node, "pg_pool", None) or getattr(node, "_pg_pool", None))
                    node.delegation = _dl
                _ok = await _dl.reassign_task(_tid, _new)
                if _ok:
                    await _dl.add_note(_tid, f"[REASSIGN] Chat parancs: {username} átirányította → {_new}", "system")
                    out_content = f"✅ **Task átirányítva** → `{_new}`\n• task_id: `{_tid[:18]}…`"
                else:
                    out_content = f"❌ Átirányítás sikertelen — a task nem található vagy nem pending/accepted állapotban van: `{_tid[:18]}…`"
            except Exception as _ra_e:
                out_content = f"❌ Reassign hiba: {_ra_e}"

    elif cmd == "cancel":
        # /cancel <task_id> — delegáció érvénytelenítése
        _tid = (args or "").strip()
        if not _tid:
            out_content = "⚠️ Használat: `/cancel <task_id>` — task_id-t a `/tasks` listából"
        else:
            try:
                _dl = getattr(node, "delegation", None)
                if _dl is None:
                    from core.delegation import DelegationManager
                    _dl = DelegationManager(node.node_name, getattr(node, "pg_pool", None) or getattr(node, "_pg_pool", None))
                    node.delegation = _dl
                _ok = await _dl.cancel_task(_tid)
                if _ok:
                    await _dl.add_note(_tid, f"[CANCEL] Chat parancs: {username} érvénytelenítette", "system")
                    out_content = f"🚫 **Task érvénytelenítve**: `{_tid[:18]}…`"
                else:
                    out_content = f"❌ Törlés sikertelen — a task nem található vagy már lefutott: `{_tid[:18]}…`"
            except Exception as _cx_e:
                out_content = f"❌ Cancel hiba: {_cx_e}"

    elif cmd in ("debate", "ask", "all", "ideas", "vote"):
        # These are ROUTED to agents with special framing — handled by returning
        # a directive the normal path will use. Store marker prefix in content.
        if cmd == "debate" and not args:
            out_content = "⚠️ Használat: `/debate <téma>` — pl. `/debate mennyi 2+2`"
        elif cmd == "ask":
            sub = args.split(maxsplit=1) if args else []
            if len(sub) < 2:
                out_content = "⚠️ Használat: `/ask <agent> <kérdés>` — pl. `/ask morzsa mi a helyzet?`"
        elif cmd == "all" and not args:
            out_content = "⚠️ Használat: `/all <kérdés>`"
        elif cmd == "ideas" and not args:
            out_content = "⚠️ Használat: `/ideas <téma>` — pl. `/ideas hogyan lehetne gyorsabb a mesh P2P réteg?`"
        else:
            return {"route": cmd, "args": args}, True

    return {"content": out_content}, True


def _get_mesh_agent_names(node):
    """Dinamikus agent-név lista a mesh-ből (v0.46.7) — NINCS hardkódolva.

    Forrás sorrend: peer_discovery ismereplők + saját node név.
    A mesh tagság flexibilis — bármely node fel/le léphet, ez a függvény
    mindig az aktuális állapotot adja vissza.
    """
    names = set()
    node_name = getattr(node, "node_name", "") or ""
    if node_name:
        names.add(node_name.lower())
    pd = getattr(node, "peer_discovery", None)
    if pd is not None:
        try:
            for name in pd.get_all_peers().keys():
                if name:
                    names.add(str(name).lower())
        except Exception:
            try:
                kp = getattr(pd, "known_peers", None)
                if kp:
                    names.update(str(n).lower() for n in kp.keys())
            except Exception:
                pass
    return names


async def handle_chat_send(node, request, pool, user):
    """POST /api/chat/send — Send a DM from dashboard user to an agent.

    Body: { recipient: "morzsa", content: "hello", msg_type: "chat" }
    """
    from aiohttp import web
    data = await request.json()
    recipient = data.get("recipient", "broadcast")
    content = data.get("content", "") or data.get("text", "")
    msg_type = data.get("msg_type", "chat")
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    display_name = getattr(user, "display_name", None) or username
    node_name = getattr(node, "node_name", "nova")

    if not content.strip():
        return web.json_response({"error": "content is required"}, status=400)

    # ── MCP bridge end-device identity ──
    # A trusted bridge user (mcp-bridge) nevében érkező üzenetek a VALÓDI MCP
    # kliens neve alatt jelennek meg (pl. 'opencode') — sidebar + shared room.
    sender_name = username
    is_mcp_sender = False
    if username == "mcp-bridge":
        client = (data.get("sender") or "").strip()
        if client and client.replace("-", "").replace("_", "").isalnum():
            sender_name = client
            is_mcp_sender = True
            await _ensure_chat_user(pool, sender_name, client, node_name)
            # end-device jelzés a chat users táblában
            try:
                await pool.execute(
                    """UPDATE mesh.mesh_chat_users SET is_mcp_end_device = true WHERE username = $1""",
                    sender_name)
            except Exception:
                pass  # oszlop még nem létezik — nem blokkol

    await _ensure_chat_user(pool, username, display_name, node_name)

    # v0.46.15: MCP end-device üzenetek a KÖZÖS SZOBÁBA kerülnek (username='broadcast'),
    # hogy minden participant lássa őket — pontosan, mint egy bejelentkezett user üzeneteit.
    # A DM-ből érkezők is: a shared room megjeleníti a DM-eket is (sender/recipient szűrés
    # nélkül, l. handle_chat_messages broadcast query) — így az opencode "teljes jogú".
    if is_mcp_sender and recipient != "broadcast":
        # DM az end-device-tól: tárolás 'broadcast' username-nel, hogy a shared room
        # és a DM-view is lássa; a recipient marad az eredeti címzett.
        pass  # az insert lentebb már username=broadcast-szel megy (lásd insert_pg)

    msg_uuid = str(uuid.uuid4())

    # v0.46.15: MCP end-device küldés → username='broadcast' (közös szoba láthatóság)
    insert_username = "broadcast" if is_mcp_sender else username

    # Store in PG
    try:
        row = await pool.fetchrow(
            """INSERT INTO mesh.mesh_chat_messages
               (message_uuid, username, sender, recipient, content, msg_type, status)
               VALUES ($1, $2, $3, $4, $5, $6, 'sent')
               RETURNING id, created_at""",
            msg_uuid, insert_username, sender_name, recipient, content, msg_type
        )
        msg_id = row["id"] if row else None
        created_at = str(row["created_at"]) if row else None
    except Exception as e:
        return web.json_response({"error": f"DB error: {e}"}, status=500)

    # v0.46.15: Ha a címzett egy MCP end-device (DM → opencode), a üzenet az
    # mcp_end_device_inbox queue-ba is bekerül — a bridge on-demand kézbesíti
    # (mesh_inbox tool) vagy long-poll. A dashboard user→end-device DM így célba ér.
    if not is_mcp_sender and recipient not in ("broadcast", "") and not recipient.startswith("user:"):
        try:
            from core.mcp_registry import is_end_device
            if is_end_device(recipient, parent_node=node_name):
                await pool.execute(
                    """INSERT INTO mesh.mcp_end_device_inbox
                       (client_name, kind, sender, sender_display, content, message_uuid)
                       VALUES ($1, 'dm', $2, $3, $4, $5)""",
                    recipient, username, display_name, content, msg_uuid
                )
                log.info(f"📥 MCP inbox: DM {username}→{recipient} queued for end-device delivery")
                # v0.46.16: DM-wake — a tétlen MCP kliens (opencode) headless futtatással
                # ébreszthető: `opencode run "prompt"` SSH-n a kliens hostján.
                # A prompt ráirányítja a mesh_inbox toolra → feldolgozza a DM-eket.
                try:
                    from core import mcp_registry as _mreg
                    _client = _mreg._load().get(recipient, {})
                    _host = _client.get("host", "") or ("192.168.1.30" if _client.get("parent_node") == "morzsa" else "")
                    if _host:
                        # ── Wake 2.0: MCP DM-wake dedup a mesh.wake_log-on keresztül ──
                        # Korábban: minden bejövő DM új SSH Popen-t indított (race condition,
                        # több párhuzamos opencode run ugyanarra a kliensre).
                        # Most: 60s coalescing ablak — egy kliensre egy wake.
                        _wl_dup = False
                        try:
                            _pool = getattr(node, 'pg_pool', None) or getattr(node, '_pg_pool', None)
                            if _pool:
                                from core.wake_lib import WakeLog
                                _wlog = WakeLog(_pool)
                                _ok, _wid, _wstat = await _wlog.send_wake(
                                    target_agent=f"mcp:{recipient}",
                                    target_host=_host,
                                    health_port=0,  # SSH-alapú wake, nincs HTTP port
                                    prompt=f"DM-wake {recipient} (opencode)",
                                    sender=username,
                                    message_id=msg_uuid,
                                    wake_type="mcp_dm",
                                    payload={},  # SSH megy külön az alábbi Popen-nel
                                )
                                if _wstat == "coalesced":
                                    _wl_dup = True
                                    log.info(f"🔁 MCP DM-wake coalesced → {recipient} (60s ablak, wake_log {_wid})")
                        except Exception as _wl_e:
                            log.debug(f"wake_log MCP dedup failed (fallback direct): {_wl_e}")
                        if _wl_dup:
                            return web.json_response({
                                "ok": True, "message_id": msg_uuid, "db_id": msg_id,
                                "recipient": recipient, "status": "coalesced_wake",
                            })
                        _wake_prompt = (
                            f"🔔 Új DM érkezett {username}-tól: {content[:300]}\n"
                            f"Hívd meg a mesh_inbox MCP eszközt, olvasd el a DM-eket, "
                            f"majd válaszolj a mesh_dm_send eszközzel (recipient: {username})."
                        )
                        import subprocess as _sp
                        _cmd = (
                            f"timeout 240 ~/.opencode/bin/opencode run "
                            f"{_wake_prompt!r}"
                        )
                        _sp.Popen(
                            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                             f"openclaw@{_host}", _cmd],
                            stdout=_sp.DEVNULL, stderr=_sp.DEVNULL,
                        )
                        log.info(f"🔔 MCP DM-wake: opencode run → {_host} (session a2a-dm-{msg_uuid[:8]})")
                    else:
                        log.debug(f"MCP DM-wake: {recipient} host ismeretlen — pull kézbesítés marad")
                except Exception as _we:
                    log.warning(f"MCP DM-wake failed: {_we}")
        except Exception as _qe:
            log.debug(f"MCP inbox queue failed: {_qe}")

    # ── Telegram-style /command interceptor ──
    if content.strip().startswith("/"):
        _cmd_result, _handled = await _process_chat_command(
            node, pool, username, display_name, recipient, content.strip()
        )
        if _handled:
            # Routing directive (debate/ask/all)? → mark content so routing picks it up
            if isinstance(_cmd_result, dict) and _cmd_result.get("route"):
                cmd_route = _cmd_result["route"]
                cmd_args = _cmd_result.get("args", "")
                # fall through to normal mesh routing with special payload below
            else:
                # Plain command (help/status/clear) — store the system reply and stop
                if _cmd_result.get("content"):
                    try:
                        await pool.execute(
                            """INSERT INTO mesh.mesh_chat_messages
                               (message_uuid, username, sender, recipient, content, msg_type, status)
                               VALUES ($1, $2, $3, $4, $5, 'agent_reply', 'sent')""",
                            str(uuid.uuid4()), username, node_name, recipient, _cmd_result["content"],
                        )
                    except Exception as e:
                        log.warning(f"Command reply store failed: {e}")
                return web.json_response({
                    "ok": True,
                    "message_id": msg_uuid,
                    "db_id": msg_id,
                    "recipient": recipient,
                    "command": True,
                    "status": "sent",
                })
    else:
        cmd_route = None
        cmd_args = ""

    # Route to agent via mesh (if not broadcast and not self)
    # If recipient starts with "user:", it's a user→user DM — store only in PG
    # (the other user will see it via /api/chat/messages polling)
    mesh_sent = False
    is_user_dm = recipient.startswith("user:")

    if is_user_dm:
        # User→user DM: already stored in PG above, the recipient user will see it
        # when they poll /api/chat/messages. No mesh routing needed.
        log.info(f"💬 Chat user DM {username}→{recipient}: stored in PG")
        mesh_sent = True
    elif recipient == node_name:
        # User → self (this node): process locally via ollama API
        try:
            mesh_sent = True
            log.info(f"💬 Chat local {username}→{recipient}: sent (triggering self-wake)")
            # Trigger self-wake via local wake-agent API (ollama direct)
            import asyncio as _aio
            import aiohttp as _aiohttp_sw
            my_host = "127.0.0.1"
            reply_endpoint = f"http://{my_host}:{node.config.health_port}/api/agent-reply"
            async def _self_wake():
                await _aio.sleep(1)
                try:
                    # v0.46.0 R0: Honcho context injection (same as mesh path in node.py)
                    _sw_prompt = f"Új üzenet érkezett {username}-tól: {content[:500]}"
                    try:
                        from core.honcho_bridge import get_honcho_context
                        _hb_ctx = await get_honcho_context(getattr(node, '_pg_pool', None), username.lower(), chat_username=username)
                        if _hb_ctx:
                            _sw_prompt = f"{_hb_ctx}\n\n{_sw_prompt}"
                            log.info(f"🪪 Honcho context injected into self-wake prompt (peer: {username.lower()})")
                    except Exception as _hb_e:
                        log.debug(f"Honcho context injection (self-wake) skipped: {_hb_e}")
                    wake_url = f"http://127.0.0.1:{node.config.health_port}/api/wake-agent"
                    async with _aiohttp_sw.ClientSession() as sess:
                        # Retry on 429 (busy/cooldown) — DM replies must not be dropped
                        _dm_attempts = 4
                        for _att in range(1, _dm_attempts + 1):
                            async with sess.post(wake_url, json={
                                "prompt": _sw_prompt,
                                "agent_name": node_name,
                                "sender": username,
                                "sender_display": display_name,
                                "chat_username": username,
                                "chat_msg_uuid": msg_uuid,
                                "chat_type": "user_dm",
                                "reply_endpoint": reply_endpoint,
                                "mesh_secret": "mesh-wake-secret-2026"
                            }, timeout=_aiohttp_sw.ClientTimeout(total=120)) as resp:
                                if resp.status == 200:
                                    log.info(f"🔔 Self-wake DM: {resp.status}")
                                    return
                                elif resp.status == 429 and _att < _dm_attempts:
                                    _ra = 8
                                    try:
                                        _ra = max(2, min(int((await resp.json()).get("retry_after", 8)), 20))
                                    except Exception:
                                        pass
                                    log.info(f"⏳ Self-wake DM 429 — attempt {_att}/{_dm_attempts}, retry in {_ra}s")
                                    await _aio.sleep(_ra)
                                    continue
                                else:
                                    log.info(f"🔔 Self-wake DM: {resp.status}")
                                    return
                except Exception as e:
                    log.warning(f"🔔 Self-wake DM failed: {e}")
            _aio.create_task(_self_wake())
        except Exception as e:
            log.warning(f"💬 Chat local reply failed: {e}")
    elif recipient == "broadcast":
        # ── Broadcast: send to ALL peers via mesh + wake-agent ALL ──
        try:
            # Command framing (debate/all): agents get explicit role instructions
            _cmd_prefix = ""
            if cmd_route == "debate":
                _cmd_prefix = (
                    f"🔔 VITA INDUL — téma: {cmd_args}\n"
                    "SZEREP: Kifejted a SAJÁT álláspontodat a témáról, majd egy KÜLÖNBÖZŐ agent nevét megcímezve "
                    "konkrét kihívást/ellenvetést fogalmazol meg neki. Rövid, éles érvelés.\n"
                    "ÖTLETLÁDA: Ha a vitából konkrét a2a-mesh-fejlesztési javaslatod születik, azt MINDIG "
                    "külön sorban jelöld: [ÖTLET] <javaslat> — ez automatikusan az Ötletládába kerül.\n"
                )
            elif cmd_route == "all":
                _cmd_prefix = (
                    f"🔔 KÖZÖS ELEMZÉS — kérdés: {cmd_args}\n"
                    "SZEREP: Mindegyikőtök ugyanarra a kérdésre válaszol — SAJÁT nézőpontból, különböző szemszögekből. Ne ismételj másra.\n"
                )
            elif cmd_route == "ideas":
                _cmd_prefix = (
                    f"🗳️ ÖTLETSZERVERTÉS — téma: {cmd_args}\n"
                    "SZEREP: Összedöntöd a legjobb a2a-mesh-fejlesztési ÖTLETEIDET ehhez a témához. "
                    "MINDEN javaslatot KÜLÖNB sorban, pontosan így jelölve adj meg:\n"
                    "[ÖTLET] <konkrét, megvalósítható javaslat>\n"
                    "Például:\n"
                    "[ÖTLET] P2P keepalive ping-ek batchelése a forgalom csökkentésére\n"
                    "[ÖTLET] Kanban kártyák automatikus archiválása 30 nap után\n"
                    "A jelölt sorok AUTOMATIKUSAN az Ötletládába kerülnek. Rövid indoklás is elfér.\n"
                )
            elif cmd_route == "vote":
                # Ötletláda-lista lekérése, hogy az agentek konkrét ID-kkal szavozzanak
                try:
                    _idea_rows = await pool.fetch(
                        """SELECT idea_id, title, upvotes, downvotes FROM mesh.mesh_ideas
                           WHERE status = 'idea' ORDER BY created_at DESC LIMIT 6""")
                    _idea_list = "\n".join(
                        f"  • {r['idea_id']} — {r['title'][:60]} (+{r['upvotes']}/-{r['downvotes']})"
                        for r in _idea_rows) or "  (nincs nyitott ötlet)"
                except Exception as _e:
                    _idea_list = f"  (lista-lekérés sikertelen: {_e})"
                _cmd_prefix = (
                    f"🗳️ SZAVAZÁS INDUL — ötletláda szavazat! {('Célpont: ' + cmd_args) if cmd_args else 'Az alábbi nyitott ötletekre'}\n"
                    "NYITOTT ÖTLETEK:\n"
                    f"{_idea_list}\n"
                    "SZEREP: Minden agent EGY SZAVAZATOT ad le. A szavazat formátuma KÖTELEZŐ:\n"
                    "[SZAVAZAT] idea_<id> up   (támogatás) vagy\n"
                    "[SZAVAZAT] idea_<id> down (elutasítás)\n"
                    "A szavazatokat a rendszer automatikusan rögzíti. Score ≥ +2 → approved, ≤ -2 → rejected.\n"
                    "Véleményedet röviden indokold, de a [SZAVAZAT] sor kötelező!\n"
                )
            payload = {
                "text": f"{_cmd_prefix}{content}" if _cmd_prefix else content,
                "subject": content[:80],
                "sender_display": display_name,
                "chat_username": username,
                "chat_msg_uuid": msg_uuid,
                "chat_type": "broadcast",
                "command": cmd_route or "",
            }
            result = await node.broadcast("a2a_message", payload, priority=5)
            mesh_sent = True
            log.info(f"💬 Chat broadcast {username}→all: sent via mesh (success={result.success}, transport={result.transport})")

            # ── Topic switch: create capsule from previous conversation ──
            from .capsules import TOPIC_SWITCH_MARKERS, store_capsule, extract_topic_from_prompt, summarize_conversation
            is_topic_switch = any(marker in content for marker in TOPIC_SWITCH_MARKERS)
            if is_topic_switch:
                log.info(f"🔔 Topic switch detected in chat_send — creating capsule")
                try:
                    # Fetch previous messages from PG
                    rows = await pool.fetch(
                        """SELECT sender, content FROM mesh.mesh_chat_messages
                           WHERE msg_type = 'chat' AND status = 'sent'
                             AND content NOT LIKE '%🔔%'
                           ORDER BY created_at DESC LIMIT 20"""
                    )
                    prev_msgs = []
                    for row in reversed(rows):  # Reverse to chronological
                        prev_msgs.append({'sender': row['sender'], 'content': row['content'] or ''})

                    if len(prev_msgs) >= 2:
                        prev_topic = "ismeretlen téma"
                        for h in reversed(prev_msgs):
                            topic = extract_topic_from_prompt(h.get('content', ''))
                            if topic:
                                prev_topic = topic
                                break
                        if not prev_topic or prev_topic == "ismeretlen téma":
                            texts = [h.get('content', '')[:100] for h in prev_msgs[:3]]
                            prev_topic = ' '.join(texts)[:200]

                        summary_msgs = [{'sender': h.get('sender', '?'), 'text': h.get('content', '')} for h in prev_msgs]
                        summary = summarize_conversation(summary_msgs)
                        agents_involved = list(set(h.get('sender', '') for h in prev_msgs if h.get('sender', '').lower() in ('nova', 'morzsa', 'runa')))

                        await store_capsule(
                            pool, prev_topic, summary, agents_involved,
                            0, 0,
                        )
                        log.info(f"📚 Capsule created in chat_send for topic '{prev_topic[:50]}' ({len(prev_msgs)} msgs)")
                except Exception as e:
                    log.warning(f"Capsule creation in chat_send failed: {e}")

            import asyncio as _aio

            # Get my LAN IP for reply_endpoint
            my_host = "127.0.0.1"
            if hasattr(node, '_get_local_ip'):
                try:
                    my_host = node._get_local_ip()
                except Exception:
                    pass
            reply_endpoint = f"http://{my_host}:{node.config.health_port}/api/agent-reply"

            # ── @mention awareness: direct address in the common room ──
            # If the message contains @agentname(s), ONLY those agents wake with a
            # "NEKED ÍRTÁK" directive; unmentioned agents stay silent.
            import re as _re_mention
            _mentioned = [m.lower() for m in _re_mention.findall(r"@(\w+)", content)]
            # v0.46.7: dinamikus agent-lista a mesh-ből (nem hardkódolt —
            # a mano hiánya miatt korábban @mano → broadcast minden agentre)
            _valid_agents = _get_mesh_agent_names(node)
            _mentioned = [a for a in _mentioned if a in _valid_agents]
            if _mentioned:
                log.info(f"💬 Mention detected in broadcast: {', '.join(_mentioned)} — targeted wake only")

            # Wake-agent on ALL online peers
            FALLBACK_PEERS = {
                "morzsa": {"host": "192.168.1.30", "health_port": 8650},
                "runa": {"host": "192.168.1.100", "health_port": 8650},
                "nova": {"host": "192.168.1.8", "health_port": 8650},
                "tor": {"host": "100.74.221.46", "health_port": 8650},
                "mano": {"host": "192.168.1.43", "health_port": 8650},
            }
            for peer_name, peer_info in FALLBACK_PEERS.items():
                if peer_name == node_name:
                    continue  # Skip self
                # Mention targeting: skip agents that were NOT mentioned
                if _mentioned and peer_name not in _mentioned:
                    log.info(f"💬 Skip wake {peer_name} — not @mentioned")
                    continue
                peer_host = peer_info["host"]
                peer_port = peer_info["health_port"]
                wake_url = f"http://{peer_host}:{peer_port}/api/wake-agent"
                log.info(f"🔔 Wake-agent broadcast → {peer_name} at {wake_url}")
                _mentioned_direct = peer_name in _mentioned

                # ── Wake 2.0: dedup + coalescing a mesh.wake_log-on keresztül ──
                # 60s-en belüli ismételt wake ugyanarra a peer-re NEM indul újra
                # (a P2P-triggerelt wake-et a peer node maga logolja a dedupban).
                _wl_body = {
                    "prompt": (f"🔔 NEKED ÍRTÁK a közös szobában! {username} kifejezetten hozzád intézte: {_cmd_prefix}{content}"
                               if _mentioned_direct else
                               f"Új üzenet érkezett {username}-tól (közös szoba): {_cmd_prefix}{content}")[:2000],
                    "agent_name": peer_name,
                    "sender": username,
                    "sender_display": display_name,
                    "chat_username": username,
                    "chat_msg_uuid": msg_uuid,
                    "chat_type": "broadcast",
                    "reply_endpoint": reply_endpoint,
                    "mesh_secret": "mesh-wake-secret-2026",
                }
                async def _wake_broadcast(pn=peer_name, url=wake_url, ment=_mentioned_direct, wl_body=_wl_body):
                    import aiohttp as _aiohttp
                    await _aio.sleep(2)  # Delay 2s — let P2P wake-agent trigger first
                    try:
                        _pool = getattr(node, 'pg_pool', None) or getattr(node, '_pg_pool', None)
                        if _pool:
                            from core.wake_lib import WakeLog
                            _wlog = WakeLog(_pool)
                            _ok, _wid, _wstat = await _wlog.send_wake(
                                target_agent=pn,
                                target_host=peer_host,
                                health_port=peer_port,
                                prompt=wl_body["prompt"],
                                sender=username,
                                message_id=msg_uuid,
                                wake_type="broadcast",
                                payload=wl_body,
                            )
                            if _wstat == "coalesced":
                                log.info(f"🔁 Wake coalesced → {pn} (wake_log {_wid}) — no duplicate HTTP POST")
                                return
                        # Fallback: legacy direct POST (nincs PG vagy a wake_log hibás)
                        async with _aiohttp.ClientSession() as sess:
                            async with sess.post(url, json=wl_body, timeout=_aiohttp.ClientTimeout(total=120)) as resp:
                                if resp.status == 429:
                                    log.info(f"🔔 Wake-agent broadcast {pn}: 429 (P2P already triggered — OK)")
                                else:
                                    log.info(f"🔔 Wake-agent broadcast {pn}: {resp.status}")
                    except Exception as e:
                        log.warning(f"🔔 Wake-agent broadcast {pn} failed: {e}")
                        # ── MQTT wake fallback (ha HTTP nem ment, de a peer MQTT-n elerheto) ──
                        try:
                            _mqtt_tr = getattr(node, '_mqtt_transport', None)
                            if _mqtt_tr and _mqtt_tr.is_available():
                                _mqtt_tr.publish_wake_to(pn, json.dumps(wl_body))
                                log.info(f"🔔 Wake-agent broadcast {pn}: MQTT fallback sent ✓")
                        except Exception as _mqtt_e:
                            log.warning(f"🔔 Wake-agent broadcast {pn} MQTT fallback failed: {_mqtt_e}")
                _aio.create_task(_wake_broadcast())

            # Self-wake: Nova also responds to broadcast (not just peers)
            # v0.46.9: mention-szűrés itt is — ha @valaki mást említettek, Nova NEM kel fel
            _self_mentioned = node_name.lower() in _mentioned
            if _mentioned and not _self_mentioned:
                log.info(f"💬 Skip self-wake: @{', @'.join(_mentioned)} mentioned, not me ({node_name})")
            else:
                try:
                    self_wake_url = f"http://127.0.0.1:{node.config.health_port}/api/wake-agent"
                    async def _wake_self_broadcast():
                        import aiohttp as _aiohttp_sw
                        await _aio.sleep(1)
                        try:
                            if _self_mentioned:
                                _sw_prompt = f"🔔 NEKED ÍRTÁK a közös szobában! {username} kifejezetten hozzád intézte: {_cmd_prefix}{content}"[:2000] + " — VÁLASZOLNOD KELL. Több agentnak nem kell válaszolnia."
                            else:
                                _sw_prompt = f"Új üzenet érkezett {username}-tól (közös szoba): {_cmd_prefix}{content}"[:2000]
                            async with _aiohttp_sw.ClientSession() as sess:
                                async with sess.post(self_wake_url, json={
                                    "prompt": _sw_prompt,
                                    "agent_name": node_name,
                                    "sender": username,
                                    "sender_display": display_name,
                                    "chat_username": username,
                                    "chat_msg_uuid": msg_uuid,
                                    "chat_type": "broadcast",
                                    "reply_endpoint": reply_endpoint,
                                    "mesh_secret": "mesh-wake-secret-2026"
                                }, timeout=_aiohttp_sw.ClientTimeout(total=120)) as resp:
                                    log.info(f"🔔 Self-wake broadcast: {resp.status}")
                        except Exception as e:
                            log.warning(f"🔔 Self-wake broadcast failed: {e}")
                    _aio.create_task(_wake_self_broadcast())
                except Exception as e:
                    log.warning(f"Self-wake setup failed: {e}")
        except Exception as e:
            log.warning(f"💬 Chat broadcast {username}→all: mesh send failed: {e}")
    elif recipient not in ("broadcast", ""):
        # /ask command in a DM channel → reroute to the asked agent if specified
        _dm_target = recipient
        _dm_text = content
        if cmd_route == "ask" and cmd_args:
            _ask_parts = cmd_args.split(maxsplit=1)
            if len(_ask_parts) == 2 and _ask_parts[0].lower() in _get_mesh_agent_names(node):  # v0.46.7: dinamikus
                _dm_target = _ask_parts[0].lower()
                _dm_text = f"🔔 CÉLZOTT KÉRDÉS (zsolt): {_ask_parts[1]}"
        try:
            payload = {
                "text": _dm_text,
                "subject": _dm_text[:80],
                "sender_display": display_name,
                "chat_username": username,
                "chat_msg_uuid": msg_uuid,
                "chat_type": "user_dm",
                "command": cmd_route or "",
            }
            result = await node.send_direct(_dm_target, "a2a_message", payload, priority=5)
            mesh_sent = True
            log.info(f"💬 Chat DM {username}→{recipient}: sent via mesh")
            # Auto-ack removed — receiver node sends ack via P2P + wake-agent reply
            # Trigger wake-agent on the receiving node via its dashboard API
            try:
                import asyncio as _aio
                # Get peer host from known_peers, static config, or hardcoded fallback
                peer_info = None
                if hasattr(node, 'peer_discovery'):
                    kp = getattr(node.peer_discovery, 'known_peers', None)
                    if kp:
                        peer_info = kp.get(recipient)
                # Fallback: hardcoded peer IPs (avoids P2P discovery dependency)
                if not peer_info:
                    FALLBACK_PEERS = {
                        "morzsa": {"host": "192.168.1.30", "health_port": 8650},
                        "runa": {"host": "192.168.1.100", "health_port": 8650},
                        "nova": {"host": "192.168.1.8", "health_port": 8650},
                    }
                    peer_info = FALLBACK_PEERS.get(recipient)
                if peer_info:
                    peer_host = peer_info.get('host', '')
                    peer_health_port = peer_info.get('health_port', 8650)
                    wake_url = f"http://{peer_host}:{peer_health_port}/api/wake-agent"
                    log.info(f"🔔 Wake-agent HTTP {recipient} → {wake_url}")
                    # Use peer's actual host for reply_endpoint (not 127.0.0.1)
                    # so the peer can POST the agent's reply back to our dashboard API
                    my_host = "127.0.0.1"
                    if hasattr(node, '_get_local_ip'):
                        try:
                            my_host = node._get_local_ip()
                        except Exception:
                            pass
                    elif hasattr(node.config, 'host') and node.config.host:
                        my_host = node.config.host
                    reply_endpoint = f"http://{my_host}:{node.config.health_port}/api/agent-reply"
                    async def _wake():
                        import aiohttp as _aiohttp
                        try:
                            async with _aiohttp.ClientSession() as sess:
                                async with sess.post(wake_url, json={
                                    "prompt": f"Új üzenet érkezett {username}-tól: {content[:500]}",
                                    "agent_name": recipient,
                                    "sender": username,
                                    "sender_display": display_name,
                                    "chat_username": username,
                                    "chat_msg_uuid": msg_uuid,
                                    "chat_type": "user_dm",
                                    "reply_endpoint": reply_endpoint,
                                    "mesh_secret": "mesh-wake-secret-2026"
                                }, timeout=_aiohttp.ClientTimeout(total=180)) as resp:
                                    log.info(f"🔔 Wake-agent {recipient}: {resp.status}")
                        except Exception as e:
                            log.warning(f"🔔 Wake-agent {recipient} failed: {e}")
                    _aio.create_task(_wake())
            except Exception as e:
                log.debug(f"Wake-agent remote trigger failed: {e}")
        except Exception as e:
            log.warning(f"💬 Chat DM {username}→{recipient}: mesh send failed: {e}")

    return web.json_response({
        "ok": True,
        "message_id": msg_uuid,
        "db_id": msg_id,
        "recipient": recipient,
        "mesh_sent": mesh_sent,
        "created_at": created_at,
        "status": "sent"
    })


async def handle_chat_messages(node, request, pool, user):
    """GET /api/chat/messages?with=morzsa&limit=30&before_id=123

    Chat history with cursor-based pagination:
    - Initial load: returns last `limit` messages (default 30)
    - Scroll-up: pass before_id (oldest loaded message id) to fetch older messages
    - Response includes total_count so the frontend knows if more history exists
    """
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    peer = request.query.get("with", "")
    limit = min(int(request.query.get("limit", 30)), 200)
    before_id = request.query.get("before_id", "")
    try:
        before_id = int(before_id) if before_id else None
    except ValueError:
        before_id = None

    try:
        if peer:
            # DM conversation between user and specific agent/user
            # v0.44.1: broadcast agent replies (username='broadcast') must also appear
            # in DM view with that agent, otherwise replies vanish on reload.
            # v0.48.8: user↔user DM — a beszélgetés a KÉT résztvevő szemszögéből is
            # lekérdezhető: az üzenet username=a küldő, de a címzett user
            # a sender/recipient mezők alapján találja rá (user:x ↔ user:y).
            is_user_peer = peer.startswith("user:")
            # user↔user DM rekordstruktúra: a KÜLDŐ sorában
            # username=zsolt, sender=zsolt, recipient='user:hajnalka'.
            # A címzett (hajnalka) szemszögéből a partnerrel folytatott
            # beszélgetés = amit ő küldött (recipient=peer) + amit a partner
            # küldött neki (sender=partner_neve, recipient='user:'||ő_maga).
            if is_user_peer:
                partner = peer[5:]  # 'user:zsolt' → 'zsolt'
                if before_id:
                    rows = await pool.fetch(
                        """SELECT id, message_uuid, username, sender, recipient, content,
                                  msg_type, status, created_at, read_at
                           FROM mesh.mesh_chat_messages
                           WHERE (username = $1 AND recipient = $2 AND id < $3)
                              OR (sender = $4 AND recipient = 'user:' || $1 AND id < $3)
                           ORDER BY created_at DESC LIMIT $5""",
                        username, peer, before_id, partner, limit
                    )
                else:
                    rows = await pool.fetch(
                        """SELECT id, message_uuid, username, sender, recipient, content,
                                  msg_type, status, created_at, read_at
                           FROM mesh.mesh_chat_messages
                           WHERE (username = $1 AND recipient = $2)
                              OR (sender = $3 AND recipient = 'user:' || $1)
                           ORDER BY created_at DESC LIMIT $4""",
                        username, peer, partner, limit
                    )
                # Total count for this conversation
                total_row = await pool.fetchrow(
                    """SELECT COUNT(*) as cnt FROM mesh.mesh_chat_messages
                       WHERE (username = $1 AND recipient = $2)
                          OR (sender = $3 AND recipient = 'user:' || $1)""",
                    username, peer, partner
                )
            elif before_id:
                # Agent-DM, régebbi üzenetek (scroll-up)
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE (username = $1 OR username = 'broadcast')
                         AND (sender = $2 OR recipient = $2)
                         AND id < $3
                       ORDER BY created_at DESC LIMIT $4""",
                    username, peer, before_id, limit
                )
                total_row = await pool.fetchrow(
                    """SELECT COUNT(*) as cnt FROM mesh.mesh_chat_messages
                       WHERE (username = $1 OR username = 'broadcast')
                         AND (sender = $2 OR recipient = $2)""",
                    username, peer
                )
            else:
                # Agent-DM, első betöltés
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE (username = $1 OR username = 'broadcast')
                         AND (sender = $2 OR recipient = $2)
                       ORDER BY created_at DESC LIMIT $3""",
                    username, peer, limit
                )
                total_row = await pool.fetchrow(
                    """SELECT COUNT(*) as cnt FROM mesh.mesh_chat_messages
                       WHERE (username = $1 OR username = 'broadcast')
                         AND (sender = $2 OR recipient = $2)""",
                    username, peer
                )
        else:
            # All messages for this user (general/broadcast channel)
            # v0.44.1: broadcast agent replies are stored with username='broadcast'
            # (see dashboard.py on_mesh_message persist) — include them so the shared
            # conversation survives a page reload, not just via live WS.
            # v0.48.8: user↔user DM sorok NEM kerülnek a general listába —
            # a recipient 'user:' prefixszel kezdődik (self-DM: user:<sajátnév>),
            # ezek kizárólag a DM nézetben jelennek meg.
            if before_id:
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE (username = $1 OR (username = 'broadcast' AND recipient = 'broadcast'))
                         AND id < $2
                         AND NOT (recipient LIKE 'user:%')
                       ORDER BY created_at DESC LIMIT $3""",
                    username, before_id, limit
                )
            else:
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE (username = $1 OR (username = 'broadcast' AND recipient = 'broadcast'))
                         AND NOT (recipient LIKE 'user:%')
                       ORDER BY created_at DESC LIMIT $2""",
                    username, limit
                )
            total_row = await pool.fetchrow(
                """SELECT COUNT(*) as cnt FROM mesh.mesh_chat_messages
                   WHERE (username = $1 OR (username = 'broadcast' AND recipient = 'broadcast'))
                     AND NOT (recipient LIKE 'user:%')""",
                username
            )

        messages = []
        # Collect message_uuids of file-type messages for attachment lookup
        _file_uuids = [r["message_uuid"] for r in rows if r.get("msg_type") == "file"]
        _attachments = {}
        if _file_uuids:
            try:
                _att_rows = await pool.fetch(
                    """SELECT message_uuid, file_name, safe_name, file_type, mime_type, file_size
                       FROM mesh.mesh_chat_files WHERE message_uuid = ANY($1)""",
                    _file_uuids,
                )
                for _ar in _att_rows:
                    _attachments[_ar["message_uuid"]] = dict(_ar)
            except Exception as _ae:
                log.debug(f"Attachment lookup failed: {_ae}")
        for row in rows:
            r = dict(row)
            r["created_at"] = str(r["created_at"]) if r.get("created_at") else None
            r["read_at"] = str(r["read_at"]) if r.get("read_at") else None
            if r.get("msg_type") == "file" and r.get("message_uuid") in _attachments:
                _a = _attachments[r["message_uuid"]]
                r["attachment"] = {
                    "file_name": _a["file_name"],
                    "safe_name": _a["safe_name"],
                    "mime_type": _a["mime_type"],
                    "file_size": _a["file_size"],
                    "url": f"/api/files/uploaded/{_a['safe_name']}",
                }
            messages.append(r)

        total_count = total_row["cnt"] if total_row else 0
        # has_more: are there older messages beyond what we just returned?
        oldest_id = messages[-1]["id"] if messages else 0
        has_more = oldest_id > 1 and len(messages) >= limit

        return web.json_response({
            "messages": messages,
            "count": len(messages),
            "total_count": total_count,
            "has_more": has_more,
            "oldest_id": oldest_id if messages else None,
        })
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_chat_inbox(node, request, pool, user):
    """GET /api/chat/inbox — Unread messages for this user from agents."""
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")

    try:
        rows = await pool.fetch(
            """SELECT id, message_uuid, sender, recipient, content,
                      msg_type, created_at
               FROM mesh.mesh_chat_messages
               WHERE username = $1 AND sender != $2 AND read_at IS NULL
               ORDER BY created_at DESC LIMIT 50""",
            username, username
        )

        unread = []
        for row in rows:
            r = dict(row)
            r["created_at"] = str(r["created_at"]) if r.get("created_at") else None
            unread.append(r)

        return web.json_response({"unread": unread, "count": len(unread)})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_chat_mcp_inbox(node, request, pool, user):
    """GET /api/chat/mcp-inbox?client=opencode&deliver=1

    Az MCP end-device beérkező üzenetei (DM queue). Csak a trusted bridge
    user (mcp-bridge) hívhatja. deliver=1 → delivered_at=now() jelölés.
    """
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    if username != "mcp-bridge":
        return web.json_response({"error": "forbidden — mcp-bridge only"}, status=403)

    client = request.query.get("client", "").strip()
    deliver = request.query.get("deliver", "0") == "1"
    if not client:
        return web.json_response({"error": "client parameter required"}, status=400)

    try:
        rows = await pool.fetch(
            """SELECT id, kind, sender, sender_display, content, message_uuid, created_at
               FROM mesh.mcp_end_device_inbox
               WHERE client_name = $1 AND delivered_at IS NULL
               ORDER BY created_at ASC LIMIT 100""",
            client
        )
        items = []
        for row in rows:
            r = dict(row)
            r["created_at"] = str(r["created_at"]) if r.get("created_at") else None
            items.append(r)
        if deliver and items:
            await pool.execute(
                """UPDATE mesh.mcp_end_device_inbox
                   SET delivered_at = NOW()
                   WHERE client_name = $1 AND delivered_at IS NULL""",
                client
            )
        return web.json_response({"items": items, "count": len(items)})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_chat_mark_read(node, request, pool, user):
    """POST /api/chat/read — Mark messages from a specific agent as read.
    Body: { from_agent: "morzsa" }
    """
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    data = await request.json()
    from_agent = data.get("from_agent", "")

    try:
        # v0.48.8: user↔user DM read-jelzés — a partner (user:xyz) által nekem
        # küldött üzenetek: sender=partner_neve (sima username!), recipient='user:'||én.
        # Az agent-DM marad a régi logika (username=én, sender=agent).
        is_user_from = from_agent.startswith("user:")
        if is_user_from:
            partner = from_agent[5:]
            result = await pool.execute(
                """UPDATE mesh.mesh_chat_messages
                   SET read_at = NOW()
                   WHERE read_at IS NULL
                     AND sender = $2
                     AND recipient = 'user:' || $1""",
                username, partner
            )
        else:
            result = await pool.execute(
                """UPDATE mesh.mesh_chat_messages
                   SET read_at = NOW()
                   WHERE username = $1 AND sender = $2 AND read_at IS NULL""",
                username, from_agent
            )
        return web.json_response({"ok": True, "updated": result})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_chat_contacts(node, request, pool, user):
    """GET /api/chat/contacts — List agents this user has chatted with + unread counts."""
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")

    # v0.48.8: heartbeat — a contacts-poll (3s) frissíti a hívó last_seen-jét,
    # így a bejelentkezett user online-nak látszik, amíg a lapja nyitva van.
    try:
        await pool.execute(
            "UPDATE mesh.mesh_chat_users SET last_seen = NOW() WHERE username = $1",
            username
        )
    except Exception:
        pass

    try:
        rows = await pool.fetch(
            """SELECT DISTINCT recipient as agent,
                      COUNT(*) as total,
                      COUNT(CASE WHEN read_at IS NULL AND sender != $1 THEN 1 END) as unread,
                      MAX(created_at) as last_msg
               FROM mesh.mesh_chat_messages
               WHERE username = $1 AND recipient != $1
               GROUP BY recipient
               ORDER BY last_msg DESC""",
            username
        )

        contacts = []
        for row in rows:
            r = dict(row)
            r["last_msg"] = str(r["last_msg"]) if r.get("last_msg") else None
            contacts.append(r)

        # Also list all available mesh agents (including self)
        try:
            agent_rows = await pool.fetch(
                "SELECT node_name FROM mesh.mesh_nodes ORDER BY node_name"
            )
            existing = {c["agent"] for c in contacts}
            local_node = getattr(node, "node_name", "nova")
            for ar in agent_rows:
                name = ar["node_name"]
                if name not in existing:
                    contacts.append({"agent": name, "total": 0, "unread": 0, "last_msg": None})
                    existing.add(name)
        except Exception:
            pass

        # MCP end devices (agents connected via the MCP bridge) — sidebar visibility
        try:
            mcp_rows = await pool.fetch(
                """SELECT username, display_name FROM mesh.mesh_chat_users
                   WHERE is_mcp_end_device = true AND username != $1 ORDER BY username""",
                username
            )
            for mr in mcp_rows:
                mname = mr["username"]
                contacts.append({
                    "agent": mname,
                    "display_name": mr.get("display_name") or mname,
                    "total": 0, "unread": 0, "last_msg": None,
                    "is_mcp_end_device": True,
                })
        except Exception:
            pass

        # Also list dashboard users (for user↔user DM)
        try:
            # v0.48.8: az alap a mesh.mesh_users auth tábla (minden regisztrált,
            # aktív user) — a chat_users-ból csak a display_name JOIN-olódik.
            # Node/bridge fiókok kiszűrése: mesh_nodes nevei, '%-agent' végűek,
            # mcp-bridge, mesh — ezekből amúgy is van agent-contact fentebb.
            # SAJÁT USER IS listázódik (megjelöltük: "(te)"), online-státusz a
            # chat_users.last_seen alapján (60s ablak = online).
            user_rows = await pool.fetch(
                """SELECT mu.username, COALESCE(cu.display_name, mu.display_name) AS display_name,
                          CASE WHEN cu.last_seen > NOW() - INTERVAL '60 seconds' THEN true ELSE false END AS online
                   FROM mesh.mesh_users mu
                   LEFT JOIN mesh.mesh_chat_users cu ON cu.username = mu.username
                   WHERE mu.is_active = 1
                     AND mu.username NOT IN (SELECT node_name FROM mesh.mesh_nodes)
                     AND mu.username NOT IN ('mcp-bridge', 'mesh')
                     AND mu.username NOT LIKE '%-agent'
                   ORDER BY online DESC, mu.username""",
            )
            for ur in user_rows:
                uname = "user:" + ur["username"]
                is_self = (ur["username"] == username)
                contacts.append({
                    "agent": uname,
                    # display_name tisztán marad — az "(te)" címkét a frontend
                    # teszi hozzá az is_self flag alapján (duplikáció elkerülése)
                    "display_name": ur.get("display_name") or ur["username"],
                    "total": 0, "unread": 0, "last_msg": None,
                    "is_user": True,
                    "is_self": is_self,
                    "online": bool(ur.get("online")),
                })
        except Exception:
            pass

        return web.json_response({"contacts": contacts, "count": len(contacts)})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def store_agent_reply(pool, username, from_agent, content, msg_type="agent_reply"):
    """Store an agent reply as a DM to the user. Called when an agent sends
    a reply message that contains chat_username in the payload."""
    try:
        await pool.execute(
            """INSERT INTO mesh.mesh_chat_messages
               (message_uuid, username, sender, recipient, content, msg_type, status)
               VALUES ($1, $2, $3, $2, $4, $5, 'delivered')""",
            str(uuid.uuid4()), username, from_agent, content, msg_type
        )
        log.info(f"💬 Chat reply stored: {from_agent}→{username} ({len(content)} chars)")
        return True
    except Exception as e:
        log.warning(f"Failed to store agent reply: {e}")
        return False