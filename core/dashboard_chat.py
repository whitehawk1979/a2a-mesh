"""A2A Mesh Chat — Per-user DM system for dashboard ↔ agent communication.

Each dashboard user gets a personal chat identity. Messages are stored in PG
(mesh.mesh_chat_messages) and routed to agents via the mesh. Agent replies are
stored as DMs back to the user.
"""
import uuid
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

    await _ensure_chat_user(pool, username, display_name, node_name)

    msg_uuid = str(uuid.uuid4())

    # Store in PG
    try:
        row = await pool.fetchrow(
            """INSERT INTO mesh.mesh_chat_messages
               (message_uuid, username, sender, recipient, content, msg_type, status)
               VALUES ($1, $2, $3, $4, $5, $6, 'sent')
               RETURNING id, created_at""",
            msg_uuid, username, username, recipient, content, msg_type
        )
        msg_id = row["id"] if row else None
        created_at = str(row["created_at"]) if row else None
    except Exception as e:
        return web.json_response({"error": f"DB error: {e}"}, status=500)

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
                    wake_url = f"http://127.0.0.1:{node.config.health_port}/api/wake-agent"
                    async with _aiohttp_sw.ClientSession() as sess:
                        # Retry on 429 (busy/cooldown) — DM replies must not be dropped
                        _dm_attempts = 4
                        for _att in range(1, _dm_attempts + 1):
                            async with sess.post(wake_url, json={
                                "prompt": f"Új üzenet érkezett {username}-tól: {content[:500]}",
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
                    f"🗳️ SZAVAZÁS INDUL — ötletláda szavazat! {('Célpont: ' + args) if args else 'Az alábbi nyitott ötletekre'}\n"
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
            _valid_agents = {"nova", "morzsa", "runa", "tor"}
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
                async def _wake_broadcast(pn=peer_name, url=wake_url, ment=_mentioned_direct):
                    import aiohttp as _aiohttp
                    await _aio.sleep(2)  # Delay 2s — let P2P wake-agent trigger first
                    try:
                        if ment:
                            _b_prompt = f"🔔 NEKED ÍRTÁK a közös szobában! {username} kifejezetten hozzád intézte: {_cmd_prefix}{content}"[:2000] + " — VÁLASZOLNOD KELL. Több agentnek nem kell válaszolnia."
                        else:
                            # _cmd_prefix ide is kell: a /ideas, /debate formátum-utasítás
                            # így jut el a peer-ekhez (korábban a P2P-ág [:300] vágása levette).
                            _b_prompt = f"Új üzenet érkezett {username}-tól (közös szoba): {_cmd_prefix}{content}"[:2000]
                        async with _aiohttp.ClientSession() as sess:
                            async with sess.post(url, json={
                                "prompt": _b_prompt,
                                "agent_name": pn,
                                "sender": username,
                                "sender_display": display_name,
                                "chat_username": username,
                                "chat_msg_uuid": msg_uuid,
                                "chat_type": "broadcast",
                                "reply_endpoint": reply_endpoint,
                                "mesh_secret": "mesh-wake-secret-2026"
                            }, timeout=_aiohttp.ClientTimeout(total=120)) as resp:
                                if resp.status == 429:
                                    log.info(f"🔔 Wake-agent broadcast {pn}: 429 (P2P already triggered — OK)")
                                else:
                                    log.info(f"🔔 Wake-agent broadcast {pn}: {resp.status}")
                    except Exception as e:
                        log.warning(f"🔔 Wake-agent broadcast {pn} failed: {e}")
                _aio.create_task(_wake_broadcast())

            # Self-wake: Nova also responds to broadcast (not just peers)
            try:
                self_wake_url = f"http://127.0.0.1:{node.config.health_port}/api/wake-agent"
                async def _wake_self_broadcast():
                    import aiohttp as _aiohttp_sw
                    await _aio.sleep(1)
                    try:
                        async with _aiohttp_sw.ClientSession() as sess:
                            async with sess.post(self_wake_url, json={
                                "prompt": f"Új üzenet érkezett {username}-tól (közös szoba): {_cmd_prefix}{content}"[:2000],
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
            if len(_ask_parts) == 2 and _ask_parts[0].lower() in ("nova", "morzsa", "runa", "tor"):
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
            if before_id:
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE username = $1 AND (sender = $2 OR recipient = $2)
                         AND id < $3
                       ORDER BY created_at DESC LIMIT $4""",
                    username, peer, before_id, limit
                )
            else:
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE username = $1 AND (sender = $2 OR recipient = $2)
                       ORDER BY created_at DESC LIMIT $3""",
                    username, peer, limit
                )
            # Total count for this conversation
            total_row = await pool.fetchrow(
                """SELECT COUNT(*) as cnt FROM mesh.mesh_chat_messages
                   WHERE username = $1 AND (sender = $2 OR recipient = $2)""",
                username, peer
            )
        else:
            # All messages for this user (general/broadcast channel)
            if before_id:
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE username = $1 AND id < $2
                       ORDER BY created_at DESC LIMIT $3""",
                    username, before_id, limit
                )
            else:
                rows = await pool.fetch(
                    """SELECT id, message_uuid, username, sender, recipient, content,
                              msg_type, status, created_at, read_at
                       FROM mesh.mesh_chat_messages
                       WHERE username = $1
                       ORDER BY created_at DESC LIMIT $2""",
                    username, limit
                )
            total_row = await pool.fetchrow(
                """SELECT COUNT(*) as cnt FROM mesh.mesh_chat_messages
                   WHERE username = $1""",
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


async def handle_chat_mark_read(node, request, pool, user):
    """POST /api/chat/read — Mark messages from a specific agent as read.
    Body: { from_agent: "morzsa" }
    """
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    data = await request.json()
    from_agent = data.get("from_agent", "")

    try:
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

        # Also list dashboard users (for user↔user DM)
        try:
            user_rows = await pool.fetch(
                "SELECT username, display_name FROM mesh.mesh_chat_users WHERE username != $1 ORDER BY username",
                username
            )
            for ur in user_rows:
                uname = "user:" + ur["username"]
                contacts.append({
                    "agent": uname,
                    "display_name": ur.get("display_name") or ur["username"],
                    "total": 0, "unread": 0, "last_msg": None,
                    "is_user": True
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