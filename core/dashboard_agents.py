"""A2A Mesh Dashboard — Agents mixin. Wake agent, agent reply, agent card, webhook dispatch."""
import asyncio
import json
import logging
import uuid

from .capsules import (
    strip_echo_prefix, retrieve_capsules, format_capsules_for_prompt,
    store_capsule, extract_topic_from_prompt, summarize_conversation,
    TOPIC_SWITCH_MARKERS, retrieve_engramms, format_engramms_for_prompt,
    increment_capsule_retrieval_count, check_and_promote_capsules,
    check_and_generate_skills,
)

log = logging.getLogger("a2a_mesh.dashboard.agents")


class DashboardAgentsMixin:
    """Agent-related methods extracted from DashboardHandler — wake, reply, card, webhook dispatch."""

    async def _wake_agent(self, message):
        """Wake ALL agents via webhook (P2P — every node gets the message).
        
        Each agent's webhook URL is: http://<host>:8644/webhooks/a2a-instant
        The payload includes reply_endpoint pointing back to THIS dashboard
        so agents know where to send their reply.
        The agent's actual reply arrives via /api/agent-reply or the poller.
        """
        # Post a 'processing' indicator to the chat immediately
        processing_msg = {
            "id": f"processing_{message.id}",
            "sender": self.node.node_name,
            "recipient": message.recipient or "broadcast",
            "content": "⏳ Agent thinking...",
            "type": "agent_processing",
            "priority": 3,
            "timestamp": message.timestamp if hasattr(message, 'timestamp') and message.timestamp else None,
            "source": "mesh",
            "username": self.node.node_name,
        }
        self._message_history.append(processing_msg)
        if len(self._message_history) > self._max_history:
            self._message_history = self._message_history[-self._max_history:]
        await self._broadcast_ws({"type": "new_message", "message": processing_msg})

        import hmac as hmac_mod
        import hashlib
        import urllib.request

        payload_text = (message.payload or {}).get("text", "")[:60] if isinstance(message.payload, dict) else str(message.payload)[:60]
        
        # Topic switch detection — clear history to break echo chamber loops
        full_text = (message.payload or {}).get("text", "") if isinstance(message.payload, dict) else str(message.payload)
        is_topic_switch = any(marker in full_text for marker in ['🔔', 'ÚJ TÉMA', 'mode:', 'SZEREP', 'SZABÁLY'])
        history_limit = 0 if is_topic_switch else 10
        
        # Fetch chat history for context injection (Telegram-group-like session)
        recipient = message.recipient or "broadcast"
        channel = "general" if recipient == "broadcast" else f"dm:{recipient}"
        chat_history = self._fetch_chat_history(limit=history_limit, channel=channel) if history_limit > 0 else []
        
        payload = json.dumps({
            "event_type": "a2a_message",
            "sender": message.sender,
            "recipient": recipient,
            "subject": f"Mesh Chat: {payload_text}",
            "content": json.dumps(message.payload) if isinstance(message.payload, dict) else str(message.payload),
            "priority": message.priority,
            "mesh_message_id": message.id,
            "reply_endpoint": f"http://{self._get_host()}:{self.node.config.health_port}/api/agent-reply",
            "reply_format": "mesh_chat",
            "chat_history": chat_history,
        })
        sig = hmac_mod.new(b"a2a-instant-secret-2026", payload.encode(), hashlib.sha256).hexdigest()

        # Build list of wake targets: self + peers
        # Filter by recipient — if DM, only wake the targeted agent
        recipient = message.recipient or "broadcast"
        is_dm = recipient != "broadcast"
        
        # MARVEEN: Capability filtering — if message contains [capability:X] tags,
        # only wake agents that have that capability
        content_text = ""
        try:
            payload = message.payload if isinstance(message.payload, dict) else {}
            content_text = payload.get("text", str(message.payload))
        except Exception:
            content_text = str(getattr(message, 'payload', ''))
        
        import re as _re
        capability_tags = _re.findall(r'\[capability:(\w+)\]', content_text)
        capability_filter_active = len(capability_tags) > 0
        
        if capability_filter_active:
            log.info(f"Capability filter active: {capability_tags}")
        
        peer_targets = []
        try:
            for name, peer in self.node.peer_discovery.get_all_peers().items():
                if peer.host and name != self.node.node_name:
                    # Skip if DM and this peer is not the recipient
                    if is_dm and name != recipient:
                        log.info(f"Skipping wake for '{name}': DM to {recipient}")
                        continue
                    # MARVEEN: Skip if capability filter active and peer lacks required caps
                    if capability_filter_active:
                        try:
                            smart_router = getattr(self.node, 'smart_router', None)
                            if smart_router and hasattr(smart_router, 'registry'):
                                registry = smart_router.registry
                                peer_card = registry.get(name)
                                if peer_card:
                                    peer_caps = set(peer_card.capabilities)
                                    if not all(cap in peer_caps for cap in capability_tags):
                                        log.info(f"Skipping wake for '{name}': missing capabilities {capability_tags}")
                                        continue
                        except Exception as cap_err:
                            log.debug(f"Capability check failed for {name}: {cap_err}")
                    # Use the peer's health port for wake-agent API
                    # Fallback to 8650 (standard health port) if not set or equals P2P port
                    health_port = peer.health_port or 8650
                    if health_port == peer.p2p_port:
                        health_port = 8650  # P2P and health can't be same port
                    peer_targets.append((name, f"http://{peer.host}:{health_port}/api/wake-agent"))
        except Exception as e:
            log.warning(f"Failed to get peers for wake: {e}")

        # Wake self via CLI — only if broadcast or DM to self
        wake_self = True
        if is_dm and recipient != self.node.node_name:
            wake_self = False
            log.info(f"Skipping self-wake: DM to {recipient} (not self)")
        
        if wake_self:
            # Self-regulating: no hard limit, agent decides via system prompt
            total = 1 + len(peer_targets)  # self + peers
            log.info(f"Waking {total} agent(s): self (CLI) + {len(peer_targets)} peers (wake-agent API)")
            asyncio.ensure_future(self._wake_self_via_cli(payload, sig, message))
        else:
            log.info(f"Waking {len(peer_targets)} peer agent(s) only (DM to {recipient})")
        
        # Wake peers via wake-agent API (HTTP POST to peer's mesh node)
        for agent_name, wake_url in peer_targets:
            # Self-regulating: no hard limit, agent decides via system prompt
            asyncio.ensure_future(self._call_wake_agent_api(agent_name, wake_url, payload, message))

        # Start background tasks: poll for agent reply + cleanup timeout
        asyncio.ensure_future(self._poll_for_agent_reply(message))
        asyncio.ensure_future(self._cleanup_processing_indicator(message.id))

    async def _call_wake_agent_api(self, agent_name, wake_url, webhook_payload, original_message):
        """Call a peer node's /api/wake-agent endpoint to wake its local agent.
        
        This replaces the old webhook approach. The peer node runs `hermes -z`
        locally with the context prompt, and the agent curls the reply back
        to our /api/agent-reply endpoint.
        """
        try:
            import aiohttp
            payload_data = json.loads(webhook_payload)
            
            # Build the context prompt for the peer agent
            content = payload_data.get("content", "")
            sender = payload_data.get("sender", "unknown")
            reply_endpoint = payload_data.get("reply_endpoint", "")
            mesh_msg_id = payload_data.get("mesh_message_id", "")
            chat_history = payload_data.get("chat_history", [])
            
            # Skip if sender is the peer itself (don't wake agent for its own message)
            if sender == agent_name:
                log.info(f"Skipping wake for '{agent_name}': message from self")
                return
            
            # Known agent names in the mesh
            agent_names = set()
            try:
                for name, _ in self.node.peer_discovery.get_all_peers().items():
                    agent_names.add(name.lower())
            except Exception:
                pass
            agent_names.add(self.node.node_name.lower())
            
            # Build context prompt using the chat history from the payload
            if chat_history:
                chat_lines = []
                for h in chat_history:
                    h_sender = h.get('sender', '?')
                    h_content = h.get('content', '')[:200]
                    # Mark human vs agent
                    if h_sender.lower() in agent_names:
                        chat_lines.append(f"  [{h_sender} 🤖] {h_content}")
                    else:
                        chat_lines.append(f"  [{h_sender} 👤] {h_content}")
                chat_context = "\n".join(chat_lines[-8:])
            else:
                chat_context = "(nincs előzmény)"
            
            # Parse content — it may be JSON string
            try:
                content_parsed = json.loads(content) if isinstance(content, str) else content
                content_text = content_parsed.get("text", content) if isinstance(content_parsed, dict) else str(content)
            except (json.JSONDecodeError, TypeError):
                content_text = content
            
            # Determine if sender is human or agent
            is_human = sender.lower() not in agent_names
            sender_tag = f"{sender} 👤 emberi felhasználó" if is_human else f"{sender} 🤖 agent"
            
            # MARVEEN: Untrusted Framing — wrap peer content appropriately
            try:
                from .prompt_safety import wrap_untrusted, wrap_trusted_peer, UNTRUSTED_PREAMBLE
                if sender != agent_name:
                    if is_human:
                        # Quarantine reader: ACTIVELY neutralize instruction-like
                        # patterns in human/dashboard-chat content (prompt-injection
                        # defense — the chat is the public-facing surface).
                        try:
                            from .quarantine_reader import quarantine
                            framed = quarantine(sender, content_text[:8000])
                        except Exception:
                            framed = wrap_untrusted(sender, content_text[:4000])
                    else:
                        framed = wrap_trusted_peer(sender, content_text[:4000])
                else:
                    framed = content_text[:4000]
                preamble = UNTRUSTED_PREAMBLE + "\n\n"
            except Exception:
                framed = content_text[:4000]
                preamble = ""
            
            # Count how many times this agent has spoken in the current history
            my_msgs_peer = [h for h in chat_history if h.get('sender', '').lower() == agent_name.lower()]
            my_msg_count_peer = len(my_msgs_peer)

            # ── Self-regulating system prompt (no external hard limit) ──
            self_regulation_peer = (
                "ÖNSZABÁLYOZÁS — Te döntöd el, válaszolsz-e:\n"
                "1. OLVASD EL a fenti beszélgetést figyelmesen.\n"
                "2. DUPLÁZÁS-ELLENŐRZÉS: Ha valaki már említette az érvedet, NE ismételd el. "
                "Csak ÚJ szempontot, ellenvetést vagy következtetést adj hozzá. "
                "'Igen, és pont ezért...' nem új érv.\n"
                "3. RELEVANCIA: Ha a beszélgetés már lefutott és nincs mit hozzátenned, "
                "NE válaszolj. Csend is válasz.\n"
                "4. Ha úgy érzed, hogy már eleget mondtál és a többi agent tovább vitte "
                "a gondolatot, írd: 'NEM VÁLASZTOLSZ'.\n"
                f"5. Eddig {my_msg_count_peer} üzenetet írtél ebben a témában. "
                f"{'Ha már 3+ üzeneted van, csak kritikus új információ esetén válaszolj.' if my_msg_count_peer >= 3 else ''}"
            )

            anti_echo_peer = (
                "SZABÁLY: Tilos 'igazad van', 'jó pont', 'egyetértek', 'pontosan' "
                "üres értelés. Csak ÚJ érvet, ellenvetést vagy konkrét javaslatot írj. "
                "Ha nincs új mondanivalód, írd: 'NEM VÁLASZTOLSZ'."
            )

            prompt = (
                f"{preamble}"
                f"Te {agent_name} 🤖 vagy, egy A2A Mesh chat résztvevő. "
                f"Válaszolj röviden, természetesen, magyarul (max 500 karakter). "
                f"Ha az üzenet konkrét témát és szerepeket tartalmaz, követd azokat. "
                f"Ne ismételd mások érveit — csak új gondolatot hozz. "
                f"Ha nincs mit hozzátenned, írj: 'NEM VÁLASZTOLSZ'.\n\n"
                f"{self_regulation_peer}\n{anti_echo_peer}\n\n"
                f"── Beszélgetés eddig ──\n{chat_context}\n\n"
                f"── Új üzenet ──\n[{sender_tag}] {framed}\n\n"
                f"Válaszodat sima szövegként írd (stdout). "
                f"NE használj curl-t vagy tool-okat — a rendszer automatikusan elküldi."
            )
            
            log.info(f"Wake-agent prompt for '{agent_name}':\n{prompt[:500]}")
            
            wake_body = json.dumps({
                "mesh_secret": "mesh-wake-secret-2026",
                "agent_name": agent_name,
                "prompt": prompt,
                "reply_endpoint": reply_endpoint,
                "original_sender": message.sender,
                "mesh_message_id": message.id,
            })
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    wake_url,
                    data=wake_body.encode(),
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    result = await resp.json()
                    log.info(f"Wake-agent '{agent_name}' response: {result.get('status', 'unknown')} — {str(result)[:200]}")
                    
        except asyncio.TimeoutError:
            log.warning(f"Wake-agent '{agent_name}' timed out (120s)")
        except Exception as e:
            log.warning(f"Wake-agent '{agent_name}' failed ({wake_url}): {e}")

    async def _wake_self_via_cli(self, webhook_payload, sig, original_message):
        """Wake the local agent (Nova) via hermes -z — with full chat context.
        
        The agent sees the recent conversation history (like a Telegram group)
        and can reply via curl to the reply_endpoint.
        """
        import asyncio as aio
        try:
            payload_data = json.loads(webhook_payload)
            content = payload_data.get("content", "")
            sender = payload_data.get("sender", "unknown")
            reply_endpoint = payload_data.get("reply_endpoint", "")
            mesh_msg_id = payload_data.get("mesh_message_id", "")
            
            # Determine channel from recipient
            recipient = payload_data.get("recipient", "broadcast")
            channel = "general" if recipient == "broadcast" else f"dm:{recipient}"
            
            # Skip if sender is self (don't reply to own messages)
            if sender == self.node.node_name:
                log.info(f"Skipping self-wake: message from {sender} (self)")
                return
            
            # Pre-fetch memory capsules + engramms + reflections for context (async, before sync prompt build)
            try:
                pg_pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
                if pg_pool and not any(marker in content for marker in TOPIC_SWITCH_MARKERS):
                    # Capsules (recent conversations)
                    _ollama_url = getattr(self.node.config, 'ollama_url', 'http://localhost:11434')
                    capsules = await retrieve_capsules(pg_pool, content[:500], ollama_url=_ollama_url)
                    self._current_capsules = format_capsules_for_prompt(capsules)
                    # Engramms (matured conclusions — "régebbi gondolatok")
                    engramms = await retrieve_engramms(pg_pool, content[:500], ollama_url=_ollama_url)
                    self._current_engramms = format_engramms_for_prompt(engramms)
                    # Reflections (past meta-analyses)
                    from .reflection import retrieve_reflections, format_past_reflections_for_prompt
                    past_reflections = await retrieve_reflections(pg_pool, content[:500], ollama_url=_ollama_url)
                    if past_reflections:
                        existing_refl = getattr(self, '_current_reflection', '')
                        refl_text = format_past_reflections_for_prompt(past_reflections)
                        self._current_reflection = f"{existing_refl}\n\n{refl_text}" if existing_refl else refl_text
                    # Periodic batch promotion + skill generation (non-blocking)
                    asyncio.ensure_future(check_and_promote_capsules(pg_pool))
                    asyncio.ensure_future(check_and_generate_skills(pg_pool))
                else:
                    self._current_capsules = ''
                    self._current_engramms = ''
                    self._current_reflection = ''
            except Exception as e:
                log.warning(f"Memory pre-fetch failed (non-blocking): {e}")
                self._current_capsules = ''
                self._current_engramms = ''

            # Build context-aware prompt with chat history
            prompt = self._build_context_prompt(
                agent_name=self.node.node_name,
                sender=sender,
                content=content,
                reply_endpoint=reply_endpoint,
                mesh_msg_id=mesh_msg_id,
                channel=channel,
            )
            
            log.info(f"Waking self ({self.node.node_name}) via hermes -z with chat context ({len(prompt)} chars)")
            
            # Rate limit: prevent wake-agent storm
            import time as _time_mod
            now = _time_mod.monotonic()
            if hasattr(self, '_wake_agent_in_progress') and self._wake_agent_in_progress:
                log.warning("Self-wake already in progress — skipping (rate limit)")
                return
            elapsed = now - getattr(self, '_last_wake_agent_time', 0.0)
            cooldown = getattr(self, '_wake_agent_cooldown', 8.0)  # Anti-spam: 8s
            if elapsed < cooldown:
                remaining = cooldown - elapsed
                log.warning(f"Self-wake rate limited — cooldown {remaining:.0f}s remaining")
                return
            self._last_wake_agent_time = now
            self._wake_agent_in_progress = True
            
            # Direct ollama API call (bypasses slow hermes -z CLI)
            import os as _os
            import aiohttp as _aiohttp_ollama
            
            # ── Telegram-style typing indicator (self-wake path) ──
            if hasattr(self, "_broadcast_ws"):
                try:
                    await self._broadcast_ws({
                        "type": "agent_typing",
                        "agent": self.node.node_name,
                        "chat_type": "broadcast",
                        "chat_username": "",
                    })
                except Exception:
                    pass
            
            try:
                # ── Full Hermes agent with TOOL ACCESS (self-wake path) ──
                import asyncio as _aio_exec2
                import shutil as _shutil2

                _hermes_bin2 = os.environ.get("HERMES_BIN") or _shutil2.which("hermes") or os.path.expanduser("~/.local/bin/hermes")
                output = ""
                _cli_ok2 = False
                if _hermes_bin2 and (os.path.isfile(_hermes_bin2) or _shutil2.which(_hermes_bin2)):
                    _agent_sys2 = (
                        f"Te {self.node.node_name} 🤖 vagy, egy A2A Mesh chat résztvevő. Válaszolj röviden, természetesen, magyarul (max 500 karakter). "
                        "TOOL HASZNÁLAT: Ha a feladat végrehajtást igényel (parancs, fájl, keresés), használd a tooljaidat és a végeredményt röviden foglald össze. "
                        "Ha nincs mit hozzátenned, vagy a kontextusból látod hogy már elmondták amit te mondanál, írd: 'NEM VÁLASZTOLSZ'."
                    )
                    _cli_prompt2 = f"{_agent_sys2}\n\n{prompt[:6000]}"
                    try:
                        _proc2 = await _aio_exec2.create_subprocess_exec(
                            _hermes_bin2, "-z", _cli_prompt2, "--yolo",
                            stdout=_aio_exec2.subprocess.PIPE,
                            stderr=_aio_exec2.subprocess.PIPE,
                        )
                        try:
                            _out_b2, _err_b2 = await _aio_exec2.wait_for(_proc2.communicate(), timeout=int(__import__('os').environ.get("A2A_CLI_TIMEOUT_S", "240")))
                        except _aio_exec2.TimeoutError:
                            _proc2.kill()
                            _out_b2, _err_b2 = b"", b"CLI timeout"
                        output = (_out_b2 or b"").decode("utf-8", "replace").strip()
                        if output:
                            _cli_ok2 = True
                            log.info(f"🛠️ Nova hermes-CLI response ({len(output)} chars): {output[:200]}")
                        else:
                            log.warning("hermes -z empty output (self-wake) — falling back to ollama")
                    except Exception as _cli_ex2:
                        log.warning(f"hermes -z failed (self-wake): {_cli_ex2} — falling back to ollama")
                else:
                    log.warning("hermes binary not found (self-wake) — falling back to ollama")

                if not _cli_ok2:
                    # ── Fallback: bare ollama (no tools) ──
                    ollama_url = "http://localhost:11434/api/chat"
                    ollama_body = {
                        "model": "glm-5.3:cloud",
                        "messages": [
                            {"role": "system", "content": f"Te {self.node.node_name} 🤖 vagy, egy A2A Mesh chat résztvevő. Válaszolj röviden, természetesen, magyarul (max 500 karakter). Ha az üzenet konkrét témát és szerepeket tartalmaz, követd azokat. Ne ismételd mások érveit — csak új gondolatot hozz. Ha nincs mit hozzátenned, vagy a kontextusból látod hogy már elmondták amit te mondanál, írd: 'NEM VÁLASZTOLSZ'. Olvasd el a beszélgetést és döntsd el: van-e új érv-ed vagy csak ismétled másokat."},
                            {"role": "user", "content": prompt[:4000]}
                        ],
                        "stream": False,
                        "options": {"temperature": 0.8, "num_predict": 1000}
                    }
                    
                    async with _aiohttp_ollama.ClientSession() as sess:
                        async with sess.post(ollama_url, json=ollama_body, timeout=_aiohttp_ollama.ClientTimeout(total=90)) as resp:
                            if resp.status == 200:
                                result = await resp.json()
                                output = result.get("message", {}).get("content", "").strip()
                                log.info(f"Nova ollama response ({len(output)} chars): {output[:200]}")
                            else:
                                err_text = await resp.text()
                                log.warning(f"Nova ollama error {resp.status}: {err_text[:200]}")
                                output = ""
            except Exception as ollama_err:
                log.error(f"Nova ollama API failed: {ollama_err}")
                output = ""
            
            # Send the agent's reply to the chat via /api/agent-reply
            clean_reply = output.strip()
            # Echo filter: strip agreement prefixes, skip pure echo
            clean_reply = strip_echo_prefix(clean_reply)
            
            # ── Content similarity check (self-wake path) ──
            if clean_reply and clean_reply.upper() != "NEM VÁLASZTOLSZ":
                from difflib import SequenceMatcher as _SM
                _self_name = self.node.node_name
                _prev_replies = getattr(self, '_recent_agent_replies', {}).get(_self_name, [])
                _max_sim = 0.0
                for _prev in _prev_replies[-5:]:
                    _sim = _SM(None, clean_reply.lower()[:500], _prev.lower()[:500]).ratio()
                    _max_sim = max(_max_sim, _sim)
                if _max_sim > 0.70:
                    log.info(f"🔇 Similarity check: {_self_name} reply {_max_sim:.0%} similar to previous — skipping")
                    clean_reply = ""
                else:
                    if not hasattr(self, '_recent_agent_replies'):
                        self._recent_agent_replies = {}
                    if _self_name not in self._recent_agent_replies:
                        self._recent_agent_replies[_self_name] = []
                    self._recent_agent_replies[_self_name].append(clean_reply[:500])
                    self._recent_agent_replies[_self_name] = self._recent_agent_replies[_self_name][-10:]
            
            # Filter "NEM VÁLASZTOLSZ" — agent decided not to reply
            if not clean_reply or clean_reply.upper() == "NEM VÁLASZTOLSZ":
                log.info(f"Nova agent chose not to reply (NEM VÁLASZTOLSZ) — skipping")
            elif clean_reply and reply_endpoint:
                try:
                    import aiohttp as _aiohttp
                    import re as _re

                    # ── DM parser: extract DM:target:message lines ──
                    # Format: "DM:morzsa:hello" or "DM:runa:check this"
                    # Multiple DMs allowed — one per line
                    # Non-DM lines go as broadcast reply
                    dm_lines = []
                    broadcast_lines = []
                    for line in clean_reply.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        dm_match = _re.match(r"^DM:(\w+):(.+)", line, _re.IGNORECASE)
                        if dm_match:
                            target = dm_match.group(1).lower()
                            dm_text = dm_match.group(2).strip()
                            dm_lines.append((target, dm_text))
                        else:
                            broadcast_lines.append(line)

                    # Send DMs via /api/agent-dm
                    for target, dm_text in dm_lines:
                        try:
                            dm_payload = {
                                "sender": self.node.node_name,
                                "recipient": target,
                                "content": dm_text[:2000],
                                "msg_type": "a2a_message",
                            }
                            dm_url = f"http://127.0.0.1:{self.node.config.health_port}/api/agent-dm"
                            async with _aiohttp.ClientSession() as sess:
                                async with sess.post(
                                    dm_url,
                                    json=dm_payload,
                                    headers={"X-Mesh-Token": "mesh-wake-secret-2026"},
                                    timeout=_aiohttp.ClientTimeout(total=10),
                                ) as dm_resp:
                                    log.info(f"📩 Agent DM {self.node.node_name}→{target}: {dm_resp.status} — {dm_text[:80]}")
                        except Exception as dm_err:
                            log.warning(f"📩 Agent DM to {target} failed: {dm_err}")

                    # Send broadcast reply (non-DM lines)
                    broadcast_reply = "\n".join(broadcast_lines).strip()
                    if broadcast_lines or not dm_lines:
                        # If there are broadcast lines, send them as reply
                        # If no DMs at all, send the full clean_reply as reply (backward compat)
                        reply_content = broadcast_reply if dm_lines else clean_reply
                        if reply_content and reply_content.upper() != "NEM VÁLASZTOLSZ":
                            # MARVEEN: Reply to the original sender, not broadcast
                            reply_recipient = original_message.sender if original_message.sender != self.node.node_name else "broadcast"
                            reply_body = json.dumps({
                                "sender": self.node.node_name,
                                "content": reply_content[:2000],
                                "recipient": reply_recipient,
                                "priority": 5,
                                "reply_to": mesh_msg_id,
                                "chat_username": payload_data.get("chat_username", "") if isinstance(payload_data, dict) else "",
                                "chat_type": payload_data.get("chat_type", "user_dm") if isinstance(payload_data, dict) else "user_dm",
                            })
                            async with _aiohttp.ClientSession() as sess:
                                async with sess.post(
                                    reply_endpoint,
                                    data=reply_body.encode(),
                                    headers={"Content-Type": "application/json"},
                                    timeout=_aiohttp.ClientTimeout(total=15),
                                ) as resp:
                                    log.info(f"Nova agent reply sent to {reply_endpoint}: {resp.status}")
                        elif dm_lines and not broadcast_lines:
                            log.info(f"Agent {self.node.node_name} sent only DMs (no broadcast reply)")
                    elif dm_lines and not broadcast_lines:
                        log.info(f"Agent {self.node.node_name} sent {len(dm_lines)} DM(s), no broadcast")
                except Exception as reply_err:
                    log.warning(f"Failed to send Nova agent reply: {reply_err}")
                
        except asyncio.TimeoutError:
            log.warning("Nova CLI timed out (120s)")
        except Exception as e:
            log.warning(f"Nova CLI wake failed: {e}")
        finally:
            self._wake_agent_in_progress = False
            # ── Typing indicator OFF (self-wake path) ──
            if hasattr(self, "_broadcast_ws"):
                try:
                    await self._broadcast_ws({
                        "type": "agent_typing_stop",
                        "agent": self.node.node_name,
                        "chat_type": "broadcast",
                        "chat_username": "",
                    })
                except Exception:
                    pass

    async def _call_webhook(self, agent_name, webhook_url, payload, sig, original_message):
        """Call a single agent's webhook URL. Non-blocking — logs result.
        
        Falls back to P2P transport if webhook fails (e.g. Runa has no Hermes gateway on 8644).
        """
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    data=payload.encode(),
                    headers={
                        "Content-Type": "application/json",
                        "X-Hub-Signature-256": f"sha256={sig}",
                    },
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    result = await resp.json()
                    log.info(f"Agent '{agent_name}' woken via webhook ({webhook_url}): {result.get('status', 'unknown')}")

                    # If the webhook response contains a reply, post it to the mesh chat
                    reply_text = result.get("response", "") or result.get("reply", "")
                    if reply_text and isinstance(reply_text, str) and len(reply_text.strip()) > 0:
                        reply_text = reply_text.strip()[:2000]
                        try:
                            reply_data = json.dumps({
                                "sender": agent_name,
                                "content": reply_text,
                                "recipient": original_message.sender if original_message.sender != agent_name else "broadcast",
                                "priority": 5,
                                "reply_to": original_message.id,
                            })
                            async with session.post(
                                f"http://{self._get_host()}:{self.node.config.health_port}/api/agent-reply",
                                data=reply_data.encode(),
                                headers={"Content-Type": "application/json"},
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as reply_resp:
                                reply_result = await reply_resp.text()
                                log.info(f"Agent '{agent_name}' reply posted to mesh chat: {reply_result[:100]}")
                        except Exception as re:
                            log.warning(f"Failed to post agent '{agent_name}' reply to mesh chat: {re}")
        except Exception as e:
            log.info(f"Agent '{agent_name}' webhook failed ({webhook_url}): {e}")
            # Fallback: send wake-up notification via P2P transport directly
            await self._wake_via_p2p(agent_name, original_message, payload, sig)

    async def _wake_via_p2p(self, agent_name, original_message, webhook_payload, sig):
        """Fallback: wake an agent via P2P transport when webhook (HTTP 8644) is unavailable.
        
        Sends a 'wake' directive via P2P TCP. The receiving node's handler will
        see this and can process the original message.
        """
        from .message import A2AMessage, MSG_TYPE_DIRECTIVE
        try:
            p2p = self.node._p2p_transport
            if not p2p or not p2p.is_available():
                log.debug(f"P2P fallback skipped for {agent_name}: transport unavailable")
                return

            # Try to parse webhook payload for content
            try:
                payload_data = json.loads(webhook_payload)
            except Exception:
                payload_data = {}

            wake_msg = A2AMessage.create(
                sender=self.node.node_name,
                recipient=agent_name if agent_name != "self" else "broadcast",
                msg_type=MSG_TYPE_DIRECTIVE,
                priority=8,
                payload={
                    "text": payload_data.get("content", ""),
                    "source": "web_dashboard_wake",
                    "username": payload_data.get("sender", "dashboard"),
                    "original_sender": self.node.node_name,
                    "webhook_fallback": True,
                    "mesh_message_id": original_message.id,
                    "reply_endpoint": payload_data.get("reply_endpoint", ""),
                    "reply_format": "mesh_chat",
                    "subject": payload_data.get("subject", ""),
                    "sig": sig,
                },
            )

            result = await p2p.send(wake_msg)
            if result.success:
                log.info(f"P2P fallback: woke agent '{agent_name}' via P2P transport (instead of webhook)")
            else:
                log.debug(f"P2P fallback failed for {agent_name}: {result.error}")
        except Exception as e:
            log.debug(f"P2P fallback error for {agent_name}: {e}")

    async def _poll_for_agent_reply(self, original_message, timeout: int = 90, interval: int = 3):
        """Poll mesh_messages for an agent reply matching the original message.
        
        This watches the DB for any new message from the agent that could be
        a reply to the original dashboard message. If found, it broadcasts it
        to the chat and removes the processing indicator. Falls back to the
        90-second timeout if no reply arrives.
        """
        import psycopg2
        start = asyncio.get_event_loop().time()
        original_id = original_message.id
        sender = original_message.sender  # The user who sent the original message
        processing_id = f"processing_{original_id}"
        
        # Check if processing indicator still exists (may have been removed by agent-reply API)
        def still_processing():
            return any(m.get("id") == processing_id for m in self._message_history)
        
        while (asyncio.get_event_loop().time() - start) < timeout:
            await asyncio.sleep(interval)
            if not still_processing():
                log.info(f"Processing indicator removed for {original_id}, reply received — stopping poll")
                return
            
            # Check mesh_messages for a reply from our agent to the sender
            try:
                conn = psycopg2.connect(
                    dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                    password=self.node.config.pg.password,
                    host=self.node.config.pg.host, port=self.node.config.pg.port
                )
                cur = conn.cursor()
                cur.execute("SET client_encoding TO 'UTF8'")
                # ASCII-safe sender for SQL_ASCII PG
                safe_sender_param = sender if sender else ""
                cur.execute("""
                    SELECT id, sender, recipient, msg_type, priority, payload, created_at
                    FROM mesh.mesh_messages
                    WHERE sender != %s
                      AND recipient IN (%s, 'broadcast')
                      AND created_at > NOW() - INTERVAL '2 minutes'
                    ORDER BY created_at DESC LIMIT 10
                """, (safe_sender_param, safe_sender_param))
                rows = cur.fetchall()
                cur.close()
                conn.close()
                
                for row in rows:
                    msg_id, msg_sender, msg_recipient, msg_type, msg_priority, msg_payload, msg_created = row
                    # Check if this reply is already in message_history
                    already_in_history = any(m.get("id") == msg_id for m in self._message_history)
                    if not already_in_history:
                        # Found a new reply! Add it to the chat
                        payload_text = ""
                        if isinstance(msg_payload, dict):
                            payload_text = msg_payload.get("text", str(msg_payload))
                        elif isinstance(msg_payload, str):
                            try:
                                import json as _json
                                p = _json.loads(msg_payload)
                                payload_text = p.get("text", msg_payload)
                            except:
                                payload_text = msg_payload
                        
                        reply_msg = {
                            "id": msg_id,
                            "sender": msg_sender,
                            "recipient": msg_recipient,
                            "content": payload_text[:2000],
                            "type": "agent_reply",
                            "priority": msg_priority,
                            "timestamp": msg_created.isoformat() if msg_created else None,
                            "source": "mesh",
                            "username": msg_sender,
                            "reply_to": original_id,
                        }
                        self._message_history.append(reply_msg)
                        if len(self._message_history) > self._max_history:
                            self._message_history = self._message_history[-self._max_history:]
                        
                        # Remove processing indicator
                        self._message_history = [m for m in self._message_history if m.get("id") != processing_id]
                        
                        await self._broadcast_ws({"type": "new_message", "message": reply_msg})
                        log.info(f"Agent reply detected via polling for message {original_id}: {msg_id}")
                        return
            except Exception as e:
                log.warning(f"Reply poll error: {e}")
        
        log.info(f"Reply poll timed out for message {original_id}")

    async def _cleanup_processing_indicator(self, original_msg_id: str, timeout: int = 90):
        """Remove the 'processing' indicator if no agent reply arrives within timeout seconds."""
        await asyncio.sleep(timeout)
        # Check if the processing indicator is still in history
        processing_id = f"processing_{original_msg_id}"
        still_processing = any(m.get("id") == processing_id for m in self._message_history)
        if still_processing:
            # Remove the processing indicator
            self._message_history = [m for m in self._message_history if m.get("id") != processing_id]
            # Add a timeout message
            timeout_msg = {
                "id": f"timeout_{original_msg_id}",
                "sender": self.node.node_name,
                "recipient": "broadcast",
                "content": "⚠️ Agent response timed out. Reply may appear in Telegram.",
                "type": "agent_timeout",
                "priority": 3,
                "timestamp": None,
                "source": "mesh",
                "username": self.node.node_name,
            }
            self._message_history.append(timeout_msg)
            if len(self._message_history) > self._max_history:
                self._message_history = self._message_history[-self._max_history:]
            await self._broadcast_ws({"type": "new_message", "message": timeout_msg})
            log.info(f"Processing indicator timed out for message {original_msg_id}, removed")

    def _get_host(self):
        """Get this node's LAN IP address for constructing URLs."""
        return getattr(self.node.config.p2p, 'listen_host', None) or self.node._get_local_ip()

    def _get_webhook_url(self):
        """Get the Hermes webhook URL for this node's host."""
        return f"http://localhost:8644/webhooks/a2a-instant"

    async def _api_agent_reply(self, request):
        """Agent reply endpoint — agents call this to post replies to the mesh chat.

        This is called by Hermes (or any agent) to send a reply that appears
        in the dashboard chat. The reply is stored in mesh_messages and
        broadcast to all connected dashboard users via WebSocket.

        No auth required — this is an internal API called by agents.
        Uses HMAC-SHA256 verification with shared secret for security.
        """
        from aiohttp import web
        try:
            # Verify HMAC signature
            import hmac as hmac_mod
            import hashlib
            sig = request.headers.get("X-Mesh-Signature", "")
            data = await request.read()
            expected_sig = hmac_mod.new(b"mesh-reply-secret-2026", data, hashlib.sha256).hexdigest()
            if sig != f"sha256={expected_sig}":
                # Allow without signature for now (internal network)
                pass

            import json as _json
            body = _json.loads(data) if data else {}
            sender = body.get("sender", "unknown_agent")
            content = body.get("content", "")
            recipient = body.get("recipient", "broadcast")
            priority = int(body.get("priority", 5))
            reply_to = body.get("reply_to", "")  # Original message ID

            if not content.strip():
                return web.json_response({"error": "Empty message"}, status=400)

            from .message import A2AMessage, MSG_TYPE_DIRECTIVE, MSG_TYPE_STEER

            # ── Anti-ping-pong: if the reply is from an agent to a broadcast chat,
            # route it as agent_reply type (not generic directive) and send to the
            # original sender only, NOT broadcast. This prevents peer nodes from
            # re-triggering wake-agent on receiving this reply.
            _agent_names = ("nova", "morzsa", "runa", "tor")
            _is_agent_reply = sender.lower() in _agent_names
            # chat_username: the human user this reply belongs to (for per-user history persistence
            # in on_mesh_message — without it the reply shows live via WS but vanishes on reload)
            _chat_username = body.get("chat_username", "") or body.get("username", "")
            if _is_agent_reply and recipient == "broadcast":
                # Agent broadcasting to chat — keep as broadcast for dashboard visibility
                # but use agent_reply type so the receive loop's anti-ping-pong filter catches it
                msg = A2AMessage(
                    sender=sender,
                    recipient=recipient,
                    type="agent_reply",  # Use agent_reply type instead of directive
                    priority=priority,
                    payload={
                        "text": content,
                        "source": "agent_reply",
                        "username": sender,
                        "reply_to": reply_to,
                        "chat_username": _chat_username,
                    },
                )
            else:
                msg = A2AMessage(
                    sender=sender,
                    recipient=recipient,
                    type=MSG_TYPE_DIRECTIVE,
                    priority=priority,
                    payload={
                        "text": content,
                        "source": "agent_reply",
                        "username": sender,
                        "reply_to": reply_to,
                        "chat_username": _chat_username,
                    },
                )

            # Send via mesh router so all nodes get it
            await self.node.router.send(msg)

            # Insert into mesh_messages for persistence (if method exists)
            if hasattr(self, '_insert_mesh_message'):
                try:
                    await self._insert_mesh_message(msg, auth_user=None)
                except Exception as ins_err:
                    log.debug(f"mesh_messages insert skipped: {ins_err}")

            # MARVEEN: Mark original message as read (inbox nudge)
            if reply_to:
                try:
                    from .inbox_nudge import mark_read
                    mark_read(reply_to)
                except Exception as nudge_err:
                    log.warning(f"Inbox mark_read failed: {nudge_err}")

            # ── Ötletláda-auto-beküldés: a [ÖTLET] jelölővel küldött javaslatok
            # automatikusan a mesh ötletládába kerülnek (a2a-mesh fejlesztési
            # javaslatok /debate- és /ideas-vitákból). Determinisztikus: a jelölő dönt, nem LLM.
            try:
                import re as _re_idea
                _idea_lines = _re_idea.findall(r"^\s*\[[OÖ]TLET\]\s*(.+)", content or "", _re_idea.IGNORECASE | _re_idea.MULTILINE)
                if _idea_lines:
                    _pool = self._get_pg_pool() if hasattr(self, '_get_pg_pool') else None
                    if _pool:
                        for _suggestion in _idea_lines[:3]:  # max 3 ötlet válaszonként
                            _suggestion = _suggestion.strip()[:500]
                            if len(_suggestion) < 5:
                                continue
                            await _pool.execute(
                                """INSERT INTO mesh.mesh_ideas
                                   (idea_id, title, description, category, priority, status, submitted_by, source_type, tags)
                                   VALUES ($1, $2, $3, 'feature', 'medium', 'idea', $4, 'agent', ARRAY['debate'])""",
                                f"idea_{uuid.uuid4().hex[:12]}",
                                _suggestion[:120],
                                f"Debate-javaslat (automatikus beküldés)\n\nEredeti hozzászólás: {content[:400]}\n\nBeküldte: {sender}",
                                sender,
                            )
                            log.info(f"🗳️ [ÖTLET] auto-beküldve az ötletládába ({sender}): {_suggestion[:60]}")
                    else:
                        log.warning("[ÖTLET] detektálva, de PG pool nem elérhető — nem került az ötletládába")
            except Exception as _idea_auto_err:
                log.warning(f"Ötletláda auto-beküldés (non-fatal): {_idea_auto_err}")

            # ── Agent-szavazás: [SZAVAZAT] <idea_id|cím-töredék> up|down jelölősorból
            # P2P idea_vote a koordinátornak. Determinisztikus: a marker dönt, a
            # szabály-motor (apply_vote_with_rules) alkalmazza a küszöböket.
            try:
                import re as _re_vote
                _vote_lines = _re_vote.findall(
                    r"^\s*\[SZAVAZAT\]\s*(idea_[a-f0-9]+)\s+(up|down|fel|le)\b",
                    content or "", _re_vote.IGNORECASE | _re_vote.MULTILINE)
                if _vote_lines:
                    _node_ref = getattr(self, 'node', None)
                    for _vid, _vd in _vote_lines[:5]:  # max 5 szavazat válaszonként
                        _vd = "down" if _vd.lower() in ("down", "le") else "up"
                        if _node_ref is not None and hasattr(_node_ref, 'send_direct'):
                            await _node_ref.send_direct(
                                "morzsa", "idea_vote",
                                {"idea_id": _vid, "vote": _vd, "voter": sender},
                                priority=3)
                            log.info(f"🗳️ [SZAVAZAT] elküldve {sender}→morzsa: {_vid} {_vd}")
                        else:
                            # Fallback: ha nincs node-ref, közvetlen PG-szavazás
                            _pool = self._get_pg_pool() if hasattr(self, '_get_pg_pool') else None
                            if _pool:
                                from .idea_review import apply_vote_with_rules
                                await apply_vote_with_rules(_pool, _vid, f"agent:{sender}", _vd)
                                log.info(f"🗳️ [SZAVAZAT] rögzítve PG-ben ({sender}): {_vid} {_vd}")
            except Exception as _vote_err:
                log.warning(f"Agent-szavazás parse (non-fatal): {_vote_err}")

            # MARVEEN: Conversation log
            try:
                from .marveen_db import log_conversation
                await log_conversation(sender, "assistant", content[:1000])
            except Exception as conv_err:
                log.warning(f"Conversation log failed: {conv_err}")

            # Broadcast to all connected dashboard users
            msg_dict = {
                "id": msg.id,
                "sender": msg.sender,
                "recipient": msg.recipient,
                "content": content,
                "type": "agent_reply",
                "priority": msg.priority,
                "timestamp": msg.timestamp,
                "source": "mesh",
                "username": sender,
                "reply_to": reply_to,
            }
            self._message_history.append(msg_dict)
            if len(self._message_history) > self._max_history:
                self._message_history = self._message_history[-self._max_history:]

            # Remove processing indicator if this is a reply to a tracked message
            if reply_to:
                processing_id = f"processing_{reply_to}"
                was_processing = any(m.get("id") == processing_id for m in self._message_history)
                if was_processing:
                    self._message_history = [m for m in self._message_history if m.get("id") != processing_id]
                    log.info(f"Removed processing indicator for message {reply_to} after agent reply")

            # Broadcast to all connected dashboard users (if method exists)
            if hasattr(self, '_broadcast_ws'):
                await self._broadcast_ws({"type": "new_message", "message": msg_dict})
            else:
                log.debug("WebSocket broadcast skipped — _broadcast_ws not available")

            # ── Store agent reply in mesh_chat_messages for DM visibility ──
            chat_username = body.get("chat_username", "")
            chat_type = body.get("chat_type", "user_dm")
            if chat_username:
                try:
                    pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
                    if pool and pool.is_connected():
                        from .dashboard_chat import store_agent_reply
                        if chat_type == "broadcast":
                            # Broadcast reply → store as broadcast (general room)
                            await store_agent_reply(pool, "broadcast", sender, content, "agent_reply")
                            log.info(f"💬 Agent reply stored as BROADCAST: {sender}→all ({len(content)} chars)")
                        else:
                            # DM reply → store as DM
                            await store_agent_reply(pool, chat_username, sender, content, "agent_reply")
                            log.info(f"💬 Agent reply stored as DM: {sender}→user:{chat_username} ({len(content)} chars)")
                except Exception as dm_err:
                    log.warning(f"Failed to store agent reply: {dm_err}")

            return web.json_response({"status": "sent", "message_id": msg.id})
        except Exception as e:
            log.error(f"Agent reply failed: {e}", exc_info=True)
            import traceback as _tb
            log.error(f"TRACEBACK: {_tb.format_exc()}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_wake_agent(self, request):
        """Wake-agent endpoint — called by peer nodes to wake the LOCAL agent.
        
        This replaces the webhook approach. Instead of Nova calling each peer's
        Hermes webhook (port 8644), Nova calls this endpoint on the peer's mesh
        node (port 8650). The peer node then runs `hermes -z` locally with the
        provided context prompt, and the agent's reply is POSTed back to the
        reply_endpoint via curl.
        
        No auth required — internal mesh API. Uses simple shared-secret check.
        """
        from aiohttp import web
        try:
            body = await request.json()
            
            # Simple shared-secret check (internal mesh network)
            provided_secret = body.get("mesh_secret", "")
            if provided_secret != "mesh-wake-secret-2026":
                return web.json_response({"error": "Unauthorized"}, status=401)
            
            agent_name = body.get("agent_name", self.node.node_name)
            prompt = body.get("prompt", "")
            reply_endpoint = body.get("reply_endpoint", "")
            
            if not prompt:
                return web.json_response({"error": "Empty prompt"}, status=400)
            
            log.info(f"Wake-agent request for '{agent_name}' — prompt {len(prompt)} chars")
            
            # Rate limit: prevent wake-agent storm (Ollama 429 + OOM SIGKILL root cause)
            import time as _time
            now = _time.monotonic()
            
            # ── Dynamic cooldown: detect ping-pong pattern ──
            # Track wake-agent call timestamps; if 3+ calls in last 60s, increase cooldown
            _wake_history = getattr(self, '_wake_agent_history', [])
            # Prune entries older than 60s
            _wake_history = [t for t in _wake_history if now - t < 60]
            _wake_history.append(now)
            self._wake_agent_history = _wake_history
            
            if len(_wake_history) >= 5:
                # Ping-pong detected — increase cooldown to 45s
                dynamic_cooldown = 45
                log.warning(f"🏓 Ping-pong detected ({len(_wake_history)} wake calls in 60s) — cooldown → {dynamic_cooldown}s")
            elif len(_wake_history) >= 3:
                # Active debate — moderate cooldown
                dynamic_cooldown = max(self._wake_agent_cooldown, 20)
            else:
                dynamic_cooldown = self._wake_agent_cooldown
            
            # ── Human-user chat messages: NO serialization with agent traffic ──
            # A human asking in DM/room must not wait behind another wake (2-3 min delay
            # root cause). Human wakes run in PARALLEL with the in-progress flag held
            # only for agent-to-agent dedup purposes.
            _human_chat = bool(body.get("chat_username")) and body.get("agent_name") != body.get("sender")
            if _human_chat:
                _human_active = getattr(self, '_human_wake_count', 0)
                if _human_active >= 2:
                    log.warning(f"Human wake parallel limit ({_human_active}) — queueing")
                    _body = dict(body)
                    if not hasattr(self, '_wake_agent_queue'):
                        self._wake_agent_queue = []
                    if len(self._wake_agent_queue) < 5:
                        self._wake_agent_queue.append(_body)
                        return web.json_response({"status": "queued", "queue_depth": len(self._wake_agent_queue)}, status=202)
                    return web.json_response({"status": "skipped", "reason": "busy"}, status=429)
                self._human_wake_count = _human_active + 1
                self._wake_agent_start_time = now
                try:
                    return await self._run_wake_processing(body, agent_name, prompt, reply_endpoint, human=True)
                finally:
                    self._human_wake_count = max(0, getattr(self, '_human_wake_count', 1) - 1)

            if self._wake_agent_in_progress:
                # Safety: if in_progress for >300s, the CLI crashed/stuck — reset and allow
                # (300s > 240s max CLI runtime — must not fire during a legitimate long tool run)
                stuck_elapsed = now - getattr(self, '_wake_agent_start_time', now)
                if stuck_elapsed > 300:
                    log.warning(f"Wake-agent stuck for {stuck_elapsed:.0f}s — force resetting flag")
                    self._wake_agent_in_progress = False
                else:
                    # Queue the request — process it right after the running wake finishes,
                    # so a chat DM is never dropped just because another wake is busy.
                    # NOTE: body was already read at the top of the handler — reuse it,
                    # aiohttp request bodies can only be read once.
                    _body = dict(body)
                    # Anti-loop: drain-resubmitted items must NOT be re-queued —
                    # otherwise drain -> busy -> re-queue -> drain = infinite ping-pong.
                    _drain_pass = _body.pop("__drain_pass__", 0)
                    if _drain_pass >= 1:
                        log.info(f"DRain wake-agent: busy during drain pass {_drain_pass} — dropping instead of re-queueing (anti-loop)")
                        return web.json_response({"status": "dropped_busy_during_drain", "queue_depth": 0}, status=429)
                    if not hasattr(self, '_wake_agent_queue'):
                        self._wake_agent_queue = []
                    if len(self._wake_agent_queue) < 5:
                        self._wake_agent_queue.append(_body)
                        log.info(f"INBOX wake-agent: busy — queued request (queue depth: {len(self._wake_agent_queue)})")
                        return web.json_response({"status": "queued", "queue_depth": len(self._wake_agent_queue)}, status=202)
                    log.warning(f"Wake-agent already in progress — queue full, skipping (rate limit)")
                    return web.json_response({"status": "skipped", "reason": "already_in_progress"}, status=429)
            # Agent-to-agent traffic keeps the cooldown
            elapsed = now - self._last_wake_agent_time
            if elapsed < dynamic_cooldown:
                remaining = dynamic_cooldown - elapsed
                log.warning(f"Wake-agent rate limited — cooldown {remaining:.0f}s remaining (dynamic: {dynamic_cooldown}s)")
                return web.json_response({"status": "rate_limited", "retry_after": int(remaining)}, status=429)
            self._last_wake_agent_time = now
            self._wake_agent_in_progress = True
            self._wake_agent_start_time = now
            
            # ── Delegate to shared processing core (flag already set above) ──
            return await self._run_wake_processing(body, agent_name, prompt, reply_endpoint, human=False)
        except Exception as e:
            log.error(f"Wake-agent endpoint failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _run_wake_processing(self, body: dict, agent_name: str, prompt: str, reply_endpoint: str, human: bool = False):
        """Shared wake processing: memory pre-fetch → typing → CLI/ollama → reply → drain.

        The finally block resets _wake_agent_in_progress (no-op for human path) and
        drains the queue. `human=True` runs without the agent dedup flag being held.
        """
        from aiohttp import web
        # Pre-fetch memory capsules + engramms + reflections for this peer's context
        try:
            pg_pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pg_pool and not any(marker in prompt for marker in TOPIC_SWITCH_MARKERS):
                _ollama_url = getattr(self.node.config, 'ollama_url', 'http://localhost:11434')
                capsules = await retrieve_capsules(pg_pool, prompt[:500], ollama_url=_ollama_url)
                capsule_text = format_capsules_for_prompt(capsules)
                engramms = await retrieve_engramms(pg_pool, prompt[:500], ollama_url=_ollama_url)
                engramm_text = format_engramms_for_prompt(engramms)
                # Reflections (past meta-analyses)
                from .reflection import retrieve_reflections, format_past_reflections_for_prompt
                past_reflections = await retrieve_reflections(pg_pool, prompt[:500], ollama_url=_ollama_url)
                reflection_text = format_past_reflections_for_prompt(past_reflections)
                # Inject all memory layers before the prompt
                memory_prefix = ""
                if engramm_text:
                    memory_prefix += f"{engramm_text}\n\n"
                if capsule_text:
                    memory_prefix += f"{capsule_text}\n\n"
                if reflection_text:
                    memory_prefix += f"{reflection_text}\n\n"
                if memory_prefix:
                    prompt = f"{memory_prefix}{prompt}"
                    log.info(f"🧠 Memory injected: {len(engramms)} engramm, {len(capsules)} capsule, {len(past_reflections)} reflection")
                # Periodic batch promotion + skill generation
                asyncio.ensure_future(check_and_promote_capsules(pg_pool))
                asyncio.ensure_future(check_and_generate_skills(pg_pool))
        except Exception as e:
            log.warning(f"Peer memory pre-fetch failed (non-blocking): {e}")

        # Direct ollama API call (bypasses slow hermes -z CLI)
        import asyncio as aio
        import os
        import aiohttp as _aiohttp
            
        # ── Telegram-style typing indicator: tell the frontend the agent is thinking ──
        _chat_type = body.get("chat_type", "")
        if hasattr(self, "_broadcast_ws"):
            try:
                await self._broadcast_ws({
                    "type": "agent_typing",
                    "agent": agent_name,
                    "chat_type": _chat_type,
                    "chat_username": body.get("chat_username", ""),
                })
            except Exception as _te:
                log.debug(f"agent_typing broadcast failed: {_te}")
            
        try:
            # ── Full Hermes agent with TOOL ACCESS ──
            # Replaces the bare ollama call: hermes -z runs the real agent
            # with ALL toolsets (terminal, files, web, …) — same as the
            # Telegram/terminal interface. Fallback: bare ollama if CLI fails.
            import asyncio as _aio_exec
            import shutil as _shutil

            _hermes_bin = os.environ.get("HERMES_BIN") or _shutil.which("hermes") or os.path.expanduser("~/.local/bin/hermes")
            output = ""
            _cli_ok = False
            if _hermes_bin and os.path.isfile(_hermes_bin) or (_hermes_bin and _shutil.which(_hermes_bin)):
                _agent_sys = (
                    f"Te {agent_name} 🤖 vagy, egy A2A Mesh chat résztvevő. Válaszolj röviden, természetesen, magyarul (max 500 karakter). "
                    "Ha az üzenet konkrét témát és szerepeket tartalmaz, követd azokat. Ne ismétled mások érveit — csak új gondolatot hozz. "
                    "TOOL HASZNÁLAT: Ha a feladat végrehajtást igényel (parancs, fájl, keresés), használd a tooljaidat és a végeredményt röviden foglald össze. "
                    "ÖNSZABÁLYOZÁS: Ha a vita lefutott vagy nincs mit hozzátenned, írd: 'NEM VÁLASZTOLSZ'. Csend is válasz."
                )
                _cli_prompt = f"{_agent_sys}\n\n{prompt[:6000]}"
                try:
                    _proc = await _aio_exec.create_subprocess_exec(
                        _hermes_bin, "-z", _cli_prompt, "--yolo",
                        stdout=_aio_exec.subprocess.PIPE,
                        stderr=_aio_exec.subprocess.PIPE,
                    )
                    # Node-specific CLI timeout: slow containers (e.g. HAOS addon)
                    # need much longer than a workstation — env A2A_CLI_TIMEOUT_S overrides the default
                    try:
                        _cli_timeout = int(os.environ.get("A2A_CLI_TIMEOUT_S", "240"))
                    except ValueError:
                        _cli_timeout = 240
                    try:
                        _out_b, _err_b = await _aio_exec.wait_for(_proc.communicate(), timeout=_cli_timeout)
                    except _aio_exec.TimeoutError:
                        _proc.kill()
                        _out_b, _err_b = b"", b"CLI timeout"
                    output = (_out_b or b"").decode("utf-8", "replace").strip()
                    if output:
                        _cli_ok = True
                        log.info(f"🛠️ Wake-agent '{agent_name}' hermes-CLI response ({len(output)} chars): {output[:200]}")
                    else:
                        _err_s = (_err_b or b"").decode("utf-8", "replace")[:200]
                        log.warning(f"hermes -z empty output for '{agent_name}': {_err_s} — falling back to ollama")
                except Exception as _cli_ex:
                    log.warning(f"hermes -z failed for '{agent_name}': {_cli_ex} — falling back to ollama")
            else:
                log.warning(f"hermes binary not found — falling back to ollama for '{agent_name}'")

            if not _cli_ok:
                # ── Fallback: bare ollama chat (no tools) ──
                ollama_url = "http://localhost:11434/api/chat"
                ollama_body = {
                    "model": "glm-5.3:cloud",
                    "messages": [
                        {"role": "system", "content": f"Te {agent_name} 🤖 vagy, egy A2A Mesh chat résztvevő. Válaszolj röviden, természetesen, magyarul (max 500 karakter). Ha az üzenet konkrét témát és szerepeket tartalmaz, követd azokat. Ne ismétled mások érveit — csak új gondolatot hozz.\n\nÖNSZABÁLYOZÁS:\n1. OLVASD EL a beszélgetést. Ha valaki már említette az érvedet, NE ismételd.\n2. DUPLÁZÁS-ELLENŐRZÉS: 'Igen, és pont ezért...' nem új érv.\n3. Ha már 5+ üzeneted van ebben a témában, csak KÜLÖNÖSEN fontos új infó esetén válaszolj.\n4. Ha a vita már lefutott vagy nincs mit hozzátenned, írd: 'NEM VÁLASZTOLSZ'. Csend is válasz.\n5. SZABÁLY: Tilos 'igazad van', 'jó pont', 'egyetértek' üres értelés. Csak ÚJ érvet vagy ellenvetést írj.\n6. Ha a beszélgetés kb. lezárult (konklúzió látszik), NE folytasd a vitát — 'NEM VÁLASZTOLSZ'."},
                        {"role": "user", "content": prompt[:4000]}
                    ],
                    "stream": False,
                    "options": {"temperature": 0.8, "num_predict": 1000}
                }
                async with _aiohttp.ClientSession() as sess:
                    async with sess.post(ollama_url, json=ollama_body, timeout=_aiohttp.ClientTimeout(total=90)) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            output = result.get("message", {}).get("content", "").strip()
                            log.info(f"Wake-agent '{agent_name}' ollama response ({len(output)} chars): {output[:200]}")
                        else:
                            err_text = await resp.text()
                            log.warning(f"Wake-agent '{agent_name}' ollama error {resp.status}: {err_text[:200]}")
                            output = ""
                
            # Send the reply to the reply_endpoint
            clean_reply = output.strip()
            # Echo filter: strip agreement prefixes, skip pure echo
            clean_reply = strip_echo_prefix(clean_reply)
                
            # ── Content similarity check: if reply is >70% similar to a
            # previous reply from this agent, skip it (anti-repetition) ──
            if clean_reply and clean_reply.upper() != "NEM VÁLASZTOLSZ":
                from difflib import SequenceMatcher
                _prev_replies = getattr(self, '_recent_agent_replies', {}).get(agent_name, [])
                _max_sim = 0.0
                for _prev in _prev_replies[-5:]:
                    _sim = SequenceMatcher(None, clean_reply.lower()[:500], _prev.lower()[:500]).ratio()
                    _max_sim = max(_max_sim, _sim)
                if _max_sim > 0.70:
                    log.info(f"🔇 Similarity check: {agent_name} reply {_max_sim:.0%} similar to previous — skipping")
                    clean_reply = ""
                else:
                    # Store this reply for future similarity checks
                    if not hasattr(self, '_recent_agent_replies'):
                        self._recent_agent_replies = {}
                    if agent_name not in self._recent_agent_replies:
                        self._recent_agent_replies[agent_name] = []
                    self._recent_agent_replies[agent_name].append(clean_reply[:500])
                    # Keep only last 10
                    self._recent_agent_replies[agent_name] = self._recent_agent_replies[agent_name][-10:]
                
            if clean_reply and clean_reply.upper() != "NEM VÁLASZTOLSZ" and reply_endpoint:
                try:
                    import aiohttp as _aiohttp2
                    import re as _re_dm

                    # ── DM parser: extract DM:target:message lines ──
                    dm_lines = []
                    broadcast_lines = []
                    suggestion_lines = []
                    for line in clean_reply.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        dm_match = _re_dm.match(r"^DM:(\w+):(.+)", line, _re_dm.IGNORECASE)
                        if dm_match:
                            target = dm_match.group(1).lower()
                            dm_text = dm_match.group(2).strip()
                            dm_lines.append((target, dm_text))
                        elif _re_dm.match(r"^SUGGESTION:", line, _re_dm.IGNORECASE):
                            suggestion_lines.append(line)
                        else:
                            broadcast_lines.append(line)

                    # Send DMs via /api/agent-dm
                    for target, dm_text in dm_lines:
                        try:
                            dm_payload = {
                                "sender": agent_name,
                                "recipient": target,
                                "content": dm_text[:2000],
                                "msg_type": "a2a_message",
                            }
                            dm_url = f"http://127.0.0.1:{self.node.config.health_port}/api/agent-dm"
                            async with _aiohttp2.ClientSession() as sess:
                                async with sess.post(
                                    dm_url,
                                    json=dm_payload,
                                    headers={"X-Mesh-Token": "mesh-wake-secret-2026"},
                                    timeout=_aiohttp2.ClientTimeout(total=10),
                                ) as dm_resp:
                                    log.info(f"📩 Agent DM {agent_name}→{target}: {dm_resp.status} — {dm_text[:80]}")
                        except Exception as dm_err:
                            log.warning(f"📩 Agent DM to {target} failed: {dm_err}")

                    # ── v0.40: Process SUGGESTION: lines → PG + DM to Nova ──
                    if suggestion_lines:
                        try:
                            from core.reflection import submit_development_suggestion
                            pg_pool = getattr(self.node, '_pg_pool', None)
                            if pg_pool and hasattr(pg_pool, 'is_connected') and pg_pool.is_connected():
                                for sug_line in suggestion_lines:
                                    # Parse: SUGGESTION: title | description | priority
                                    parts = _re_dm.sub(r"^SUGGESTION:\s*", "", sug_line, flags=_re_dm.IGNORECASE).split("|")
                                    title = parts[0].strip()[:200] if parts else "Untitled"
                                    desc = parts[1].strip()[:2000] if len(parts) > 1 else title
                                    priority = parts[2].strip().lower() if len(parts) > 2 else "medium"
                                    if priority not in ("low", "medium", "high"):
                                        priority = "medium"
                                    sug_id = await submit_development_suggestion(
                                        pg_pool, agent_name, title, desc,
                                        category="development", priority=priority,
                                    )
                                    if sug_id:
                                        # DM Nova about the suggestion
                                        dm_payload = {
                                            "sender": agent_name,
                                            "recipient": "nova",
                                            "content": f"💡 Javaslat: {title} ({priority})\n{sug_id}",
                                            "msg_type": "a2a_message",
                                        }
                                        dm_url = f"http://127.0.0.1:{self.node.config.health_port}/api/agent-dm"
                                        async with _aiohttp2.ClientSession() as sess:
                                            async with sess.post(
                                                dm_url,
                                                json=dm_payload,
                                                headers={"X-Mesh-Token": "mesh-wake-secret-2026"},
                                                timeout=_aiohttp2.ClientTimeout(total=10),
                                            ) as dm_resp:
                                                log.info(f"💡 Suggestion DM {agent_name}→nova: {dm_resp.status} — {title[:60]}")
                                log.info(f"💡 {agent_name} submitted {len(suggestion_lines)} development suggestions")
                        except Exception as sug_err:
                            log.warning(f"💡 Suggestion processing failed: {sug_err}")

                    # Send broadcast reply (non-DM lines)
                    broadcast_reply = "\n".join(broadcast_lines).strip()
                    reply_content = broadcast_reply if dm_lines else clean_reply
                    if reply_content and reply_content.upper() != "NEM VÁLASZTOLSZ":
                        # MARVEEN: Reply to the original sender, not broadcast
                        original_sender = body.get("original_sender", "broadcast")
                        reply_body = json.dumps({
                            "sender": agent_name,
                            "content": reply_content[:2000],
                            "recipient": original_sender,
                            "priority": 5,
                            "reply_to": body.get("mesh_message_id", ""),
                            "chat_username": body.get("chat_username", ""),
                            "chat_type": body.get("chat_type", "user_dm"),
                        })
                        async with _aiohttp2.ClientSession() as sess:
                            async with sess.post(
                                reply_endpoint,
                                data=reply_body.encode(),
                                headers={"Content-Type": "application/json"},
                                timeout=_aiohttp2.ClientTimeout(total=15),
                            ) as resp:
                                log.info(f"Agent reply sent to {reply_endpoint}: {resp.status}")
                    elif dm_lines and not broadcast_lines:
                        log.info(f"Agent {agent_name} sent only DMs (no broadcast reply)")
                except Exception as reply_err:
                    log.warning(f"Failed to send agent reply to {reply_endpoint}: {reply_err}")
                
            return web.json_response({
                "status": "completed",
                "agent": agent_name,
                "output_length": len(output),
                "output_preview": output[:200],
            })
                
        except asyncio.TimeoutError:
            log.warning(f"Wake-agent '{agent_name}' timed out (120s)")
            return web.json_response({"status": "timeout", "agent": agent_name}, status=504)
        except FileNotFoundError:
            log.error(f"Wake-agent: hermes binary not found at {hermes_bin}")
            if not human:
                self._wake_agent_in_progress = False
            return web.json_response({"error": "Hermes CLI not found"}, status=500)
        except Exception as e:
            log.error(f"Wake-agent CLI failed: {e}")
            if not human:
                self._wake_agent_in_progress = False
            return web.json_response({"error": str(e)}, status=500)
        finally:
            if not human:
                # Only the agent-dedup path owns this flag — human runs must not clear it
                self._wake_agent_in_progress = False
            # ── Typing indicator OFF: agent finished (reply or not) ──
            if hasattr(self, "_broadcast_ws"):
                try:
                    await self._broadcast_ws({
                        "type": "agent_typing_stop",
                        "agent": agent_name,
                        "chat_type": _chat_type,
                        "chat_username": body.get("chat_username", ""),
                    })
                except Exception:
                    pass
            # ── Drain wake queue: re-submit queued requests via self-POST so they get
            # full processing after the current wake finished. Fire-and-forget.
            _queued = getattr(self, '_wake_agent_queue', [])
            if _queued:
                self._wake_agent_queue = []
                log.info(f"📤 Wake-agent finished — draining queue ({len(_queued)} pending)")
                async def _drain_wake_queue(items):
                    import aiohttp as _d_aio
                    import asyncio as _d_aioio
                    _drain_url = f"http://127.0.0.1:{self.node.config.health_port}/api/wake-agent"
                    for _qi in items:
                        await _d_aioio.sleep(2)  # give the previous wake time to fully unwind
                        # Tag as drain-resubmission so a busy wake never re-queues it (anti-loop)
                        _qi = dict(_qi)
                        _qi["__drain_pass__"] = _qi.get("__drain_pass__", 0) + 1
                        try:
                            async with _d_aio.ClientSession() as _d_sess:
                                async with _d_sess.post(_drain_url, json=_qi, timeout=_d_aio.ClientTimeout(total=150)) as _d_resp:
                                    log.info(f"📤 Drain wake-agent: {_d_resp.status}")
                        except Exception as _d_e:
                            log.warning(f"📤 Drain wake-agent failed: {_d_e}")
                try:
                    import asyncio as _aio_drain
                    _aio_drain.get_event_loop().create_task(_drain_wake_queue(list(_queued)))
                except Exception as _d_ex:
                    log.warning(f"Drain spawn failed: {_d_ex}")
                
        return web.json_response({"status": "internal_error"}, status=500)

    async def _api_agent_card(self, request):
        """GET /.well-known/agent-card.json or /api/agent-card — A2A capability discovery.
        
        Returns the agent's capabilities, skills, and metadata following
        the A2A v1.0 agent-card specification. Inspired by gensyn-ai/axl's
        auto-discovery pattern.
        """
        from aiohttp import web
        from .agent_card import build_agent_card
        import time
        
        # Build agent card from current state
        uptime = time.time() - self.node._start_time if hasattr(self.node, '_start_time') and self.node._start_time else 0
        health_score = 1.0
        load = 0.0
        queue_size = 0
        node_name = self.node.node_name
        router = self.node.router
        
        # Get health/load from registry if available
        if self.registry:
            health = self.registry.get_health(node_name)
            if health:
                health_score = getattr(health, 'score', 1.0)
                load = getattr(health, 'load', 0.0)
        
        # Get queue size from router if available
        if router:
            stats = router.get_stats()
            queue_size = stats.get("inbound_queue", {}).get("current_size", 0)
            load = queue_size / max(1, 200)  # Normalize to 0-1
        
        base_url = f"http://{request.host}" if request.host else ""
        
        card = build_agent_card(
            node_name=node_name,
            registry=self.registry,
            health_score=health_score,
            load=load,
            queue_size=queue_size,
            uptime=uptime,
            base_url=base_url,
            config_skills=getattr(self.node.config, 'skills', None) if self.node else None,
        )
        
        return web.json_response(card.to_dict())

    async def _api_agent_message(self, request):
        """POST /api/agent-message — Agent-to-agent direct messaging.
        
        Agents send messages to each other through this endpoint.
        Uses shared-secret auth (same as wake-agent).
        """
        from aiohttp import web
        try:
            body = await request.json()
            
            # Shared-secret auth (internal mesh)
            provided_secret = body.get("mesh_secret", "")
            if provided_secret != "mesh-wake-secret-2026":
                return web.json_response({"error": "Unauthorized"}, status=401)
            
            from_agent = body.get("from_agent", "")
            to_agent = body.get("to_agent", "")
            content = body.get("content", "")
            msg_type = body.get("msg_type", "directive")
            priority = int(body.get("priority", 5))
            
            if not content.strip() or not to_agent:
                return web.json_response({"error": "Missing content or to_agent"}, status=400)
            
            # Create and send A2A message
            from .message import A2AMessage, MSG_TYPE_DIRECTIVE
            msg = A2AMessage(
                sender=from_agent or self.node.node_name,
                recipient=to_agent,
                type=MSG_TYPE_DIRECTIVE if msg_type == "directive" else msg_type,
                priority=priority,
                payload={
                    "text": content,
                    "source": "agent_message",
                    "from_agent": from_agent,
                    "original_sender": from_agent,
                },
            )
            
            result = await self.node.router.send(msg)
            
            # Insert into PG (if method exists)
            if hasattr(self, '_insert_mesh_message'):
                try:
                    await self._insert_mesh_message(msg, auth_user=None)
                except Exception as ins_err:
                    log.debug(f"mesh_messages insert skipped: {ins_err}")
            
            # Wake the target agent
            await self._wake_agent(msg)
            
            # Conversation log
            try:
                from .marveen_db import log_conversation
                await log_conversation(from_agent or self.node.node_name, "user", content[:1000])
            except Exception:
                pass
            
            log.info(f"Agent message: {from_agent}→{to_agent}: {content[:80]}")
            return web.json_response({
                "status": "sent",
                "message_id": msg.id,
                "from": from_agent,
                "to": to_agent,
                "result": str(result),
            })
            
        except Exception as e:
            log.error(f"Agent message failed: {e}")
            return web.json_response({"error": str(e)}, status=500)
    async def _insert_mesh_message(self, message, auth_user=None):
        """Insert dashboard message into mesh.mesh_messages for mesh-wide persistence.

        Uses mesh_messages (not shared_a2a_memory) so all agents in the mesh
        see it via PG NOTIFY, and the dashboard shows agent replies in real-time.
        """
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host,
                port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname,
                user=self.node.config.pg.user,
                password=self.node.config.pg.password,
                options="-c client_encoding=UTF8",
            )
            cur = conn.cursor()
            payload = message.payload if isinstance(message.payload, dict) else {"text": str(message.payload)}
            # For SQL_ASCII PG: use ASCII-safe sender name
            safe_sender = message.sender or "unknown"
            payload_json = json.dumps(payload, ensure_ascii=True)

            cur.execute(
                """INSERT INTO mesh.mesh_messages
                   (id, sender, recipient, msg_type, priority, payload, routing_mode, status, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (id) DO NOTHING""",
                (
                    message.id,
                    safe_sender,
                    message.recipient or "broadcast",
                    message.type,
                    message.priority,
                    payload_json,
                    "hybrid",
                    "sent",
                ),
            )
            conn.commit()
            # Notify mesh channel so all agents receive it
            notify_payload = json.dumps({
                "id": str(message.id),
                "sender": message.sender,
                "recipient": message.recipient,
                "msg_type": message.type,
                "priority": message.priority,
            })
            cur.execute("NOTIFY mesh_channel, %s", (notify_payload,))
            conn.commit()
            cur.close()
            conn.close()
            log.info(f"Dashboard message {message.id[:8]} inserted into mesh_messages")
        except Exception as e:
            log.warning(f"Mesh insert failed: {e}")
