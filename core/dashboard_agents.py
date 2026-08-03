"""A2A Mesh Dashboard — Agents mixin. Wake agent, agent reply, agent card, webhook dispatch."""
import asyncio
import json
import logging

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
        
        # Fetch chat history for context injection (Telegram-group-like session)
        recipient = message.recipient or "broadcast"
        channel = "general" if recipient == "broadcast" else f"dm:{message.sender}"
        chat_history = self._fetch_chat_history(limit=10, channel=channel)
        
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

        # Build list of webhook targets: self + all known peers
        webhook_targets = [
            ("self", self._get_webhook_url()),
        ]
        # Build wake targets: self (CLI) + peers (wake-agent API)
        peer_targets = []
        try:
            for name, peer in self.node.peer_discovery.get_all_peers().items():
                if peer.host and name != self.node.node_name:
                    # Use the peer's health port for wake-agent API
                    # Fallback to 8650 (standard health port) if not set or equals P2P port
                    health_port = peer.health_port or 8650
                    if health_port == peer.p2p_port:
                        health_port = 8650  # P2P and health can't be same port
                    peer_targets.append((name, f"http://{peer.host}:{health_port}/api/wake-agent"))
        except Exception as e:
            log.warning(f"Failed to get peers for wake: {e}")

        total = 1 + len(peer_targets)  # self + peers
        log.info(f"Waking {total} agent(s): self (CLI) + {len(peer_targets)} peers (wake-agent API)")

        # Wake self via CLI (hermes -z with context)
        asyncio.ensure_future(self._wake_self_via_cli(payload, sig, message))

        # Wake peers via wake-agent API (HTTP POST to peer's mesh node)
        for agent_name, wake_url in peer_targets:
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
                    if h_sender.lower() in agent_names or h_sender in ('nova', 'morzsa', 'runa'):
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
            is_human = sender.lower() not in agent_names and sender not in ('nova', 'morzsa', 'runa')
            sender_tag = f"{sender} 👤 emberi felhasználó" if is_human else f"{sender} 🤖 agent"
            
            prompt = (
                f"Te egy A2A Mesh chat résztvevője vagy ({agent_name} 🤖). "
                f"Ez egy közös chat session, mint egy Telegram csoport. "
                f"A chatben emberi felhasználók (👤) és AI agentek (🤖) vesznek részt. "
                f"Az emberi felhasználók (pl. Zsolt) írnak üzeneteket, az agentek válaszolnak. "
                f"Látod a beszélgetés előzményeit és az új üzenetet.\n\n"
                f"── Beszélgetés eddig ──\n{chat_context}\n\n"
                f"── Új üzenet ──\n[{sender_tag}] {content_text[:500]}\n\n"
                f"Válaszolj röviden, természetesen (magyarul, max 500 karakter). "
                f"Ha az üzenet emberi felhasználótól (👤) van, neki válaszolj. "
                f"Ha egy másik agent (🤖) írt és nem hozzád szól, nem kell válaszolnod. "
                f"Ha nem kell válaszolnod, ne küldd el a curl-t.\n\n"
                f"Válaszod elküldéséhez futtasd:\n"
                f"curl -s -X POST {reply_endpoint} -H 'Content-Type: application/json' "
                f"-d '{{\"sender\":\"{agent_name}\",\"content\":\"VÁLASZOD\","
                f"\"recipient\":\"{sender}\",\"priority\":5,\"reply_to\":\"{mesh_msg_id}\"}}'"
            )
            
            log.info(f"Wake-agent prompt for '{agent_name}':\n{prompt[:500]}")
            
            wake_body = json.dumps({
                "mesh_secret": "mesh-wake-secret-2026",
                "agent_name": agent_name,
                "prompt": prompt,
                "reply_endpoint": reply_endpoint,
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
            channel = "general" if recipient == "broadcast" else f"dm:{sender}"
            
            # Skip if sender is self (don't reply to own messages)
            if sender == self.node.node_name:
                log.info(f"Skipping self-wake: message from {sender} (self)")
                return
            
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
            cooldown = getattr(self, '_wake_agent_cooldown', 30.0)
            if elapsed < cooldown:
                remaining = cooldown - elapsed
                log.warning(f"Self-wake rate limited — cooldown {remaining:.0f}s remaining")
                return
            self._last_wake_agent_time = now
            self._wake_agent_in_progress = True
            
            # Run hermes -z (one-shot query) with terminal toolset
            import os as _os
            _hermes_bin = _os.path.expanduser("~/.hermes/hermes-agent/venv/bin/hermes")
            if not _os.path.exists(_hermes_bin):
                _hermes_bin = "hermes"  # fallback to PATH
            _hermes_home = _os.path.expanduser("~/.hermes")
            proc = await aio.create_subprocess_exec(
                _hermes_bin,
                "-z", prompt,
                "-t", "terminal",
                "--yolo",
                stdout=aio.subprocess.PIPE,
                stderr=aio.subprocess.PIPE,
                env={**_os.environ, "HERMES_HOME": _hermes_home},
            )
            
            stdout, stderr = await aio.wait_for(proc.communicate(), timeout=90)
            output = stdout.decode('utf-8', errors='replace') if stdout else ""
            err = stderr.decode('utf-8', errors='replace') if stderr else ""
            
            log.info(f"Nova CLI response ({len(output)} chars): {output[:200]}")
            if err:
                log.warning(f"Nova CLI stderr: {err[:200]}")
                
        except asyncio.TimeoutError:
            log.warning("Nova CLI timed out (90s)")
        except Exception as e:
            log.warning(f"Nova CLI wake failed: {e}")
        finally:
            self._wake_agent_in_progress = False

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
                safe_sender_param = sender.encode("ascii", "replace").decode("ascii") if sender else ""
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

            body = await request.json()
            sender = body.get("sender", "unknown_agent")
            content = body.get("content", "")
            recipient = body.get("recipient", "broadcast")
            priority = int(body.get("priority", 5))
            reply_to = body.get("reply_to", "")  # Original message ID

            if not content.strip():
                return web.json_response({"error": "Empty message"}, status=400)

            from .message import A2AMessage, MSG_TYPE_DIRECTIVE, MSG_TYPE_STEER
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
                },
            )

            # Send via mesh router so all nodes get it
            await self.node.router.send(msg)

            # Insert into mesh_messages for persistence
            await self._insert_mesh_message(msg, auth_user=None)

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

            await self._broadcast_ws({"type": "new_message", "message": msg_dict})

            return web.json_response({"status": "sent", "message_id": msg.id})
        except Exception as e:
            log.error(f"Agent reply failed: {e}")
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
            if self._wake_agent_in_progress:
                log.warning(f"Wake-agent already in progress — skipping (rate limit)")
                return web.json_response({"status": "skipped", "reason": "already_in_progress"}, status=429)
            elapsed = now - self._last_wake_agent_time
            if elapsed < self._wake_agent_cooldown:
                remaining = self._wake_agent_cooldown - elapsed
                log.warning(f"Wake-agent rate limited — cooldown {remaining:.0f}s remaining")
                return web.json_response({"status": "rate_limited", "retry_after": int(remaining)}, status=429)
            self._last_wake_agent_time = now
            self._wake_agent_in_progress = True
            
            # Run hermes -z locally (same as _wake_self_via_cli but on this node)
            import asyncio as aio
            import os
            
            # Find hermes binary
            hermes_bin = os.path.expanduser("~/.hermes/hermes-agent/venv/bin/hermes")
            if not os.path.exists(hermes_bin):
                # Fallback: try PATH
                hermes_bin = "hermes"
            
            hermes_home = os.path.expanduser("~/.hermes")
            
            try:
                proc = await aio.create_subprocess_exec(
                    hermes_bin,
                    "-z", prompt,
                    "-t", "terminal",
                    "--yolo",
                    stdout=aio.subprocess.PIPE,
                    stderr=aio.subprocess.PIPE,
                    env={**os.environ, "HERMES_HOME": hermes_home},
                )
                
                stdout, stderr = await aio.wait_for(proc.communicate(), timeout=90)
                output = stdout.decode('utf-8', errors='replace') if stdout else ""
                err = stderr.decode('utf-8', errors='replace') if stderr else ""
                
                log.info(f"Wake-agent '{agent_name}' CLI response ({len(output)} chars): {output[:200]}")
                if err:
                    log.warning(f"Wake-agent '{agent_name}' stderr: {err[:200]}")
                
                # Send the agent's reply as a mesh message so all dashboards see it
                if output.strip():
                    from .message import A2AMessage, MSG_TYPE_DIRECTIVE
                    reply_msg = A2AMessage(
                        sender=agent_name,
                        recipient="broadcast",
                        type="agent_reply",
                        priority=5,
                        payload={
                            "text": output.strip(),
                            "source": "agent_reply",
                            "username": agent_name,
                            "original_sender": self.node.node_name,
                        },
                    )
                    try:
                        await self.node.router.send(reply_msg)
                        log.info(f"Agent reply from '{agent_name}' sent to mesh ({len(output)} chars)")
                    except Exception as send_err:
                        log.warning(f"Failed to send agent reply to mesh: {send_err}")
                
                return web.json_response({
                    "status": "completed",
                    "agent": agent_name,
                    "output_length": len(output),
                    "output_preview": output[:200],
                })
                
            except asyncio.TimeoutError:
                log.warning(f"Wake-agent '{agent_name}' timed out (90s)")
                return web.json_response({"status": "timeout", "agent": agent_name}, status=504)
            except FileNotFoundError:
                log.error(f"Wake-agent: hermes binary not found at {hermes_bin}")
                self._wake_agent_in_progress = False
                return web.json_response({"error": "Hermes CLI not found"}, status=500)
            except Exception as e:
                log.error(f"Wake-agent CLI failed: {e}")
                self._wake_agent_in_progress = False
                return web.json_response({"error": str(e)}, status=500)
            finally:
                self._wake_agent_in_progress = False
                
        except Exception as e:
            log.error(f"Wake-agent endpoint failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_agent_card(self, request):
        """GET /.well-known/agent-card.json or /api/agent-card — A2A capability discovery.
        
        Returns the agent's capabilities, skills, and metadata following
        the A2A v1.0 agent-card specification. Inspired by gensyn-ai/axl's
        auto-discovery pattern.
        """
        from aiohttp import web
        from ..core.agent_card import build_agent_card
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