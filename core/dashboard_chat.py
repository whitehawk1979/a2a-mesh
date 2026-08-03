"""A2A Mesh Dashboard — Chat mixin. Messages, WebSocket, chat history, memory endpoints."""
import asyncio
import json
import logging
import time

log = logging.getLogger("a2a_mesh.dashboard.chat")


class DashboardChatMixin:
    async def _api_messages(self, request):
        """Return recent messages — local history + PG messages from other agents.

        Supports channel filtering via ?channel=general|dm:<agent_name>
        - general: broadcast messages (recipient=broadcast)
        - dm:morzsa: direct messages with morzsa (sender=morzsa OR recipient=morzsa)
        """
        from aiohttp import web
        import traceback as tb
        try:
            limit = min(int(request.query.get("limit", 50)), 200)
            channel = request.query.get("channel", None)
            log.info(f"_api_messages called: limit={limit}, channel={channel}")
            
            # Local messages — deep normalize to prevent type issues
            local_messages = self._message_history[-limit:]
            log.info(f"Raw local messages: {len(local_messages)}")
            
            # CRITICAL: Ensure all messages are plain dicts with string values
            safe_local = []
            for i, m in enumerate(local_messages):
                try:
                    if not isinstance(m, dict):
                        log.warning(f"  local[{i}] is NOT a dict: type={type(m).__name__}")
                        continue
                    # Convert id to string if not None
                    mid = m.get("id")
                    if mid is not None:
                        m["id"] = str(mid)
                    else:
                        m["id"] = f"local_{i}"
                    # Convert timestamp to string if not None
                    mts = m.get("timestamp")
                    if mts is None:
                        m["timestamp"] = ""
                    elif not isinstance(mts, str):
                        m["timestamp"] = str(mts)
                    safe_local.append(m)
                except Exception as e:
                    log.warning(f"  local[{i}] normalize error: {e}")
            log.info(f"Safe local messages: {len(safe_local)}")

            # Use normalized local messages
            local_messages = safe_local

            # Also fetch recent messages from PG (other agents' responses)
            pg_messages = []
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=self.node.config.pg.host,
                    port=self.node.config.pg.port,
                    dbname=self.node.config.pg.dbname,
                    user=self.node.config.pg.user,
                    password=self.node.config.pg.password,
                )
                cur = conn.cursor()
                cur.execute("SET client_encoding TO UTF8")

                # Build WHERE clause based on channel filter
                where_clauses = []
                params = []

                # Exclude non-chat messages from chat view
                where_clauses.append("msg_type NOT IN ('heartbeat', 'memory_sync', 'diagnostic_report', 'skills_announcement', 'config_suggestion', 'ack', 'peer_offline', 'peer_online', 'node_join', 'node_leave')")

                if channel == "general":
                    # General chat: broadcast messages + agent replies (even if recipient=nova)
                    # Agent replies have recipient=sender but should appear in general chat (Telegram-like)
                    where_clauses.append("(recipient = 'broadcast' OR msg_type IN ('agent_reply', 'directive'))")
                elif channel and channel.startswith("dm:"):
                    # DM with specific agent
                    dm_agent = channel[3:]
                    where_clauses.append("(recipient = %s OR sender = %s)")
                    params.extend([dm_agent, dm_agent])

                where_sql = " AND ".join(where_clauses)

                # For SQL_ASCII PG: try reading payload, fallback to skipping bad rows
                try:
                    cur.execute(f"""
                        SELECT id, sender, recipient, msg_type, priority, payload, created_at, status
                        FROM mesh.mesh_messages
                        WHERE {where_sql}
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, params + [limit])
                    raw_rows = cur.fetchall()
                except Exception as pg_err:
                    log.warning(f"PG query failed ({pg_err}), using local messages only")
                    raw_rows = []
                
                rows = []
                for row in raw_rows:
                    msg_id, sender, recipient, msg_type, priority, payload, created_at, status = row
                    # Handle SQL_ASCII encoding: try to decode payload safely
                    if isinstance(payload, bytes):
                        payload = payload.decode("utf-8", errors="replace")
                    elif isinstance(payload, str):
                        try:
                            payload.encode("ascii")  # test if pure ASCII
                        except UnicodeEncodeError:
                            # Non-ASCII bytes in SQL_ASCII field — re-interpret as UTF-8
                            try:
                                payload = payload.encode("latin-1").decode("utf-8")
                            except (UnicodeDecodeError, UnicodeEncodeError):
                                payload = payload.encode("ascii", "replace").decode("ascii")
                    rows.append((msg_id, sender, recipient, msg_type, payload, created_at, status))
                
                for row in rows:
                    msg_id, sender, recipient, msg_type, priority, payload, created_at, status = row
                    # Parse payload — already decoded above for SQL_ASCII handling
                    import json
                    try:
                        payload_data = json.loads(payload) if isinstance(payload, str) else payload
                    except (json.JSONDecodeError, TypeError):
                        payload_data = {"text": str(payload)}
                    
                    pg_messages.append({
                        "id": str(msg_id),
                        "sender": sender,
                        "recipient": recipient,
                        "type": msg_type,
                        "priority": priority,
                        "content": payload_data.get("text", "") if isinstance(payload_data, dict) else str(payload),
                        "username": payload_data.get("username", sender) if isinstance(payload_data, dict) else sender,
                        "timestamp": created_at.isoformat() if created_at else None,
                        "status": status,
                        "source": "mesh",
                    })
                cur.close()
                conn.close()
            except Exception as e:
                log.warning(f"Failed to fetch PG messages: {e}")
            
            # Filter local messages by channel and type
            def matches_channel(msg: dict, ch: str | None) -> bool:
                # Exclude heartbeat and system messages from chat
                msg_type = msg.get("type", "")
                if msg_type in ("heartbeat", "memory_sync"):
                    return False
                # Allow agent_processing indicators
                if msg_type == "agent_processing":
                    return True
                # Allow agent_timeout indicators
                if msg_type == "agent_timeout":
                    return True
                if ch is None:
                    return True
                recip = msg.get("recipient", "broadcast")
                sender = msg.get("sender", "")
                if ch == "general":
                    # General channel: broadcast + agent replies/directives (Telegram-like)
                    return recip == "broadcast" or msg_type in ("agent_reply", "directive")
                elif ch.startswith("dm:"):
                    agent = ch[3:]
                    return sender == agent or recip == agent
                return True

            filtered_local = [m for m in local_messages if matches_channel(m, channel)]

            # Merge local + PG messages, deduplicate by ID
            # Use `or ""` to handle None values — .get() returns None when key exists with None value
            all_messages = {m.get("id") or f"local_{i}": m for i, m in enumerate(filtered_local)}
            for m in pg_messages:
                msg_id = m.get("id") or ""
                if msg_id and msg_id not in all_messages:
                    all_messages[msg_id] = m

            # Filter out agent_reply messages with heartbeat-like payload (uptime/transports only)
            def _is_heartbeat_reply(m):
                if m.get("type") != "agent_reply":
                    return False
                content = m.get("content", "")
                if isinstance(content, str):
                    content = content.strip()
                    # Check if content is a JSON dict with only uptime/transports keys
                    if content.startswith("{") and content.endswith("}"):
                        try:
                            import json as _json
                            data = _json.loads(content)
                            if isinstance(data, dict) and set(data.keys()) <= {"uptime", "transports"}:
                                return True
                        except (_json.JSONDecodeError, TypeError):
                            pass
                return False

            msg_list = [m for m in all_messages.values() if not _is_heartbeat_reply(m)]
            for m in msg_list:
                ts = m.get("timestamp")
                if ts is None or not isinstance(ts, str):
                    m["timestamp"] = str(ts) if ts is not None else ""
            msg_list.sort(key=lambda m: m.get("timestamp", "") or "")
            result = msg_list[-limit:]
            
            return web.json_response({"messages": result, "total": len(msg_list)})
        except Exception as e:
            log.error(f"Error in _api_messages: {e}\n{tb.format_exc()}")
            return web.json_response({"error": str(e), "traceback": tb.format_exc()}, status=500)

    async def _api_messages_incoming(self, request):
        """GET /api/messages/incoming — Return messages from other mesh agents.

        Query params:
          since: Unix timestamp — only return messages after this time (default: 0)
          limit: Max messages to return (default: 50, max: 200)
          sender: Filter by sender name (optional)
        """
        from aiohttp import web
        import time as _time
        try:
            since = float(request.query.get("since", 0))
            limit = min(int(request.query.get("limit", 50)), 200)
            sender_filter = request.query.get("sender", None)

            messages = []
            for m in self._message_history:
                try:
                    if not isinstance(m, dict):
                        continue
                    # Only include messages FROM other agents (not from self)
                    msg_sender = m.get("sender", "")
                    msg_recipient = m.get("recipient", "")
                    msg_time = m.get("timestamp", 0)
                    if isinstance(msg_time, str):
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(msg_time.replace("Z", "+00:00"))
                            msg_time = dt.timestamp()
                        except Exception:
                            msg_time = 0

                    # Filter by time
                    if msg_time and msg_time < since:
                        continue

                    # Filter by sender
                    if sender_filter and msg_sender != sender_filter:
                        continue

                    # Only include messages from other mesh nodes (not from self or web_user)
                    local_name = self.node.node_name
                    if msg_sender == local_name or msg_sender == "web_user":
                        continue
                    if msg_sender in ("system", ""):
                        continue

                    safe_msg = {}
                    for k, v in m.items():
                        if v is None:
                            safe_msg[k] = None
                        elif isinstance(v, (bool, int, float, str)):
                            safe_msg[k] = v
                        else:
                            safe_msg[k] = str(v)

                    safe_msg["sender"] = msg_sender
                    safe_msg["recipient"] = msg_recipient
                    safe_msg["timestamp"] = msg_time
                    messages.append(safe_msg)
                except Exception:
                    continue

            messages = messages[-limit:]
            return web.json_response({"messages": messages, "count": len(messages)})
        except Exception as e:
            from aiohttp import web
            return web.json_response({"error": str(e)}, status=500)

    async def _api_send(self, request):
        """Send a message to the mesh from the dashboard.

        Messages stay in the mesh — no webhook redirect to other platforms.
        All connected agents see messages in real-time via WebSocket broadcast.
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        recipient = data.get("recipient", "")
        content = data.get("content", "")
        msg_type = data.get("type", "message")
        priority = int(data.get("priority", 5))

        if not content.strip():
            return web.json_response({"error": "Empty message"}, status=400)

        from .message import A2AMessage, MSG_TYPE_DIRECTIVE, MSG_TYPE_STEER
        # For broadcast, use "broadcast" so all agents receive it
        effective_recipient = recipient if recipient else "broadcast"
        msg = A2AMessage(
            sender=user.display_name or "web_user",
            recipient=effective_recipient,
            type=msg_type if msg_type != "message" else MSG_TYPE_DIRECTIVE,
            priority=priority,
            payload={
                "text": content,
                "source": "web_dashboard",
                "username": user.display_name,
                "user_id": user.user_id,
                "original_sender": self.node.node_name,
            },
        )

        result = await self.node.router.send(msg)

        # Insert into PG for mesh-wide persistence (mesh_messages, not shared_a2a_memory)
        await self._insert_mesh_message(msg, user)

        # Always wake agent for dashboard messages (user is waiting for reply)
        await self._wake_agent(msg)

        self._message_history.append({
            "id": msg.id,
            "sender": msg.sender,
            "recipient": msg.recipient,
            "content": content,
            "type": msg_type,
            "priority": msg.priority,
            "timestamp": msg.timestamp,
            "source": "web_dashboard",
            "username": user.display_name,
            "result": str(result),
        })
        if len(self._message_history) > self._max_history:
            self._message_history = self._message_history[-self._max_history:]

        await self._broadcast_ws({
            "type": "new_message",
            "message": self._message_history[-1],
        })

        return web.json_response({
            "status": "sent",
            "message_id": msg.id,
            "result": str(result),
        })

    # ─── WebSocket handler ───

    async def _websocket_handler(self, request):
        """WebSocket handler for real-time dashboard updates."""
        from aiohttp import web
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Auth: check token from query param
        token = request.query.get("token", "")
        auth_user = None
        if token:
            auth_user = self.auth.verify_token(token)

        user_id = str(uuid.uuid4())[:8]
        username = auth_user.display_name if auth_user else (request.query.get("username", f"guest_{user_id}"))
        user = DashboardUser(user_id=user_id, username=username, websocket=ws)
        self._users[user_id] = user

        log.info(f"Dashboard user connected: {username} ({user_id}) auth={'yes' if auth_user else 'no'}")

        # Send initial data — include agents list so frontend can populate DM channels immediately
        try:
            # Build agents list for the connected message
            agents_data = []
            status = self.node.get_status()
            raw_transports = status.get("transports", {})
            transport_inner = raw_transports
            if isinstance(raw_transports, dict) and "transports" in raw_transports:
                transport_inner = raw_transports["transports"]
            self_transports = {}
            for key in ("p2p", "pg", "pg_notify", "http", "ble"):
                val = transport_inner.get(key, False)
                if isinstance(val, str) and "available=True" in val:
                    self_transports[key] = True
                elif isinstance(val, str) and "available=False" in val:
                    self_transports[key] = False
                elif isinstance(val, bool):
                    self_transports[key] = val
                elif hasattr(val, "available"):
                    self_transports[key] = val.available
                else:
                    self_transports[key] = bool(val)
            agents_data.append({
                "name": self.node.node_name,
                "role": self.node.config.topology.node_role,
                "status": "online",
                "transports": {
                    "p2p": self_transports.get("p2p", False),
                    "pg": self_transports.get("pg_notify", self_transports.get("pg", False)),
                    "http": self_transports.get("http", False),
                },
            })
            for name, peer in self.node.peer_discovery.get_all_peers().items():
                if peer.p2p_available and peer.pg_available:
                    peer_status = "online"
                elif peer.p2p_available:
                    peer_status = "available"
                else:
                    peer_status = "offline"
                agents_data.append({
                    "name": peer.name,
                    "role": peer.role,
                    "status": peer_status,
                    "transports": {
                        "p2p": peer.p2p_available,
                        "pg": peer.pg_available,
                        "http": peer.http_available,
                    },
                })
            await ws.send_json({
                "type": "connected",
                "user_id": user_id,
                "username": username,
                "node": self.node.node_name,
                "authenticated": auth_user is not None,
                "role": auth_user.role if auth_user else "guest",
                "agents": agents_data,
            })
            await ws.send_json({"type": "status", "data": status})
        except Exception:
            pass

        # Listen for messages from client
        try:
            async for msg in ws:
                if msg.type == 1:  # TEXT
                    try:
                        data = json.loads(msg.data)
                        msg_type = data.get("type", "")

                        if msg_type == "chat":
                            # Require auth for sending messages
                            if not auth_user:
                                await ws.send_json({"type": "error", "message": "Authentication required to send messages"})
                                continue

                            content = data.get("content", "")
                            recipient = data.get("recipient", "")
                            priority = int(data.get("priority", 5))

                            from .message import A2AMessage, MSG_TYPE_DIRECTIVE, MSG_TYPE_STEER
                            # Broadcast to all agents in the mesh
                            effective_recipient = recipient if recipient else "broadcast"
                            a2a_msg = A2AMessage(
                                sender=auth_user.display_name or "web_user",
                                recipient=effective_recipient,
                                type=MSG_TYPE_DIRECTIVE,
                                priority=priority,
                                payload={
                                    "text": content,
                                    "source": "web_dashboard",
                                    "username": auth_user.display_name,
                                    "user_id": auth_user.user_id,
                                    "original_sender": self.node.node_name,
                                },
                            )
                            result = await self.node.router.send(a2a_msg)

                            # Insert into mesh_messages for mesh-wide persistence
                            await self._insert_mesh_message(a2a_msg, auth_user)

                            # Always wake agent for dashboard messages (user is waiting for reply)
                            await self._wake_agent(a2a_msg)

                            self._message_history.append({
                                "id": a2a_msg.id,
                                "sender": a2a_msg.sender,
                                "recipient": a2a_msg.recipient,
                                "content": content,
                                "type": "message",
                                "priority": a2a_msg.priority,
                                "timestamp": a2a_msg.timestamp,
                                "source": "web_dashboard",
                                "username": auth_user.display_name,
                            })
                            if len(self._message_history) > self._max_history:
                                self._message_history = self._message_history[-self._max_history:]

                            await self._broadcast_ws({
                                "type": "new_message",
                                "message": self._message_history[-1],
                            })

                        elif msg_type == "ping":
                            await ws.send_json({"type": "pong", "timestamp": time.time()})
                    except json.JSONDecodeError:
                        pass
                elif msg.type in (2, 3):  # ERROR, CLOSE
                    break
        except Exception as e:
            log.warning(f"WebSocket error for {username}: {e}")
        finally:
            if user_id in self._users:
                del self._users[user_id]
            log.info(f"Dashboard user disconnected: {username} ({user_id})")

        return ws

    async def _broadcast_ws(self, data: dict):
        """Broadcast data to all connected WebSocket clients."""
        disconnected = []
        for user_id, user in self._users.items():
            try:
                await user.websocket.send_json(data)
            except Exception:
                disconnected.append(user_id)
        for user_id in disconnected:
            self._users.pop(user_id, None)

    async def on_mesh_message(self, message):
        """Called by the node when a mesh message is received.

        Displays agent replies in the dashboard chat in real-time.
        Filters out heartbeat and system messages.
        Extracts text from payload for proper display.
        """
        msg_type = message.type if hasattr(message, "type") else message.message_type

        # Skip non-chat messages — they flood the chat
        if msg_type in ("heartbeat", "memory_sync", "ack", "skills_announcement", "diagnostic_report", "config_suggestion", "peer_offline", "peer_online", "node_join", "node_leave"):
            return

        # Extract display text from payload — handle both dict and JSON string payloads
        if isinstance(message.payload, dict):
            payload = message.payload
        elif isinstance(message.payload, str):
            try:
                payload = json.loads(message.payload)
            except (json.JSONDecodeError, ValueError):
                payload = {"text": message.payload}
        else:
            payload = {}

        # Skip agent_reply messages that contain heartbeat-like payload (uptime/transports only)
        # These happen when an agent's webhook response is just a status dump, not a real reply
        if msg_type == "agent_reply" and isinstance(payload, dict):
            if set(payload.keys()) <= {"uptime", "transports"}:
                return

        content = payload.get("text", "") or getattr(message, "content", "") or json.dumps(payload, ensure_ascii=True)
        username = payload.get("username", "") or message.sender

        self._message_history.append({
            "id": message.id,
            "sender": message.sender,
            "recipient": message.recipient,
            "content": content,
            "type": msg_type,
            "priority": message.priority,
            "timestamp": message.timestamp,
            "source": "mesh",
            "username": username,
        })
        if len(self._message_history) > self._max_history:
            self._message_history = self._message_history[-self._max_history:]

        await self._broadcast_ws({
            "type": "new_message",
            "message": self._message_history[-1],
        })

    async def _insert_mesh_message(self, message, auth_user):
        """Insert dashboard message into mesh.mesh_messages for mesh-wide persistence.

        Uses mesh_messages (not shared_a2a_memory) so all agents in the mesh
        see it via PG NOTIFY, and the dashboard shows agent replies in real-time.
        """
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="192.168.1.30",
                port=5432,
                dbname="agent_memory",
                user="nova",
                password="nova_agent_2026",
                options="-c client_encoding=UTF8",
            )
            cur = conn.cursor()
            payload = message.payload if isinstance(message.payload, dict) else {"text": str(message.payload)}
            # Encode username safely (handle non-ASCII names like "Lakatos Miklós Zsolt")
            username = (auth_user.display_name if auth_user else "web_user").encode("ascii", "replace").decode("ascii")
            # For SQL_ASCII PG: use ASCII-safe sender name
            safe_sender = (message.sender or "unknown").encode("ascii", "replace").decode("ascii")
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

    def _fetch_chat_history(self, limit: int = 10, channel: str = "general") -> list:
        """Fetch recent chat messages from PG for context injection.
        
        Returns a list of {sender, content, timestamp} dicts — the last N
        non-heartbeat messages from the given channel.
        """
        import psycopg2
        try:
            conn = psycopg2.connect(
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
                host=self.node.config.pg.host, port=self.node.config.pg.port,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            
            where_clauses = [
                "msg_type NOT IN ('heartbeat', 'memory_sync', 'ack', 'diagnostic_report', 'skills_announcement', 'config_suggestion', 'peer_offline', 'peer_online', 'node_join', 'node_leave')",
            ]
            params = []
            if channel == "general":
                # General channel: broadcast messages + agent replies/directives
                where_clauses.append("(recipient = 'broadcast' OR msg_type IN ('agent_reply', 'directive'))")
            elif channel and channel.startswith("dm:"):
                dm_agent = channel[3:]
                where_clauses.append("(recipient = %s OR sender = %s)")
                params.extend([dm_agent, dm_agent])
            
            where_sql = " AND ".join(where_clauses)
            cur.execute(f"""
                SELECT sender, recipient, msg_type, payload, created_at
                FROM mesh.mesh_messages
                WHERE {where_sql}
                ORDER BY created_at DESC
                LIMIT %s
            """, params + [limit])
            
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            history = []
            for row in reversed(rows):  # chronological order
                sender, recipient, msg_type, payload, created_at = row
                import json as _json
                # Handle SQL_ASCII PG: decode bytes safely
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8", errors="replace")
                elif isinstance(payload, str):
                    try:
                        payload = payload.encode("latin-1").decode("utf-8")
                    except (UnicodeDecodeError, UnicodeEncodeError):
                        pass  # keep original
                try:
                    p = _json.loads(payload) if isinstance(payload, str) else payload
                except (ValueError, TypeError):
                    p = {}
                text = p.get("text", "") if isinstance(p, dict) else str(payload)
                # Skip heartbeat-like payloads
                if isinstance(p, dict) and set(p.keys()) <= {"uptime", "transports"}:
                    continue
                if not text:
                    text = str(payload)[:200]
                history.append({
                    "sender": sender,
                    "content": text[:500],
                    "timestamp": created_at.isoformat() if created_at else "",
                    "type": msg_type,
                })
            return history
        except Exception as e:
            log.warning(f"Failed to fetch chat history: {e}")
            return []

    def _build_context_prompt(self, agent_name: str, sender: str, content: str, 
                              reply_endpoint: str, mesh_msg_id: str,
                              channel: str = "general") -> str:
        """Build a prompt with full chat context for the agent.
        
        The agent sees the recent conversation history + the new message,
        like a Telegram group chat. It can reply via curl to reply_endpoint.
        """
        history = self._fetch_chat_history(limit=10, channel=channel)
        
        # Known agent names
        agent_names = set()
        try:
            for name, _ in self.node.peer_discovery.get_all_peers().items():
                agent_names.add(name.lower())
        except Exception:
            pass
        agent_names.add(self.node.node_name.lower())
        
        # Build conversation context
        if history:
            chat_lines = []
            for h in history:
                h_sender = h.get('sender', '?')
                h_content = h.get('content', '')[:200]
                if h_sender.lower() in agent_names or h_sender.lower() in ('nova', 'morzsa', 'runa'):
                    chat_lines.append(f"  [{h_sender} 🤖] {h_content}")
                else:
                    chat_lines.append(f"  [{h_sender} 👤] {h_content}")
            chat_context = "\n".join(chat_lines[-8:])  # last 8 messages
        else:
            chat_context = "(nincs előzmény)"
        
        # Determine if sender is human or agent
        is_human = sender.lower() not in agent_names and sender.lower() not in ('nova', 'morzsa', 'runa')
        sender_tag = f"{sender} 👤 emberi felhasználó" if is_human else f"{sender} 🤖 agent"
        
        prompt = (
            f"Te egy A2A Mesh chat résztvevője vagy ({agent_name} 🤖). "
            f"Ez egy közös chat session, mint egy Telegram csoport. "
            f"A chatben emberi felhasználók (👤) és AI agentek (🤖) vesznek részt. "
            f"Az emberi felhasználók (pl. Zsolt) írnak üzeneteket, az agentek válaszolnak. "
            f"Látod a beszélgetés előzményeit és az új üzenetet.\n\n"
            f"── Beszélgetés eddig ──\n{chat_context}\n\n"
            f"── Új üzenet ──\n[{sender_tag}] {content}\n\n"
            f"Válaszolj röviden, természetesen (magyarul, max 500 karakter). "
            f"Ha az üzenet emberi felhasználótól (👤) van, neki válaszolj. "
            f"Ha egy másik agent (🤖) írt és nem hozzád szól, nem kell válaszolnod. "
            f"Ha nem kell válaszolnod, ne küldd el a curl-t.\n\n"
            f"Válaszod elküldéséhez futtasd:\n"
            f"curl -s -X POST {reply_endpoint} -H 'Content-Type: application/json' "
            f"-d '{{\"sender\":\"{agent_name}\",\"content\":\"VÁLASZOD\","
            f"\"recipient\":\"{sender}\",\"priority\":5,\"reply_to\":\"{mesh_msg_id}\"}}'"
        )
        return prompt

    async def _api_memory_get(self, request):
        """Get local mesh memory cache."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        memory = self.node.memory_sync.get_all_local_memory()
        return web.json_response({"memory": memory, "count": len(memory)})

    async def _api_memory_set(self, request):
        """Set a memory key and broadcast to mesh agents."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
            key = data.get("key")
            value = data.get("value")
            if not key:
                return web.json_response({"error": "key is required"}, status=400)
            result = await self.node.memory_sync.broadcast_memory(key, value)
            if result:
                return web.json_response({"status": "broadcast", "key": key})
            return web.json_response({"error": "broadcast failed"}, status=500)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delete_message(self, request):
        """Delete a message by ID — requires auth, admin only.

        Deletes from both local history and PG mesh_messages.
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if not user.get("is_admin", False):
            return web.json_response({"error": "Admin only"}, status=403)

        msg_id = request.match_info.get("msg_id", "")
        if not msg_id:
            return web.json_response({"error": "Missing message ID"}, status=400)

        # Remove from local history
        self._message_history = [m for m in self._message_history if m.get("id") != msg_id]

        # Remove from channelMessages cache
        for ch in list(self._channel_messages_cache.keys()) if hasattr(self, "_channel_messages_cache") else []:
            self._channel_messages_cache[ch] = [m for m in self._channel_messages_cache[ch] if m.get("id") != msg_id]

        # Remove from PG
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host,
                port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname,
                user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("DELETE FROM mesh.mesh_messages WHERE id = %s", (msg_id,))
            deleted = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            log.warning(f"Failed to delete message from PG: {e}")
            deleted = 0

        # Broadcast deletion to all connected users
        await self._broadcast_ws({"type": "message_deleted", "message_id": msg_id})

        return web.json_response({"status": "deleted", "message_id": msg_id, "pg_deleted": deleted})

    async def _api_memory_sync(self, request):
        """Request full memory sync from PG."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json() if request.content_type == "application/json" else {}
            since = data.get("since")
            memories = await self.node.memory_sync.request_sync(since=since)
            return web.json_response({"synced": len(memories), "memories": memories})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)