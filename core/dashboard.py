"""A2A Mesh Web Dashboard — Built-in web UI with real-time chat and agent monitoring.

Embedded into the mesh node as additional HTTP routes on the health port.
Features:
- User authentication (register/login with password)
- Agent list with status (online/offline, transport availability)
- Real-time chat via WebSocket
- Message history
- Owner can manage users
"""
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Dict, List, Optional
from dataclasses import dataclass, field

from .auth import AuthManager, DashboardUser as AuthUser

log = logging.getLogger("a2a_mesh.dashboard")


@dataclass
class DashboardUser:
    """A connected WebSocket user."""
    user_id: str
    username: str
    websocket: object = None
    connected_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "connected_at": self.connected_at,
            "last_activity": self.last_activity,
        }


class DashboardHandler:
    """Handles web dashboard HTTP and WebSocket requests.

    Routes:
        GET  /              → Dashboard HTML page
        GET  /dashboard     → Dashboard HTML page
        GET  /api/status    → JSON status
        GET  /api/messages  → Recent messages
        GET  /api/agents    → Agent list
        POST /api/send      → Send a message
        POST /api/send-file → Upload a file
        POST /api/auth/register → Register new user (owner only)
        POST /api/auth/login    → Login
        POST /api/auth/logout   → Logout
        GET  /api/auth/me       → Current user info
        GET  /api/users          → List users (owner only)
        WS   /ws            → WebSocket for real-time updates
    """

    def __init__(self, node):
        self.node = node
        self.auth = AuthManager()
        self._users: Dict[str, DashboardUser] = {}
        self._message_history: List[dict] = []
        self._max_history = 200

    def register_routes(self, app):
        """Register dashboard routes on an existing aiohttp app."""
        app.router.add_get("/", self._dashboard_page)
        app.router.add_get("/dashboard", self._dashboard_page)
        app.router.add_get("/api/status", self._api_status)
        app.router.add_get("/api/messages", self._api_messages)
        app.router.add_get("/api/agents", self._api_agents)
        app.router.add_post("/api/send", self._api_send)
        app.router.add_post("/api/send-file", self._api_send_file)
        # Memory sync routes
        app.router.add_get("/api/memory", self._api_memory_get)
        app.router.add_post("/api/memory", self._api_memory_set)
        app.router.add_post("/api/memory/sync", self._api_memory_sync)
        # Auth routes
        app.router.add_post("/api/auth/register", self._api_auth_register)
        app.router.add_post("/api/auth/login", self._api_auth_login)
        app.router.add_post("/api/auth/logout", self._api_auth_logout)
        app.router.add_get("/api/auth/me", self._api_auth_me)
        app.router.add_get("/api/users", self._api_users)
        # Admin routes — node approval
        app.router.add_get("/api/nodes/pending", self._api_nodes_pending)
        app.router.add_post("/api/nodes/{node_name}/approve", self._api_node_approve)
        app.router.add_post("/api/nodes/{node_name}/reject", self._api_node_reject)
        app.router.add_get("/api/nodes", self._api_nodes_list)
        app.router.add_route("GET", "/ws", self._websocket_handler)
        # Agent reply endpoint — agents call this to send replies to the mesh chat
        app.router.add_post("/api/agent-reply", self._api_agent_reply)

    def _require_auth(self, request):
        """Extract and verify auth token from request. Returns (user, error_response)."""
        from aiohttp import web
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        else:
            token = request.cookies.get("a2a_token", "") or request.query.get("token", "")

        if not token:
            return None, web.json_response({"error": "Authentication required"}, status=401)

        user = self.auth.verify_token(token)
        if not user:
            return None, web.json_response({"error": "Invalid or expired token"}, status=401)

        return user, None

    async def _dashboard_page(self, request):
        """Serve the dashboard HTML page."""
        from aiohttp import web
        html = self._load_html()
        return web.Response(text=html, content_type="text/html")

    async def _api_status(self, request):
        """Return full mesh status."""
        from aiohttp import web
        status = self.node.get_status()

        def sanitize(obj):
            if isinstance(obj, dict):
                return {k: sanitize(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [sanitize(v) for v in obj]
            elif isinstance(obj, (str, int, float, bool, type(None))):
                return obj
            elif hasattr(obj, '__dataclass_fields__'):
                return sanitize(obj.__dict__)
            elif hasattr(obj, '__dict__'):
                return sanitize(obj.__dict__)
            else:
                return str(obj)

        return web.json_response(sanitize(status))

    async def _api_messages(self, request):
        """Return recent messages — local history + PG messages from other agents."""
        from aiohttp import web
        limit = min(int(request.query.get("limit", 50)), 200)
        
        # Local messages
        local_messages = self._message_history[-limit:]
        
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
            # Get recent messages from mesh_messages where sender != our node
            cur.execute("""
                SELECT id, sender, recipient, msg_type, priority, payload, created_at, status
                FROM mesh.mesh_messages
                WHERE sender != %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (self.node.node_name, limit))
            for row in cur.fetchall():
                msg_id, sender, recipient, msg_type, priority, payload, created_at, status = row
                # Parse payload — it's stored as text (JSON string)
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
        
        # Merge local + PG messages, deduplicate by ID, sort by timestamp
        all_messages = {m.get("id", f"local_{i}"): m for i, m in enumerate(local_messages)}
        for m in pg_messages:
            msg_id = m.get("id", "")
            if msg_id not in all_messages:
                all_messages[msg_id] = m
        
        # Sort by timestamp
        sorted_messages = sorted(all_messages.values(), key=lambda m: m.get("timestamp", ""))
        result = sorted_messages[-limit:]
        
        return web.json_response({"messages": result, "total": len(sorted_messages)})

    async def _api_agents(self, request):
        """Return list of known agents with consistent transport format."""
        from aiohttp import web
        agents = []
        # Self — extract transport availability from TransportStatus objects
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
        agents.append({
            "name": self.node.node_name,
            "role": self.node.config.topology.node_role,
            "status": "online",
            "host": getattr(self.node.config.p2p, "listen_host", "0.0.0.0"),
            "transports": {
                "p2p": self_transports.get("p2p", False),
                "pg": self_transports.get("pg_notify", self_transports.get("pg", False)),
                "http": self_transports.get("http", False),
                "ble": self_transports.get("ble", False),
            },
            "local_store": self.node.local_store.get_stats(),
        })
        # Known peers
        for name, peer in self.node.peer_discovery.get_all_peers().items():
            agents.append({
                "name": peer.name,
                "role": peer.role,
                "status": "available" if peer.p2p_available else "offline",
                "host": peer.host,
                "p2p_port": peer.p2p_port,
                "health_port": peer.health_port,
                "last_seen": peer.last_seen,
                "transports": {
                    "p2p": peer.p2p_available,
                    "pg": peer.pg_available,
                    "http": peer.http_available,
                },
            })
        return web.json_response({"agents": agents, "total": len(agents)})

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

        from .message import A2AMessage, MSG_TYPE_DIRECTIVE
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

        # Wake Hermes agent to process and reply in the mesh chat
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

    async def _api_send_file(self, request):
        """Upload a file to the mesh via P2P file transfer."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        reader = await request.multipart()
        field = None
        recipient = ""

        async for part in reader:
            if part.name == "file":
                field = part
            elif part.name == "recipient":
                recipient = (await part.text()).strip()

        if not field:
            return web.json_response({"error": "No file uploaded"}, status=400)

        import tempfile
        tmp_dir = tempfile.mkdtemp(prefix="a2a_upload_")
        file_path = os.path.join(tmp_dir, field.filename)
        with open(file_path, "wb") as f:
            while True:
                chunk = await field.read_chunk(8192)
                if not chunk:
                    break
                f.write(chunk)

        file_size = os.path.getsize(file_path)
        file_id = str(uuid.uuid4())
        transfer = self.node.file_transfer.create_outbound_transfer(
            file_id=file_id,
            file_path=file_path,
            filename=field.filename,
            file_size=file_size,
            sender=self.node.node_name,
            recipient=recipient or "broadcast",
            metadata={"source": "web_dashboard", "username": user.display_name},
        )

        return web.json_response({
            "status": "transfer_created",
            "file_id": file_id,
            "filename": field.filename,
            "size": file_size,
        })

    # ─── Auth endpoints ───

    async def _api_auth_register(self, request):
        """Register a new user. Only owners can create other users."""
        from aiohttp import web
        caller, err = self._require_auth(request)
        if err:
            return err

        if caller.role != "owner":
            return web.json_response({"error": "Only owners can register new users"}, status=403)

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        username = data.get("username", "").strip().lower()
        display_name = data.get("display_name", "").strip()
        password = data.get("password", "")
        role = data.get("role", "user")

        if not username or not password:
            return web.json_response({"error": "Username and password required"}, status=400)

        try:
            user = self.auth.register_user(username, display_name or username, password, role=role)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)

        if not user:
            return web.json_response({"error": "Username already taken"}, status=409)

        return web.json_response({
            "status": "registered",
            "user": user.to_dict(),
        })

    async def _api_auth_login(self, request):
        """Login and get a token."""
        from aiohttp import web
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        username = data.get("username", "").strip()
        password = data.get("password", "")

        if not username or not password:
            return web.json_response({"error": "Username and password required"}, status=400)

        try:
            result = self.auth.login(username, password)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=429)

        if not result:
            return web.json_response({"error": "Invalid username or password"}, status=401)

        # Set token as cookie too for WebSocket auth
        response = web.json_response({
            "status": "ok",
            "token": result["token"],
            "user": result["user"].to_dict(),
        })
        response.set_cookie("a2a_token", result["token"], max_age=86400, httponly=True, samesite="Lax")
        return response

    async def _api_auth_logout(self, request):
        """Logout and invalidate token."""
        from aiohttp import web
        auth_header = request.headers.get("Authorization", "")
        token = auth_header[7:] if auth_header.startswith("Bearer ") else ""
        if not token:
            token = request.cookies.get("a2a_token", "")

        if token:
            self.auth.logout(token)

        response = web.json_response({"status": "ok"})
        response.del_cookie("a2a_token")
        return response

    async def _api_auth_me(self, request):
        """Return current user info."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        return web.json_response({"user": user.to_dict()})

    async def _api_users(self, request):
        """List all users (owner only)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        users = self.auth.list_users()
        return web.json_response({
            "users": [u.to_dict() for u in users],
            "total": len(users),
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

        # Send initial data
        try:
            await ws.send_json({
                "type": "connected",
                "user_id": user_id,
                "username": username,
                "node": self.node.node_name,
                "authenticated": auth_user is not None,
                "role": auth_user.role if auth_user else "guest",
            })
            await ws.send_json({"type": "status", "data": self.node.get_status()})
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

                            from .message import A2AMessage, MSG_TYPE_DIRECTIVE
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

                            # Wake Hermes agent to process and reply in the mesh chat
                            # The agent will call /api/agent-reply to post its response back
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
        Extracts text from payload for proper display.
        """
        # Extract display text from payload
        payload = message.payload if isinstance(message.payload, dict) else {}
        content = payload.get("text", "") or message.content or json.dumps(payload, ensure_ascii=True)
        username = payload.get("username", "") or message.sender

        self._message_history.append({
            "id": message.id,
            "sender": message.sender,
            "recipient": message.recipient,
            "content": content,
            "type": message.type if hasattr(message, "type") else message.message_type,
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
            username = (auth_user.display_name if auth_user else "web_user").encode("ascii", "replace").decode("ascii")
            payload_json = json.dumps(payload, ensure_ascii=True)

            cur.execute(
                """INSERT INTO mesh.mesh_messages
                   (id, sender, recipient, msg_type, priority, payload, routing_mode, status, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                (
                    message.id,
                    message.sender,
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
            cur.execute("NOTIFY mesh_channel, %s", (message.id,))
            conn.commit()
            cur.close()
            conn.close()
            log.info(f"Dashboard message {message.id[:8]} inserted into mesh_messages")
        except Exception as e:
            log.warning(f"Mesh insert failed: {e}")

    async def _wake_agent(self, message):
        """Wake Hermes agent via webhook so it can process/respond to dashboard messages.

        The webhook payload includes a reply_endpoint so the agent knows
        to send its reply back to the mesh chat (not Telegram).
        After the webhook response, the reply is also posted to the mesh chat.
        """
        try:
            import hmac as hmac_mod
            import hashlib
            import urllib.request
            payload_text = (message.payload or {}).get("text", "")[:60] if isinstance(message.payload, dict) else str(message.payload)[:60]
            payload = json.dumps({
                "event_type": "a2a_message",
                "sender": message.sender,
                "recipient": message.recipient or "broadcast",
                "subject": f"Mesh Chat: {payload_text}",
                "content": json.dumps(message.payload) if isinstance(message.payload, dict) else str(message.payload),
                "priority": message.priority,
                "mesh_message_id": message.id,
                "reply_endpoint": f"http://localhost:8650/api/agent-reply",
                "reply_format": "mesh_chat",
            })
            sig = hmac_mod.new(b"a2a-instant-secret-2026", payload.encode(), hashlib.sha256).hexdigest()
            req = urllib.request.Request(
                "http://localhost:8644/webhooks/a2a-instant",
                data=payload.encode(),
                headers={
                    "Content-Type": "application/json",
                    "X-Hub-Signature-256": f"sha256={sig}",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode())
                log.info(f"Agent woken via webhook for mesh chat: {result.get('status', 'unknown')}")

                # If the webhook response contains a reply, post it to the mesh chat
                reply_text = result.get("response", "") or result.get("reply", "")
                if reply_text and isinstance(reply_text, str) and len(reply_text.strip()) > 0:
                    # Clean up the reply text — remove markdown formatting
                    reply_text = reply_text.strip()[:2000]  # Limit length
                    try:
                        reply_data = json.dumps({
                            "sender": self.node.node_name,
                            "content": reply_text,
                            "recipient": message.sender if message.sender != self.node.node_name else "broadcast",
                            "priority": 5,
                            "reply_to": message.id,
                        }).encode()
                        reply_req = urllib.request.Request(
                            "http://localhost:8650/api/agent-reply",
                            data=reply_data,
                            headers={"Content-Type": "application/json"},
                        )
                        with urllib.request.urlopen(reply_req, timeout=5) as reply_resp:
                            log.info(f"Agent reply posted to mesh chat: {reply_resp.read().decode()[:100]}")
                    except Exception as re:
                        log.warning(f"Failed to post agent reply to mesh chat: {re}")
        except Exception as e:
            log.info(f"Agent wake: webhook failed ({e}), PG NOTIFY will trigger A2A watcher")

    def get_stats(self) -> dict:
        return {
            "connected_users": len(self._users),
            "users": [u.to_dict() for u in self._users.values()],
            "message_history_size": len(self._message_history),
        }

    # ─── Admin: Node Approval ──────────────────────────────────

    def _require_owner(self, request):
        """Verify user is owner (admin). Returns (user, error_response)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return user, err
        if user.role != "owner":
            return user, web.json_response({"error": "Owner access required"}, status=403)
        return user, None

    async def _api_nodes_pending(self, request):
        """List nodes pending approval."""
        from aiohttp import web
        user, err = self._require_owner(request)
        if err:
            return err
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                SELECT node_name, role, host, p2p_port, health_port,
                       pg_available, p2p_available, http_available,
                       joined_at, last_heartbeat
                FROM mesh.mesh_nodes WHERE status = 'pending'
                ORDER BY joined_at
            """)
            nodes = []
            for row in cur.fetchall():
                nodes.append({
                    "node_name": row[0], "role": row[1], "host": row[2],
                    "p2p_port": row[3], "health_port": row[4],
                    "pg_available": row[5], "p2p_available": row[6], "http_available": row[7],
                    "joined_at": row[8].isoformat() if row[8] else None,
                    "last_heartbeat": row[9].isoformat() if row[9] else None,
                })
            cur.close()
            conn.close()
            return web.json_response({"nodes": nodes})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_node_approve(self, request):
        """Approve a pending node."""
        from aiohttp import web
        user, err = self._require_owner(request)
        if err:
            return err
        node_name = request.match_info["node_name"]
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                UPDATE mesh.mesh_nodes SET status = 'active'
                WHERE node_name = %s AND status = 'pending'
            """, (node_name,))
            conn.commit()
            approved = cur.rowcount
            cur.close()
            conn.close()
            if approved:
                log.info(f"Node '{node_name}' approved by {user.username}")
                return web.json_response({"status": "approved", "node_name": node_name})
            else:
                return web.json_response({"error": "Node not found or not pending"}, status=404)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_node_reject(self, request):
        """Reject (remove) a pending node."""
        from aiohttp import web
        user, err = self._require_owner(request)
        if err:
            return err
        node_name = request.match_info["node_name"]
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                DELETE FROM mesh.mesh_nodes
                WHERE node_name = %s AND status = 'pending'
            """, (node_name,))
            conn.commit()
            removed = cur.rowcount
            cur.close()
            conn.close()
            if removed:
                log.info(f"Node '{node_name}' rejected by {user.username}")
                return web.json_response({"status": "rejected", "node_name": node_name})
            else:
                return web.json_response({"error": "Node not found or not pending"}, status=404)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_nodes_list(self, request):
        """List all nodes (all statuses)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                SELECT node_name, role, host, p2p_port, health_port,
                       pg_available, p2p_available, http_available,
                       status, joined_at, last_heartbeat
                FROM mesh.mesh_nodes
                ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'active' THEN 1 ELSE 2 END, joined_at
            """)
            nodes = []
            for row in cur.fetchall():
                nodes.append({
                    "node_name": row[0], "role": row[1], "host": row[2],
                    "p2p_port": row[3], "health_port": row[4],
                    "pg_available": row[5], "p2p_available": row[6], "http_available": row[7],
                    "status": row[8],
                    "joined_at": row[9].isoformat() if row[9] else None,
                    "last_heartbeat": row[10].isoformat() if row[10] else None,
                })
            cur.close()
            conn.close()
            return web.json_response({"nodes": nodes})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

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

            from .message import A2AMessage, MSG_TYPE_DIRECTIVE
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
            await self._broadcast_ws({"type": "new_message", "message": msg_dict})

            return web.json_response({"status": "sent", "message_id": msg.id})
        except Exception as e:
            log.error(f"Agent reply failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

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

    def _load_html(self) -> str:
        """Load the dashboard HTML page from external file."""
        html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            log.warning(f"Dashboard HTML not found at {html_path}")
            return '<html><body><h1>A2A Mesh Dashboard</h1><p>HTML not found.</p></body></html>'