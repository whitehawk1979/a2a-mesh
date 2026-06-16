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
        # Auth routes
        app.router.add_post("/api/auth/register", self._api_auth_register)
        app.router.add_post("/api/auth/login", self._api_auth_login)
        app.router.add_post("/api/auth/logout", self._api_auth_logout)
        app.router.add_get("/api/auth/me", self._api_auth_me)
        app.router.add_get("/api/users", self._api_users)
        app.router.add_route("GET", "/ws", self._websocket_handler)

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
        """Return recent messages."""
        from aiohttp import web
        limit = min(int(request.query.get("limit", 50)), 200)
        messages = self._message_history[-limit:]
        return web.json_response({"messages": messages, "total": len(self._message_history)})

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
        """Send a message to the mesh from the dashboard."""
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

        # Dashboard messages get minimum P7 to ensure agent wake-up via auto-steer
        effective_priority = max(priority, 7)

        if not content.strip():
            return web.json_response({"error": "Empty message"}, status=400)

        from .message import A2AMessage, MSG_TYPE_DIRECTIVE
        msg = A2AMessage(
            sender=self.node.node_name,
            recipient=recipient if recipient else "broadcast",
            type=msg_type if msg_type != "message" else MSG_TYPE_DIRECTIVE,
            priority=effective_priority,
            payload={
                "text": content,
                "source": "web_dashboard",
                "username": user.display_name,
                "user_id": user.user_id,
            },
        )

        result = await self.node.router.send(msg)

        # Wake Hermes agent + insert into PG for A2A processing
        await self._insert_pg_message(msg, user)
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

                            # Dashboard messages get minimum P7 to ensure agent wake-up via auto-steer
                            effective_priority = max(priority, 7)

                            from .message import A2AMessage, MSG_TYPE_DIRECTIVE
                            a2a_msg = A2AMessage(
                                sender=self.node.node_name,
                                recipient=recipient if recipient else "broadcast",
                                type=MSG_TYPE_DIRECTIVE,
                                priority=effective_priority,
                                payload={
                                    "text": content,
                                    "source": "web_dashboard",
                                    "username": auth_user.display_name,
                                    "user_id": auth_user.user_id,
                                },
                            )
                            result = await self.node.router.send(a2a_msg)

                            # Wake Hermes agent to process/respond to dashboard messages
                            await self._wake_agent(a2a_msg)

                            # Also insert into PG shared_a2a_memory for A2A watcher
                            await self._insert_pg_message(a2a_msg, auth_user)

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
        """Called by the node when a mesh message is received."""
        self._message_history.append({
            "id": message.id,
            "sender": message.sender,
            "recipient": message.recipient,
            "content": message.content,
            "type": message.message_type,
            "priority": message.priority,
            "timestamp": message.timestamp,
            "source": "mesh",
        })
        if len(self._message_history) > self._max_history:
            self._message_history = self._message_history[-self._max_history:]

        await self._broadcast_ws({
            "type": "new_message",
            "message": self._message_history[-1],
        })

    async def _insert_pg_message(self, message, auth_user):
        """Insert dashboard message into PG shared_a2a_memory so A2A watcher can pick it up."""
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
            # Sanitize text for SQL_ASCII database — replace non-ASCII chars
            username = (auth_user.display_name if auth_user else "web_user").encode("ascii", "replace").decode("ascii")
            payload_json = json.dumps(payload, ensure_ascii=True)
            subject = f"Dashboard message from {username}"
            cur.execute(
                """INSERT INTO shared_a2a_memory
                   (sender_agent, recipient_agent, subject, content, memory_type, priority, status, message_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (
                    message.sender,
                    message.recipient or "broadcast",
                    subject,
                    payload_json,
                    "directive",
                    message.priority,
                    "unread",
                    message.type,
                ),
            )
            msg_id = cur.fetchone()[0]
            conn.commit()
            # Notify via PG NOTIFY
            cur.execute("NOTIFY a2a_channel, %s", (str(msg_id),))
            conn.commit()
            cur.close()
            conn.close()
            log.info(f"Dashboard message {message.id[:8]} inserted into PG (id={msg_id})")
        except Exception as e:
            log.warning(f"PG insert failed: {e}")

    async def _wake_agent(self, message):
        """Wake Hermes agent via webhook so it can process/respond to dashboard messages."""
        webhook_port = getattr(self.node, 'config', None)
        if webhook_port:
            webhook_port = getattr(webhook_port, 'webhook_port', 8644)
        else:
            webhook_port = 8644
        try:
            import aiohttp
            # Try the Hermes webhook endpoint first
            payload = {
                "message_id": message.id,
                "sender": message.sender,
                "recipient": message.recipient,
                "type": message.type,
                "priority": message.priority,
                "payload": message.payload if isinstance(message.payload, dict) else {"text": str(message.payload)},
                "source": "web_dashboard",
            }
            async with aiohttp.ClientSession() as session:
                # Try webhook endpoint
                try:
                    async with session.post(f"http://localhost:{webhook_port}/webhook", json=payload, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                        if resp.status == 200:
                            log.info(f"Agent woken via webhook for dashboard message {message.id[:8]}")
                            return
                except Exception:
                    pass
                # Fallback: try Hermes gateway health endpoint to trigger processing
                try:
                    async with session.get(f"http://localhost:{webhook_port}/health", timeout=aiohttp.ClientTimeout(total=2)) as resp:
                        log.debug(f"Gateway health check: {resp.status}")
                except Exception:
                    pass
                # Final fallback: trigger via A2A watcher PG NOTIFY (already done by _insert_pg_message)
                log.info(f"Agent wake: webhook unavailable, PG NOTIFY will trigger A2A watcher")
        except Exception as e:
            log.debug(f"Agent wake skipped: {e}")

    def get_stats(self) -> dict:
        return {
            "connected_users": len(self._users),
            "users": [u.to_dict() for u in self._users.values()],
            "message_history_size": len(self._message_history),
        }

    def _load_html(self) -> str:
        """Load the dashboard HTML page from external file."""
        html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            log.warning(f"Dashboard HTML not found at {html_path}")
            return '<html><body><h1>A2A Mesh Dashboard</h1><p>HTML not found.</p></body></html>'