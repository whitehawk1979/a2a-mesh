"""A2A Mesh Web Dashboard — Built-in web UI with real-time chat and agent monitoring.

Embedded into the mesh node as additional HTTP routes on the health port.
Features:
- Agent list with status (online/offline, transport availability)
- Real-time chat via WebSocket
- User identification (username stored in browser)
- Message history
- File transfer status
- Mesh topology visualization
"""
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Dict, List, Optional
from dataclasses import dataclass, field

log = logging.getLogger("a2a_mesh.dashboard")


@dataclass
class DashboardUser:
    """A web dashboard user."""
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

    Mounted as routes on the existing aiohttp app (same port as /health).
    Routes:
        GET  /              → Dashboard HTML page
        GET  /dashboard     → Dashboard HTML page
        GET  /api/status    → JSON status of all agents and mesh
        GET  /api/messages  → Recent messages (last 100)
        POST /api/send      → Send a message to the mesh
        WS   /ws            → WebSocket for real-time updates
    """

    def __init__(self, node):
        self.node = node
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
        app.router.add_get("/ws", self._websocket_handler)

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
        """Return list of known agents."""
        from aiohttp import web
        agents = []
        # Self
        agents.append({
            "name": self.node.node_name,
            "role": self.node.config.topology.node_role,
            "status": "online",
            "transports": self.node.get_status().get("transports", {}),
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
        return web.json_response({"agents": agents, "total": len(agents)}, dumps=lambda x: json.dumps(x, default=str))

    async def _api_send(self, request):
        """Send a message to the mesh from the dashboard."""
        from aiohttp import web
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        recipient = data.get("recipient", "")
        content = data.get("content", "")
        msg_type = data.get("type", "message")
        priority = int(data.get("priority", 5))
        username = data.get("username", "web_user")

        if not content.strip():
            return web.json_response({"error": "Empty message"}, status=400)

        # Create and send A2A message
        from .core.message import A2AMessage
        msg = A2AMessage(
            sender=self.node.node_name,
            recipient=recipient if recipient else "broadcast",
            content=content,
            message_type=msg_type,
            priority=priority,
            metadata={
                "source": "web_dashboard",
                "username": username,
                "user_id": data.get("user_id", ""),
            },
        )

        result = await self.node.router.send(msg)

        # Store in history
        self._message_history.append({
            "id": msg.id,
            "sender": msg.sender,
            "recipient": msg.recipient,
            "content": msg.content,
            "type": msg.message_type,
            "priority": msg.priority,
            "timestamp": msg.timestamp,
            "source": "web_dashboard",
            "username": username,
            "result": str(result),
        })
        if len(self._message_history) > self._max_history:
            self._message_history = self._message_history[-self._max_history:]

        # Broadcast to WebSocket clients
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
        reader = await request.multipart()
        field = None
        recipient = ""
        username = "web_user"

        async for part in reader:
            if part.name == "file":
                field = part
            elif part.name == "recipient":
                recipient = (await part.text()).strip()
            elif part.name == "username":
                username = (await part.text()).strip()

        if not field:
            return web.json_response({"error": "No file uploaded"}, status=400)

        # Save to temp file
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

        # Create file transfer
        file_id = str(uuid.uuid4())
        transfer = self.node.file_transfer.create_outbound_transfer(
            file_id=file_id,
            file_path=file_path,
            filename=field.filename,
            file_size=file_size,
            sender=self.node.node_name,
            recipient=recipient or "broadcast",
            metadata={"source": "web_dashboard", "username": username},
        )

        return web.json_response({
            "status": "transfer_created",
            "file_id": file_id,
            "filename": field.filename,
            "size": file_size,
            "recipient": recipient or "broadcast",
        })

    async def _websocket_handler(self, request):
        """WebSocket handler for real-time dashboard updates."""
        from aiohttp import web
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        user_id = str(uuid.uuid4())[:8]
        username = request.query.get("username", f"user_{user_id}")
        user = DashboardUser(user_id=user_id, username=username, websocket=ws)
        self._users[user_id] = user

        log.info(f"Dashboard user connected: {username} ({user_id})")

        # Send initial status
        try:
            await ws.send_json({
                "type": "connected",
                "user_id": user_id,
                "username": username,
                "node": self.node.node_name,
            })
            await ws.send_json({
                "type": "status",
                "data": self.node.get_status(),
            })
        except Exception:
            pass

        # Listen for messages from client
        try:
            async for msg in ws:
                if msg.type == 1:  # TEXT
                    try:
                        data = json.loads(msg.data)
                        if data.get("type") == "chat":
                            content = data.get("content", "")
                            recipient = data.get("recipient", "")
                            priority = int(data.get("priority", 5))

                            from .core.message import A2AMessage
                            a2a_msg = A2AMessage(
                                sender=self.node.node_name,
                                recipient=recipient if recipient else "broadcast",
                                content=content,
                                message_type="message",
                                priority=priority,
                                metadata={
                                    "source": "web_dashboard",
                                    "username": username,
                                    "user_id": user_id,
                                },
                            )
                            result = await self.node.router.send(a2a_msg)

                            # Store in history
                            self._message_history.append({
                                "id": a2a_msg.id,
                                "sender": a2a_msg.sender,
                                "recipient": a2a_msg.recipient,
                                "content": a2a_msg.content,
                                "type": a2a_msg.message_type,
                                "priority": a2a_msg.priority,
                                "timestamp": a2a_msg.timestamp,
                                "source": "web_dashboard",
                                "username": username,
                            })
                            if len(self._message_history) > self._max_history:
                                self._message_history = self._message_history[-self._max_history:]

                            # Broadcast to all WS clients
                            await self._broadcast_ws({
                                "type": "new_message",
                                "message": self._message_history[-1],
                            })

                        elif data.get("type") == "ping":
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