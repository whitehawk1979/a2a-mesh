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
import time
import uuid
from typing import Dict, List, Optional, Set
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
        html = self._generate_html()
        return web.Response(text=html, content_type="text/html")

    async def _api_status(self, request):
        """Return full mesh status."""
        from aiohttp import web
        status = self.node.get_status()
        return web.json_response(status)

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
        return web.json_response({"agents": agents, "total": len(agents)})

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
        # Multipart file upload
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
        import os
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
                            # Forward to mesh
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

    def _generate_html(self) -> str:
        """Generate the dashboard HTML page."""
        return '''<!DOCTYPE html>
<html lang="hu">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>A2A Mesh Dashboard</title>
<style>
:root {
  --bg: #0a0e17; --surface: #131a2b; --surface2: #1a2238;
  --border: #2a3555; --primary: #4f8cff; --primary-dim: #2d5bb9;
  --success: #22c55e; --warning: #f59e0b; --danger: #ef4444;
  --text: #e2e8f0; --text2: #94a3b8; --text3: #64748b;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:var(--bg); color:var(--text); height:100vh; display:flex; flex-direction:column; }

/* Header */
.header { background:var(--surface); border-bottom:1px solid var(--border); padding:12px 20px; display:flex; align-items:center; justify-content:space-between; flex-shrink:0; }
.header h1 { font-size:18px; font-weight:600; display:flex; align-items:center; gap:8px; }
.header h1 .mesh-icon { width:28px; height:28px; background:var(--primary); border-radius:6px; display:flex; align-items:center; justify-content:center; font-size:16px; }
.header-info { display:flex; gap:16px; align-items:center; font-size:13px; color:var(--text2); }
.status-dot { width:8px; height:8px; border-radius:50%; display:inline-block; margin-right:4px; }
.status-dot.online { background:var(--success); box-shadow:0 0 6px var(--success); }
.status-dot.offline { background:var(--danger); }

/* Main layout */
.main { display:flex; flex:1; overflow:hidden; }

/* Sidebar — Agent list */
.sidebar { width:280px; background:var(--surface); border-right:1px solid var(--border); display:flex; flex-direction:column; flex-shrink:0; }
.sidebar-header { padding:12px 16px; border-bottom:1px solid var(--border); font-size:14px; font-weight:600; display:flex; justify-content:space-between; align-items:center; }
.sidebar-header .count { background:var(--primary-dim); color:var(--primary); padding:2px 8px; border-radius:10px; font-size:12px; }
.agent-list { flex:1; overflow-y:auto; padding:8px; }
.agent-card { background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:12px; margin-bottom:8px; cursor:pointer; transition:border-color 0.2s; }
.agent-card:hover { border-color:var(--primary); }
.agent-card.active { border-color:var(--primary); box-shadow:0 0 8px rgba(79,140,255,0.2); }
.agent-card .name { font-weight:600; font-size:14px; display:flex; align-items:center; gap:6px; }
.agent-card .role { font-size:11px; color:var(--text3); text-transform:uppercase; letter-spacing:0.5px; }
.agent-card .transports { display:flex; gap:4px; margin-top:6px; flex-wrap:wrap; }
.transport-badge { font-size:10px; padding:2px 6px; border-radius:4px; background:var(--primary-dim); color:var(--primary); }
.transport-badge.inactive { background:var(--border); color:var(--text3); }

/* Chat area */
.chat-area { flex:1; display:flex; flex-direction:column; }
.chat-header { padding:12px 20px; border-bottom:1px solid var(--border); background:var(--surface); display:flex; align-items:center; justify-content:space-between; }
.chat-header .channel { font-weight:600; font-size:15px; }
.chat-header .info { font-size:12px; color:var(--text3); }

.messages { flex:1; overflow-y:auto; padding:16px 20px; display:flex; flex-direction:column; gap:8px; }
.msg { max-width:75%; padding:10px 14px; border-radius:12px; font-size:14px; line-height:1.5; word-wrap:break-word; }
.msg.sent { align-self:flex-end; background:var(--primary-dim); color:var(--text); border-bottom-right-radius:4px; }
.msg.received { align-self:flex-start; background:var(--surface2); border:1px solid var(--border); border-bottom-left-radius:4px; }
.msg.broadcast { align-self:flex-start; background:rgba(245,158,11,0.1); border:1px solid rgba(245,158,11,0.3); border-bottom-left-radius:4px; }
.msg .meta { font-size:11px; color:var(--text3); margin-top:4px; display:flex; gap:8px; }
.msg .meta .priority { padding:1px 5px; border-radius:3px; font-size:10px; }
.msg .meta .priority.p-high { background:rgba(239,68,68,0.2); color:var(--danger); }
.msg .meta .priority.p-med { background:rgba(245,158,11,0.2); color:var(--warning); }
.msg .meta .priority.p-low { background:rgba(34,197,94,0.2); color:var(--success); }
.msg .sender { font-weight:600; font-size:12px; color:var(--primary); margin-bottom:2px; }

/* Input area */
.input-area { padding:12px 20px; border-top:1px solid var(--border); background:var(--surface); display:flex; gap:8px; align-items:center; }
.input-area input[type="text"] { flex:1; background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:10px 14px; color:var(--text); font-size:14px; outline:none; }
.input-area input[type="text"]:focus { border-color:var(--primary); }
.input-area select { background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:10px 8px; color:var(--text); font-size:13px; outline:none; }
.input-area select:focus { border-color:var(--primary); }
.btn { background:var(--primary); color:white; border:none; border-radius:8px; padding:10px 18px; font-size:14px; cursor:pointer; font-weight:500; transition:opacity 0.2s; }
.btn:hover { opacity:0.9; }
.btn:active { opacity:0.8; }

/* Right panel — Status */
.right-panel { width:300px; background:var(--surface); border-left:1px solid var(--border); display:flex; flex-direction:column; flex-shrink:0; }
.panel-section { border-bottom:1px solid var(--border); padding:12px 16px; }
.panel-section h3 { font-size:13px; font-weight:600; color:var(--text2); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px; }
.stat-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
.stat-box { background:var(--surface2); border-radius:6px; padding:8px 10px; }
.stat-box .label { font-size:11px; color:var(--text3); }
.stat-box .value { font-size:18px; font-weight:700; color:var(--text); }
.stat-box .value.green { color:var(--success); }
.stat-box .value.blue { color:var(--primary); }
.stat-box .value.amber { color:var(--warning); }

/* User ID modal */
.modal-overlay { position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.7); display:flex; align-items:center; justify-content:center; z-index:100; }
.modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:24px; width:360px; }
.modal h2 { margin-bottom:16px; font-size:18px; }
.modal input { width:100%; background:var(--surface2); border:1px solid var(--border); border-radius:8px; padding:10px 14px; color:var(--text); font-size:14px; margin-bottom:12px; outline:none; }
.modal input:focus { border-color:var(--primary); }

/* Scrollbar */
::-webkit-scrollbar { width:6px; }
::-webkit-scrollbar-track { background:var(--bg); }
::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }
::-webkit-scrollbar-thumb:hover { background:var(--text3); }

@media (max-width:768px) {
  .sidebar, .right-panel { display:none; }
}
</style>
</head>
<body>

<!-- User identification modal -->
<div class="modal-overlay" id="loginModal">
  <div class="modal">
    <h2>🤖 A2A Mesh Dashboard</h2>
    <p style="color:var(--text2);margin-bottom:16px;font-size:14px;">Add meg a neved a csatlakozáshoz</p>
    <input type="text" id="usernameInput" placeholder="Felhasználónév..." maxlength="30" autofocus>
    <button class="btn" style="width:100%" onclick="connectDashboard()">Csatlakozás</button>
  </div>
</div>

<!-- Header -->
<div class="header">
  <h1><span class="mesh-icon">⚡</span> A2A Mesh</h1>
  <div class="header-info">
    <span><span class="status-dot online" id="statusDot"></span> <span id="nodeName">—</span></span>
    <span id="userCount">0 user</span>
  </div>
</div>

<div class="main">
  <!-- Sidebar: Agent list -->
  <div class="sidebar">
    <div class="sidebar-header">
      Agentek <span class="count" id="agentCount">0</span>
    </div>
    <div class="agent-list" id="agentList">
      <div style="padding:20px;text-align:center;color:var(--text3);font-size:13px;">Betöltés...</div>
    </div>
  </div>

  <!-- Chat area -->
  <div class="chat-area">
    <div class="chat-header">
      <div>
        <div class="channel" id="chatChannel">📺 Mesh Broadcast</div>
        <div class="info" id="chatInfo">Minden agent látja az üzenetet</div>
      </div>
    </div>
    <div class="messages" id="messages"></div>
    <div class="input-area">
      <select id="recipientSelect">
        <option value="">📢 Broadcast</option>
      </select>
      <input type="text" id="messageInput" placeholder="Üzenet írása..." onkeydown="if(event.key==='Enter')sendMessage()">
      <select id="prioritySelect">
        <option value="5">P5 Normál</option>
        <option value="7">P7 Magas</option>
        <option value="10">P10 Sürgős</option>
        <option value="1">P1 Alacsony</option>
      </select>
      <button class="btn" onclick="sendMessage()">Küldés</button>
    </div>
  </div>

  <!-- Right panel: Status -->
  <div class="right-panel">
    <div class="panel-section">
      <h3>Mesh Státusz</h3>
      <div class="stat-grid">
        <div class="stat-box"><div class="label">Üzenetek</div><div class="value blue" id="msgCount">0</div></div>
        <div class="stat-box"><div class="label">Agentek</div><div class="value green" id="totalAgents">0</div></div>
        <div class="stat-box"><div class="label">Local Store</div><div class="value" id="localStore">0</div></div>
        <div class="stat-box"><div class="label">P2P Peers</div><div class="value" id="p2pPeers">0</div></div>
      </div>
    </div>
    <div class="panel-section">
      <h3>Transportok</h3>
      <div class="stat-grid" id="transportGrid"></div>
    </div>
    <div class="panel-section">
      <h3>Auto-Steer</h3>
      <div class="stat-grid" id="steerGrid"></div>
    </div>
    <div class="panel-section" style="flex:1;overflow-y:auto;">
      <h3>Rendszernapló</h3>
      <div id="sysLog" style="font-size:11px;color:var(--text3);max-height:200px;overflow-y:auto;"></div>
    </div>
  </div>
</div>

<script>
let ws = null;
let nodeId = "";
let username = "";
let messageHistory = [];

function connectDashboard() {
  username = document.getElementById("usernameInput").value.trim() || "user_" + Math.random().toString(36).substr(2, 6);
  localStorage.setItem("a2a_username", username);
  document.getElementById("loginModal").style.display = "none";
  initWebSocket();
}

function initWebSocket() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(`${proto}//${location.host}/ws?username=${encodeURIComponent(username)}`);

  ws.onopen = () => { log("WebSocket connected"); loadStatus(); loadMessages(); loadAgents(); };
  ws.onmessage = (e) => {
    const data = JSON.parse(e.data);
    handleWsMessage(data);
  };
  ws.onclose = () => { log("WebSocket disconnected, reconnecting..."); setTimeout(initWebSocket, 3000); };
  ws.onerror = () => {};
}

function handleWsMessage(data) {
  switch(data.type) {
    case "connected":
      nodeId = data.node;
      document.getElementById("nodeName").textContent = data.node;
      break;
    case "status":
      updateStatus(data.data);
      break;
    case "new_message":
      addMessage(data.message);
      break;
    case "pong":
      break;
  }
}

async function loadStatus() {
  try {
    const r = await fetch("/api/status");
    const data = await r.json();
    updateStatus(data);
  } catch(e) {}
}

async function loadMessages() {
  try {
    const r = await fetch("/api/messages?limit=100");
    const data = await r.json();
    data.messages.reverse().forEach(m => addMessage(m, false));
    scrollToBottom();
  } catch(e) {}
}

async function loadAgents() {
  try {
    const r = await fetch("/api/agents");
    const data = await r.json();
    renderAgents(data.agents);
    document.getElementById("totalAgents").textContent = data.total;
    document.getElementById("agentCount").textContent = data.total;

    // Update recipient dropdown
    const sel = document.getElementById("recipientSelect");
    sel.innerHTML = \'<option value="">📢 Broadcast</option>\';
    data.agents.forEach(a => {
      if (a.name !== nodeId) {
        const opt = document.createElement("option");
        opt.value = a.name;
        opt.textContent = \`➤ \${a.name} (\${a.role})\`;
        sel.appendChild(opt);
      }
    });
  } catch(e) {}
}

function renderAgents(agents) {
  const list = document.getElementById("agentList");
  list.innerHTML = agents.map(a => {
    const isSelf = a.name === nodeId;
    const status = a.status === "online" || a.status === "available" ? "online" : "offline";
    const tr = a.transports || {};
    return \`
      <div class="agent-card \${isSelf ? "active" : ""}">
        <div class="name"><span class="status-dot \${status}"></span>\${a.name} \${isSelf ? "(te)" : ""}</div>
        <div class="role">\${a.role} \${a.host ? "· " + a.host : ""}</div>
        <div class="transports">
          \${tr.p2p !== undefined ? \'<span class="transport-badge \'+(tr.p2p?"":"inactive")+\'">P2P</span>\' : ""}
          \${tr.pg !== undefined ? \'<span class="transport-badge \'+(tr.pg?"":"inactive")+\'">PG</span>\' : ""}
          \${tr.http !== undefined ? \'<span class="transport-badge \'+(tr.http?"":"inactive")+\'">HTTP</span>\' : ""}
          \${tr.ble !== undefined ? \'<span class="transport-badge \'+(tr.ble?"":"inactive")+\'">BLE</span>\' : ""}
        </div>
      </div>\`;
  }).join("");
}

function updateStatus(data) {
  document.getElementById("msgCount").textContent = data.messages_sent || 0;
  document.getElementById("localStore").textContent = (data.local_store || {}).outbound_pending || 0;
  document.getElementById("p2pPeers").textContent = (data.peer_discovery || {}).connected_peers || 0;
  document.getElementById("userCount").textContent = ((data.dashboard || {}).connected_users || 0) + " user";

  // Transport badges
  const tr = data.transports || {};
  document.getElementById("transportGrid").innerHTML = [
    ["PG", tr.pg], ["P2P", tr.p2p], ["HTTP", tr.http], ["BLE", tr.ble]
  ].map(([n,v]) => \`<div class="stat-box"><div class="label">\${n}</div><div class="value \${v?"green":"amber"}">\${v?"✓":"✗"}</div></div>\`).join("");

  // Auto-steer
  const as = data.auto_steer || {};
  document.getElementById("steerGrid").innerHTML = [
    ["Interrupts", as.interrupts || 0], ["Queued", as.queued || 0],
    ["Processed", as.processed || 0], ["Backlog", as.backlog || 0]
  ].map(([n,v]) => \`<div class="stat-box"><div class="label">\${n}</div><div class="value">\${v}</div></div>\`).join("");
}

function addMessage(msg, scroll=true) {
  const isSent = msg.sender === nodeId;
  const isBroadcast = msg.recipient === "broadcast" || !msg.recipient;
  let cls = isSent ? "sent" : (isBroadcast ? "broadcast" : "received");
  const pri = msg.priority || 5;
  const priCls = pri >= 7 ? "p-high" : pri >= 4 ? "p-med" : "p-low";
  const priLabel = pri >= 7 ? "SÜRGŐS" : pri >= 4 ? "normál" : "alacsony";
  const senderLabel = msg.username ? msg.username : msg.sender;
  const time = msg.timestamp ? new Date(msg.timestamp * 1000).toLocaleTimeString("hu-HU") : new Date().toLocaleTimeString("hu-HU");

  const div = document.createElement("div");
  div.className = `msg ${cls}`;
  div.innerHTML = `
    <div class="sender">${senderLabel}${isBroadcast ? " → 📢 broadcast" : " → " + (msg.recipient || "broadcast")}</div>
    <div>${escapeHtml(msg.content)}</div>
    <div class="meta">
      <span>${time}</span>
      <span class="priority ${priCls}">${priLabel}</span>
      ${msg.source === "web_dashboard" ? '<span>🌐 web</span>' : '<span>🤖 mesh</span>'}
    </div>`;
  document.getElementById("messages").appendChild(div);
  messageHistory.push(msg);
  if (messageHistory.length > 200) messageHistory.shift();
  if (scroll) scrollToBottom();
}

function sendMessage() {
  const input = document.getElementById("messageInput");
  const content = input.value.trim();
  if (!content || !ws) return;

  ws.send(JSON.stringify({
    type: "chat",
    content: content,
    recipient: document.getElementById("recipientSelect").value,
    priority: parseInt(document.getElementById("prioritySelect").value),
  }));
  input.value = "";
}

function scrollToBottom() {
  const el = document.getElementById("messages");
  el.scrollTop = el.scrollHeight;
}

function escapeHtml(t) { const d = document.createElement("div"); d.textContent = t; return d.innerHTML; }

function log(msg) {
  const el = document.getElementById("sysLog");
  const time = new Date().toLocaleTimeString("hu-HU");
  el.innerHTML = `<div>[${time}] ${msg}</div>` + el.innerHTML;
}

// Auto-refresh agents
setInterval(loadAgents, 10000);
setInterval(loadStatus, 5000);

// Check saved username
const savedUser = localStorage.getItem("a2a_username");
if (savedUser) {
  document.getElementById("usernameInput").value = savedUser;
  connectDashboard();
}

document.getElementById("usernameInput").addEventListener("keydown", e => { if(e.key === "Enter") connectDashboard(); });
</script>
</body>
</html>'''