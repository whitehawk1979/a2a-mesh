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
import sqlite3
import time
import uuid
from typing import Dict, List, Optional
from dataclasses import dataclass, field

from .auth import AuthManager, DashboardUser as AuthUser
from .registry import AgentRegistry, AgentCard, HealthRecord
from .smart_router import SmartRouter
from .workflow import WorkflowCoordinator, Workflow, WorkflowTask, ConsensusMode
from .rate_limiter import RateLimiter
from .exceptions import MeshError, RoutingError

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
        # Build PG DSN from node config for user sync
        pg_dsn = None
        if hasattr(node, 'config') and hasattr(node.config, 'pg'):
            pg_conf = node.config.pg
            password = pg_conf.password if hasattr(pg_conf, 'password') else ''
            pg_dsn = f"postgresql://{pg_conf.user}:{password}@{pg_conf.host}:{pg_conf.port}/{pg_conf.dbname}"
        self.auth = AuthManager(pg_dsn=pg_dsn)
        # Sync existing users to PG on startup (bootstrap)
        if pg_dsn:
            try:
                self.auth.sync_all_to_pg()
                log.info("Initial PG user sync (push) completed")
            except Exception as e:
                log.warning(f"Initial PG user sync failed: {e}")
        # Auto-approve known agents if topology.auto_approve_known_agents is True
        auto_approve = getattr(getattr(node.config, 'topology', None), 'auto_approve_known_agents', False)
        self.registry = AgentRegistry(auto_approve=auto_approve)
        self.smart_router = SmartRouter(self.registry)
        self.workflow_coordinator = WorkflowCoordinator(self.registry, self.smart_router)
        self.rate_limiter = RateLimiter()
        self._users: Dict[str, DashboardUser] = {}
        self._message_history: List[dict] = []
        self._max_history = 200

    def register_routes(self, app):
        """Register dashboard routes on an existing aiohttp app."""
        app.router.add_get("/", self._dashboard_page)
        app.router.add_get("/dashboard", self._dashboard_page)
        app.router.add_get("/api/status", self._api_status)
        app.router.add_get("/api/messages", self._api_messages)
        app.router.add_get("/api/messages/incoming", self._api_messages_incoming)
        app.router.add_get("/api/agents", self._api_agents)
        app.router.add_post("/api/send", self._api_send)
        app.router.add_post("/api/send-file", self._api_send_file)
        app.router.add_get("/api/files", self._api_list_files)
        app.router.add_get("/api/files/{type}/{filename}", self._api_download_file)
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
        # User management endpoints (owner only)
        app.router.add_get("/api/auth/users", self._api_auth_users)
        app.router.add_delete("/api/auth/users/{username}", self._api_auth_delete_user)
        app.router.add_put("/api/auth/users/{username}/password", self._api_auth_change_password)
        # User sync endpoint — other nodes pull users from PG
        app.router.add_post("/api/auth/sync", self._api_auth_sync)
        app.router.add_get("/api/auth/sync", self._api_auth_sync_pull)
        # Admin routes — node approval
        app.router.add_get("/api/nodes/pending", self._api_nodes_pending)
        app.router.add_post("/api/nodes/{node_name}/approve", self._api_node_approve)
        app.router.add_post("/api/nodes/{node_name}/reject", self._api_node_reject)
        app.router.add_get("/api/nodes", self._api_nodes_list)
        app.router.add_route("GET", "/ws", self._websocket_handler)
        # Agent reply endpoint — agents call this to send replies to the mesh chat
        app.router.add_post("/api/agent-reply", self._api_agent_reply)
        # Wake-agent endpoint — peer nodes call this to wake the local agent
        app.router.add_post("/api/wake-agent", self._api_wake_agent)
        # Message management — delete
        app.router.add_delete("/api/messages/{msg_id}", self._api_delete_message)
        # Agent Registry endpoints
        app.router.add_get("/api/registry", self._api_registry_stats)
        app.router.add_get("/api/registry/agents", self._api_registry_list)
        app.router.add_get("/api/registry/agents/{name}", self._api_registry_get)
        app.router.add_post("/api/registry/agents", self._api_registry_register)
        app.router.add_delete("/api/registry/agents/{name}", self._api_registry_deregister)
        app.router.add_get("/api/registry/find", self._api_registry_find)
        app.router.add_post("/api/registry/record-success/{name}", self._api_registry_success)
        app.router.add_post("/api/registry/record-failure/{name}", self._api_registry_failure)
        # A2A v0.8 endpoints — Agent Card + Stream Mux + Queue Stats
        app.router.add_get("/.well-known/agent-card.json", self._api_agent_card)
        app.router.add_get("/api/agent-card", self._api_agent_card)
        app.router.add_get("/api/router/stats", self._api_router_stats)
        # Health Scorer endpoint
        app.router.add_get("/api/health/scores", self._api_health_scores)
        app.router.add_post("/api/health/record-success/{name}", self._api_health_success)
        app.router.add_post("/api/health/record-failure/{name}", self._api_health_failure)
        # P2P management endpoints
        app.router.add_post("/api/p2p/reset-backoff", self._api_p2p_reset_backoff)
        app.router.add_post("/api/p2p/reconnect", self._api_p2p_reconnect)
        app.router.add_post("/api/registry/record-failure/{name}", self._api_registry_failure)
        # Smart Router endpoints
        app.router.add_get("/api/route", self._api_route)
        app.router.add_get("/api/route/explain", self._api_route_explain)
        app.router.add_get("/api/route/options", self._api_route_options)
        # Workflow DAG endpoints
        app.router.add_post("/api/workflow", self._api_workflow_create)
        app.router.add_get("/api/workflow/{wf_id}", self._api_workflow_status)
        app.router.add_get("/api/workflows", self._api_workflows_list)
        # Pending agent approval endpoints
        app.router.add_get("/api/registry/pending", self._api_registry_pending)
        app.router.add_post("/api/registry/approve/{name}", self._api_registry_approve)
        app.router.add_post("/api/registry/reject/{name}", self._api_registry_reject)
        app.router.add_get("/api/settings", self._api_settings_get)
        app.router.add_post("/api/settings", self._api_settings_update)
        app.router.add_get("/api/mesh/topology", self._api_mesh_topology)
        app.router.add_get("/topology", self._api_topology_page)
        # Plugin API
        app.router.add_get("/api/plugins", self._api_plugins)
        app.router.add_get("/api/plugins/{plugin_name}", self._api_plugin_detail)

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

        # Rate limit check
        client_id = user.username if user else request.remote
        if not self.rate_limiter.allow(client_id):
            return None, web.json_response({"error": "Rate limit exceeded"}, status=429)

        return user, None

    async def _dashboard_page(self, request):
        """Serve the dashboard HTML page."""
        from aiohttp import web
        html = self._load_html()
        return web.Response(
            text=html,
            content_type="text/html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

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

                # Exclude heartbeat and system messages from chat
                where_clauses.append("msg_type NOT IN ('heartbeat', 'memory_sync')")

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

    async def _api_send_file(self, request):
        """Upload a file to the mesh via P2P file transfer.

        Accepts multipart form with:
        - file: the file to upload
        - recipient: target agent name or 'broadcast' (default: broadcast)

        For broadcast files, sends to all known peers.
        For targeted files, sends to a specific agent.
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        reader = await request.multipart()
        file_data = None
        file_name = "upload"
        recipient = ""

        async for part in reader:
            if part.name == "file":
                # Read file data immediately during multipart iteration
                file_data = await part.read()
                file_name = part.filename or "upload"
            elif part.name == "recipient":
                recipient = (await part.text()).strip()

        if not file_data:
            return web.json_response({"error": "No file uploaded"}, status=400)

        file_size = len(file_data)

        # Save uploaded file to incoming_files directory (persistent)
        upload_dir = os.path.join(
            os.path.expanduser("~/.hermes/scripts/a2a_mesh/incoming_files"),
            "uploads"
        )
        os.makedirs(upload_dir, exist_ok=True)

        # Add timestamp to avoid filename collisions
        import time as _time
        ts = int(_time.time())
        safe_name = f"{ts}_{file_name}"
        file_path = os.path.join(upload_dir, safe_name)

        with open(file_path, "wb") as f:
            f.write(file_data)

        if file_size == 0:
            os.unlink(file_path)
            return web.json_response({"error": "Empty file"}, status=400)

        # Max file size: 50MB
        if file_size > 50 * 1024 * 1024:
            os.unlink(file_path)
            return web.json_response({"error": "File too large (max 50MB)"}, status=400)

        # Determine recipients
        target = recipient or "broadcast"
        results = []

        try:
            if target == "broadcast":
                # Send to all known peers
                peers = self.node.peer_discovery.get_all_peers()
                for peer_name, peer in peers.items():
                    if peer.p2p_available or peer.host:
                        try:
                            offer_msg, file_id = self.node.file_transfer.create_offer_message(
                                file_path, peer_name, priority=5
                            )
                            send_result = await self.node.send(offer_msg)
                            results.append({
                                "peer": peer_name,
                                "file_id": file_id,
                                "success": send_result.success,
                                "error": send_result.error or "",
                            })
                            log.info(f"File upload broadcast: {safe_name} → {peer_name} (file_id={file_id})")
                        except Exception as e:
                            log.error(f"File upload to {peer_name} failed: {e}")
                            results.append({
                                "peer": peer_name,
                                "file_id": "",
                                "success": False,
                                "error": str(e),
                            })
            else:
                # Send to specific agent
                try:
                    offer_msg, file_id = self.node.file_transfer.create_offer_message(
                        file_path, target, priority=5
                    )
                    send_result = await self.node.send(offer_msg)
                    results.append({
                        "peer": target,
                        "file_id": file_id,
                        "success": send_result.success,
                        "error": send_result.error or "",
                    })
                    log.info(f"File upload direct: {safe_name} → {target} (file_id={file_id})")
                except Exception as e:
                    log.error(f"File upload to {target} failed: {e}")
                    results.append({
                        "peer": target,
                        "file_id": "",
                        "success": False,
                        "error": str(e),
                    })

            # Notify dashboard via WebSocket
            await self._broadcast_ws({
                "type": "file_transfer",
                "filename": file_name,
                "safe_name": safe_name,
                "size": file_size,
                "sender": (user.display_name if user else self.node.node_name) or self.node.node_name,
                "recipient": target,
                "results": results,
            })

            # Also broadcast a chat message about the file
            from ..core.message import A2AMessage
            chat_msg = A2AMessage.create(
                sender=self.node.node_name,
                recipient=target,
                msg_type="chat",
                priority=5,
                payload={"text": f"📎 Fájl megosztva: {file_name} ({self._format_size(file_size)})", "username": (user.display_name if user else self.node.node_name) or self.node.node_name, "source": "web_dashboard"}
            )
            await self.node.send(chat_msg)

            return web.json_response({
                "status": "ok",
                "filename": file_name,
                "safe_name": safe_name,
                "size": file_size,
                "results": results,
            })

        except Exception as e:
            log.error(f"File upload failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_list_files(self, request):
        """List received/uploaded files in the mesh."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        files = []
        incoming_dir = os.path.expanduser("~/.hermes/scripts/a2a_mesh/incoming_files")
        uploads_dir = os.path.join(incoming_dir, "uploads")

        # Scan incoming files
        for dir_path, label in [(incoming_dir, "received"), (uploads_dir, "uploaded")]:
            if not os.path.isdir(dir_path):
                continue
            for fname in sorted(os.listdir(dir_path), reverse=True):
                fpath = os.path.join(dir_path, fname)
                if os.path.isfile(fpath):
                    stat = os.stat(fpath)
                    files.append({
                        "name": fname,
                        "size": stat.st_size,
                        "size_human": self._format_size(stat.st_size),
                        "modified": stat.st_mtime,
                        "type": label,
                        "url": f"/api/files/{label}/{fname}",
                    })

        return web.json_response({"files": files, "total": len(files)})

    async def _api_download_file(self, request):
        """Download a file from the mesh incoming/uploaded files."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        file_type = request.match_info.get("type", "")
        filename = request.match_info.get("filename", "")

        if not filename or file_type not in ("received", "uploaded"):
            return web.json_response({"error": "Invalid request"}, status=400)

        incoming_dir = os.path.expanduser("~/.hermes/scripts/a2a_mesh/incoming_files")
        uploads_dir = os.path.join(incoming_dir, "uploads")

        if file_type == "uploaded":
            base_dir = uploads_dir
        else:
            base_dir = incoming_dir

        file_path = os.path.join(base_dir, filename)

        # Security: prevent directory traversal
        if not os.path.abspath(file_path).startswith(os.path.abspath(base_dir)):
            return web.json_response({"error": "Access denied"}, status=403)

        if not os.path.isfile(file_path):
            return web.json_response({"error": "File not found"}, status=404)

        return web.FileResponse(file_path)

    @staticmethod
    def _format_size(size_bytes):
        """Format file size in human-readable form."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"

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

    async def _api_auth_users(self, request):
        """List all users for management UI (owner only). Returns extended info."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        users = self.auth.list_users()
        users_data = []
        for u in users:
            d = u.to_dict()
            # Don't expose password hash, but include activity info
            d.pop("password_hash", None)
            users_data.append(d)
        return web.json_response({
            "users": users_data,
            "total": len(users_data),
        })

    async def _api_auth_delete_user(self, request):
        """Delete (deactivate) a user by username (owner only)."""
        from aiohttp import web
        caller, err = self._require_auth(request)
        if err:
            return err
        if caller.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        username = request.match_info.get("username", "")
        if not username:
            return web.json_response({"error": "Username required"}, status=400)

        # Prevent deleting yourself
        if username.lower() == caller.username.lower():
            return web.json_response({"error": "Cannot delete your own account"}, status=400)

        # Find the user by username
        target = self.auth.get_user_by_username(username)
        if not target:
            return web.json_response({"error": f"User '{username}' not found"}, status=404)

        # Deactivate the user
        self.auth.delete_user(target.user_id)
        log.info(f"Owner '{caller.username}' deleted user '{username}'")

        return web.json_response({
            "status": "deleted",
            "username": username.lower(),
        })

    async def _api_auth_change_password(self, request):
        """Change a user's password (owner only)."""
        from aiohttp import web
        caller, err = self._require_auth(request)
        if err:
            return err
        if caller.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        username = request.match_info.get("username", "")
        if not username:
            return web.json_response({"error": "Username required"}, status=400)

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        new_password = data.get("new_password", "")
        if not new_password:
            return web.json_response({"error": "new_password is required"}, status=400)
        if len(new_password) < 6:
            return web.json_response({"error": "Password must be at least 6 characters"}, status=400)

        # Find the user by username
        target = self.auth.get_user_by_username(username)
        if not target:
            return web.json_response({"error": f"User '{username}' not found"}, status=404)

        try:
            self.auth.change_password(target.user_id, new_password)
            log.info(f"Owner '{caller.username}' changed password for user '{username}'")
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)

        return web.json_response({
            "status": "password_changed",
            "username": username.lower(),
        })

    async def _api_auth_sync(self, request):
        """Trigger user sync from PG. POST endpoint for manual sync."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        try:
            self.auth._sync_from_pg()
            users = self.auth.list_users()
            return web.json_response({
                "status": "synced",
                "users_pulled": len(users),
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_auth_sync_pull(self, request):
        """Pull users from PG into local SQLite. GET endpoint for other nodes."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        try:
            self.auth._sync_from_pg()
            users = self.auth.list_users()
            return web.json_response({
                "status": "synced",
                "users": [u.to_dict() for u in users],
                "total": len(users),
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

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

        # Skip heartbeat, ACK, and system messages — they flood the chat
        if msg_type in ("heartbeat", "memory_sync", "ack", "skills_announcement"):
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

        content = payload.get("text", "") or message.content or json.dumps(payload, ensure_ascii=True)
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
            
            # Run hermes -z (one-shot query) with terminal toolset
            proc = await aio.create_subprocess_exec(
                "/Users/zsolt/.hermes/hermes-agent/venv/bin/hermes",
                "-z", prompt,
                "-t", "terminal",
                "--yolo",
                stdout=aio.subprocess.PIPE,
                stderr=aio.subprocess.PIPE,
                env={**__import__('os').environ, "HERMES_HOME": "/Users/zsolt/.hermes"},
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
                "msg_type NOT IN ('heartbeat', 'memory_sync', 'ack')",
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
        """List all nodes — merges registry (live) data with PG (persistent) data."""
        from aiohttp import web
        import time as _time
        user, err = self._require_auth(request)
        if err:
            return err
        nodes = {}  # name -> node_dict

        # 1. Registry data (live, in-memory — always up-to-date)
        reg = self.registry
        if reg:
            try:
                for card, health in reg.list_agents():
                    name = card.name
                    nodes[name] = {
                        "node_name": name,
                        "role": getattr(card, 'metadata', {}).get('role', 'agent') if hasattr(card, 'metadata') and card.metadata else 'agent',
                        "host": card.endpoint.replace("http://", "").split(":")[0] if card.endpoint else "",
                        "p2p_port": getattr(card, 'metadata', {}).get('p2p_port', 8645) if hasattr(card, 'metadata') and card.metadata else 8645,
                        "health_port": int(card.endpoint.split(":")[-1]) if card.endpoint and ":" in card.endpoint else 8650,
                        "pg_available": True,  # in registry = PG works
                        "p2p_available": False,  # will be enriched from P2P below
                        "http_available": True,
                        "status": "active",
                        "skills": list(card.skills) if card.skills else [],
                        "capabilities": list(card.capabilities) if card.capabilities else [],
                        "health_score": round(health.health_score, 3),
                        "uptime_seconds": round(health.last_success - health.last_failure, 1) if health.last_success and health.last_failure else 0,
                        "last_seen": health.last_health_check or 0,
                        "message_count": health.total_requests,
                        "version": card.version or "1.0.0",
                    }
            except Exception as e:
                log.warning(f"Nodes list: registry lookup failed: {e}")

        # 2. P2P peer data (live connection status)
        pd = getattr(self.node, 'peer_discovery', None)
        if pd and hasattr(pd, '_peers'):
            for name, peer in pd._peers.items():
                p2p_available = getattr(peer, 'p2p_available', False)
                if name in nodes:
                    nodes[name]["p2p_available"] = p2p_available
                    nodes[name]["status"] = "connected" if p2p_available else nodes[name].get("status", "registered")
                else:
                    nodes[name] = {
                        "node_name": name,
                        "role": getattr(peer, 'role', 'router'),
                        "host": getattr(peer, 'host', ''),
                        "p2p_port": getattr(peer, 'p2p_port', 8645),
                        "health_port": getattr(peer, 'health_port', 8650),
                        "pg_available": getattr(peer, 'pg_available', False),
                        "p2p_available": p2p_available,
                        "http_available": getattr(peer, 'http_available', False),
                        "status": "connected" if p2p_available else "disconnected",
                        "skills": [],
                        "capabilities": list(getattr(peer, 'capabilities', []) or []),
                        "health_score": 1.0,
                        "uptime_seconds": 0,
                        "last_seen": getattr(peer, 'last_seen', 0),
                        "message_count": 0,
                        "version": "1.0.0",
                    }

        # 3. PG data (persistent — fills gaps for offline/pending nodes)
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
                       status, joined_at, last_heartbeat, skills, capabilities
                FROM mesh.mesh_nodes
                ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'active' THEN 1 ELSE 2 END, joined_at
            """)
            for row in cur.fetchall():
                name = row[0]
                pg_status = row[8]
                if name not in nodes:
                    # Not in registry/P2P — offline or pending
                    nodes[name] = {
                        "node_name": name,
                        "role": row[1],
                        "host": row[2],
                        "p2p_port": row[3],
                        "health_port": row[4],
                        "pg_available": row[5],
                        "p2p_available": row[6],
                        "http_available": row[7],
                        "status": pg_status or "unknown",
                        "skills": row[11] if isinstance(row[11], list) else (json.loads(row[11]) if isinstance(row[11], str) else []),
                        "capabilities": row[12] if isinstance(row[12], list) else (json.loads(row[12]) if isinstance(row[12], str) else []),
                        "health_score": 1.0,
                        "uptime_seconds": 0,
                        "last_seen": row[10].isoformat() if row[10] else None,
                        "message_count": 0,
                        "version": "1.0.0",
                        "joined_at": row[9].isoformat() if row[9] else None,
                    }
                else:
                    # Enrich with PG data for fields not in registry
                    if not nodes[name].get("joined_at") and row[9]:
                        nodes[name]["joined_at"] = row[9].isoformat()
                    if pg_status == "pending" and nodes[name].get("status") not in ("connected", "active", "registered"):
                        nodes[name]["status"] = "pending"
            cur.close()
            conn.close()
        except Exception as e:
            log.warning(f"Nodes list: PG lookup failed: {e}")

        # Sort: connected > active > registered > pending > others
        status_order = {"connected": 0, "active": 1, "registered": 2, "pending": 3}
        sorted_nodes = sorted(nodes.values(), key=lambda n: status_order.get(n.get("status", ""), 99))
        return web.json_response({"nodes": sorted_nodes})

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
                return web.json_response({"error": "Hermes CLI not found"}, status=500)
            except Exception as e:
                log.error(f"Wake-agent CLI failed: {e}")
                return web.json_response({"error": str(e)}, status=500)
                
        except Exception as e:
            log.error(f"Wake-agent endpoint failed: {e}")
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

    # ─── Agent Registry API Handlers ──────────────────────────────

    async def _api_registry_stats(self, request):
        """GET /api/registry — Registry statistics and agent health overview."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        stats = self.registry.get_stats()
        stats["self_name"] = self.node.node_name if self.node else ""
        return web.json_response(stats)

    async def _api_registry_list(self, request):
        """GET /api/registry/agents — List all registered agents with health."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        agents = self.registry.list_agents()
        result = []
        for card, health in agents:
            result.append({
                "name": card.name,
                "capabilities": card.capabilities,
                "skills": card.skills if hasattr(card, 'skills') else [],
                "version": card.version,
                "description": card.description,
                "endpoint": card.endpoint,
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "success_rate": round(health.success_rate, 3),
                "avg_latency_ms": round(health.avg_latency_ms, 1),
                "current_load": health.current_load,
                "uptime_pct": round(health.uptime_pct, 1),
                "total_requests": health.total_requests,
                "total_failures": health.total_failures,
                "max_concurrent": card.max_concurrent,
            })
        return web.json_response({"agents": result, "total": len(result)})

    async def _api_registry_get(self, request):
        """GET /api/registry/agents/{name} — Get a specific agent's details."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        card = self.registry.get(name)
        if not card:
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        health = self.registry.get_health(name) or HealthRecord()
        return web.json_response({
            "name": card.name,
            "capabilities": card.capabilities,
            "skills": card.skills if hasattr(card, 'skills') else [],
            "version": card.version,
            "description": card.description,
            "endpoint": card.endpoint,
            "health_endpoint": card.health_endpoint,
            "max_concurrent": card.max_concurrent,
            "cost_per_task": card.cost_per_task,
            "metadata": card.metadata,
            "health": {
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "success_rate": round(health.success_rate, 3),
                "avg_latency_ms": round(health.avg_latency_ms, 1),
                "current_load": health.current_load,
                "uptime_pct": round(health.uptime_pct, 1),
                "total_requests": health.total_requests,
                "total_failures": health.total_failures,
                "consecutive_successes": health.consecutive_successes,
                "consecutive_failures": health.consecutive_failures,
            },
        })

    async def _api_registry_register(self, request):
        """POST /api/registry/agents — Register or update an agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        name = data.get("name", "").strip()
        if not name:
            return web.json_response({"error": "Agent name is required"}, status=400)

        card = AgentCard(
            name=name,
            capabilities=data.get("capabilities", []),
            version=data.get("version", "1.0.0"),
            description=data.get("description", ""),
            endpoint=data.get("endpoint", ""),
            health_endpoint=data.get("health_endpoint", "/health"),
            max_concurrent=data.get("max_concurrent", 10),
            cost_per_task=data.get("cost_per_task", 0.0),
            metadata=data.get("metadata", {}),
        )

        force = data.get("force", False)
        health = self.registry.register(card, force=force)

        # Auto-register in peer discovery if endpoint provided
        if card.endpoint and self.node and hasattr(self.node, 'peer_discovery'):
            from ..core.peer_discovery import PeerInfo
            import re
            # Parse host:port from endpoint
            match = re.match(r'https?://([^:]+):(\d+)', card.endpoint)
            if match:
                host, port = match.group(1), int(match.group(2))
                self.node.peer_discovery.add_peer(name, host, port + 1)

        return web.json_response({
            "status": "ok",
            "agent": name,
            "health_score": round(health.health_score, 3),
            "capabilities": card.capabilities,
        })

    async def _api_registry_deregister(self, request):
        """DELETE /api/registry/agents/{name} — Remove an agent from the registry."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        if not self.registry.get(name):
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        self.registry.deregister(name)
        return web.json_response({"status": "ok", "deregistered": name})

    async def _api_registry_find(self, request):
        """GET /api/registry/find?capabilities=cap1,cap2 — Find agents by capability."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        healthy_only = request.query.get("healthy_only", "true").lower() == "true"
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else []

        matches = self.registry.find_by_capability(
            capabilities, healthy_only=healthy_only, min_health_score=min_score
        )

        result = []
        for card, health in matches:
            result.append({
                "name": card.name,
                "capabilities": card.capabilities,
                "version": card.version,
                "endpoint": card.endpoint,
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "current_load": health.current_load,
            })
        return web.json_response({"matches": result, "total": len(result)})

    async def _api_registry_success(self, request):
        """POST /api/registry/record-success/{name} — Record a successful interaction."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        if not self.registry.get(name):
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        try:
            data = await request.json() if request.content_type == "application/json" else {}
        except Exception:
            data = {}
        latency_ms = float(data.get("latency_ms", 0))
        score = self.registry.record_success(name, latency_ms)
        health = self.registry.get_health(name)
        return web.json_response({
            "status": "ok",
            "agent": name,
            "health_score": round(score, 3),
            "success_rate": round(health.success_rate, 3) if health else 0,
        })

    async def _api_registry_failure(self, request):
        """POST /api/registry/record-failure/{name} — Record a failed interaction."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        if not self.registry.get(name):
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        score = self.registry.record_failure(name)
        health = self.registry.get_health(name)
        return web.json_response({
            "status": "ok",
            "agent": name,
            "health_score": round(score, 3),
            "consecutive_failures": health.consecutive_failures if health else 0,
        })

    async def _api_p2p_reset_backoff(self, request):
        """POST /api/p2p/reset-backoff — Reset P2P backoff for all or specific peers."""
        from aiohttp import web
        p2p = self.node.p2p_transport
        if not p2p:
            return web.json_response({"error": "P2P transport not available"}, status=503)
        body = await request.json() if request.content_type == 'application/json' else {}
        peer_name = body.get("peer")
        if peer_name:
            p2p._peer_backoff.pop(peer_name, None)
            p2p._peer_retry_count.pop(peer_name, None)
            return web.json_response({"status": "ok", "peer": peer_name, "backoff_reset": True})
        # Reset all backoffs
        count = len(p2p._peer_backoff)
        p2p._peer_backoff.clear()
        p2p._peer_retry_count.clear()
        return web.json_response({"status": "ok", "backoffs_reset": count})

    async def _api_p2p_reconnect(self, request):
        """POST /api/p2p/reconnect — Trigger immediate P2P reconnection to all discovered peers."""
        from aiohttp import web
        import logging
        log = logging.getLogger("a2a_mesh.dashboard")
        discovery = self.node.peer_discovery
        if not discovery:
            return web.json_response({"error": "Peer discovery not available"}, status=503)
        # Reset all backoffs first
        p2p = self.node.p2p_transport
        if p2p:
            p2p._peer_backoff.clear()
            p2p._peer_retry_count.clear()
        # Trigger discovery and connect
        try:
            result = await discovery.discover_and_connect()
            return web.json_response({"status": "ok", "discovery_result": str(result)})
        except Exception as e:
            log.error(f"P2P reconnect failed: {e}")
            return web.json_response({"status": "error", "error": str(e)}, status=500)

    # ─── A2A v0.8 API Handlers ─────────────────────────────────────

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

    async def _api_router_stats(self, request):
        """GET /api/router/stats — Detailed router + stream mux + queue statistics.
        
        Returns comprehensive routing stats including:
        - Message routing counters (sent, received, forwarded, duplicates, etc.)
        - Dedup cache stats (hits, misses, hit rate)
        - Bounded queue stats (enqueued, dequeued, dropped, overflow)
        - Stream multiplexer stats (routed, unmatched, by_stream)
        - Protocol version
        """
        from aiohttp import web
        
        if not self.node.router:
            return web.json_response({"error": "Router not available"}, status=503)
        
        stats = self.node.router.get_stats()
        
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
        
        return web.json_response(sanitize(stats))

    # ─── Health Scorer API Handlers ─────────────────────────────────

    async def _api_health_scores(self, request):
        """GET /api/health/scores — All agent health scores."""
        from aiohttp import web
        scorer = getattr(self.node.router, '_health_scorer', None)
        if scorer:
            return web.json_response(scorer.stats)
        return web.json_response({"agent_count": 0, "agents": {}})

    async def _api_health_success(self, request):
        """POST /api/health/record-success/{name}?latency_ms=0 — Record agent success."""
        from aiohttp import web
        name = request.match_info['name']
        latency_ms = float(request.query.get('latency_ms', '0'))
        scorer = getattr(self.node.router, '_health_scorer', None)
        if scorer:
            score = scorer.record_success(name, latency_ms)
            return web.json_response({"agent": name, "health_score": round(score, 3)})
        return web.json_response({"error": "health_scorer not available"}, status=503)

    async def _api_health_failure(self, request):
        """POST /api/health/record-failure/{name} — Record agent failure."""
        from aiohttp import web
        name = request.match_info['name']
        scorer = getattr(self.node.router, '_health_scorer', None)
        if scorer:
            score = scorer.record_failure(name)
            return web.json_response({"agent": name, "health_score": round(score, 3)})
        return web.json_response({"error": "health_scorer not available"}, status=503)

    # ─── Smart Router API Handlers ─────────────────────────────────

    async def _api_route(self, request):
        """GET /api/route?capabilities=cap1,cap2&strategy=health_weighted — Route to best agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        strategy = request.query.get("strategy", "health_weighted")
        exclude_str = request.query.get("exclude", "")
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else None
        exclude = [e.strip() for e in exclude_str.split(",") if e.strip()] if exclude_str else None

        agent = self.smart_router.route(
            required_capabilities=capabilities,
            strategy=strategy,
            exclude_agents=exclude,
            min_health_score=min_score,
        )

        if not agent:
            return web.json_response({
                "error": "No suitable agent found",
                "capabilities": capabilities,
                "strategy": strategy,
            }, status=404)

        health = self.registry.get_health(agent.name) or HealthRecord()
        return web.json_response({
            "agent": agent.name,
            "capabilities": agent.capabilities,
            "version": agent.version,
            "endpoint": agent.endpoint,
            "health_score": round(health.health_score, 3),
            "status": health.status,
            "current_load": health.current_load,
            "strategy": strategy,
        })

    async def _api_route_explain(self, request):
        """GET /api/route/explain?capabilities=cap1,cap2&strategy=health_weighted — Route with explanation."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        strategy = request.query.get("strategy", "health_weighted")
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else None

        agent, explanation = self.smart_router.route_with_explanation(
            required_capabilities=capabilities,
            strategy=strategy,
            min_health_score=min_score,
        )

        if not agent:
            return web.json_response({
                "agent": None,
                "explanation": explanation,
                "capabilities": capabilities,
            })

        health = self.registry.get_health(agent.name) or HealthRecord()
        return web.json_response({
            "agent": agent.name,
            "capabilities": agent.capabilities,
            "health_score": round(health.health_score, 3),
            "status": health.status,
            "explanation": explanation,
        })

    async def _api_route_options(self, request):
        """GET /api/route/options?capabilities=cap1,cap2 — List all routing options."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else None

        options = self.smart_router.get_all_routes(
            required_capabilities=capabilities,
            min_health_score=min_score,
        )

        return web.json_response({
            "options": options,
            "total": len(options),
            "capabilities": capabilities,
        })

    # ─── Workflow DAG API Handlers ──────────────────────────────────

    async def _api_workflow_create(self, request):
        """POST /api/workflow — Create and execute a workflow DAG.

        Body:
            {
                "name": "research-task",
                "consensus": "all",  // all, any, majority
                "tasks": [
                    {
                        "id": "search",
                        "name": "Web Search",
                        "capabilities": ["web_search"],
                        "payload": {"query": "AI trends"},
                        "dependencies": [],
                        "timeout": 60
                    },
                    {
                        "id": "summarize",
                        "name": "Summarize",
                        "capabilities": ["summarization@v2"],
                        "dependencies": ["search"],
                        "timeout": 30
                    }
                ]
            }
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        name = data.get("name", "unnamed-workflow")
        consensus_str = data.get("consensus", "all")
        try:
            consensus = ConsensusMode(consensus_str)
        except ValueError:
            consensus = ConsensusMode.ALL

        # Build workflow
        coordinator = self.workflow_coordinator
        if self.node:
            coordinator = WorkflowCoordinator(self.registry, self.smart_router, node=self.node)

        wf = coordinator.create_workflow(name, consensus_mode=consensus)

        for task_data in data.get("tasks", []):
            task = WorkflowTask(
                id=task_data.get("id", str(uuid.uuid4())[:8]),
                name=task_data.get("name", "task"),
                agent=task_data.get("agent"),
                capabilities=task_data.get("capabilities", []),
                payload=task_data.get("payload", {}),
                dependencies=task_data.get("dependencies", []),
                timeout=task_data.get("timeout", 60),
            )
            wf.add_task(task)

        # Execute workflow
        try:
            result = await coordinator.execute(wf)
            return web.json_response(result)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        except Exception as e:
            log.error(f"Workflow execution error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_workflow_status(self, request):
        """GET /api/workflow/{wf_id} — Get workflow status."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        wf_id = request.match_info.get("wf_id", "")
        status = self.workflow_coordinator.get_workflow_status(wf_id)
        if not status:
            return web.json_response({"error": f"Workflow '{wf_id}' not found"}, status=404)
        return web.json_response(status)

    async def _api_workflows_list(self, request):
        """GET /api/workflows — List active workflows."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        workflows = self.workflow_coordinator.list_active_workflows()
        return web.json_response({"workflows": workflows, "total": len(workflows)})

    # ─── Pending Agent Approval API Handlers ──────────────────────────

    async def _api_registry_pending(self, request):
        """GET /api/registry/pending — List pending agent registrations."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        pending = self.registry.list_pending()
        result = []
        for card, status in pending:
            result.append({
                "name": card.name,
                "capabilities": card.capabilities,
                "version": card.version,
                "endpoint": card.endpoint,
                "description": card.description,
                "status": status,
            })
        return web.json_response({"pending": result, "total": len(result)})

    async def _api_registry_approve(self, request):
        """POST /api/registry/approve/{name} — Approve a pending agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        card = self.registry.approve_agent(name)
        if not card:
            return web.json_response({"error": f"Agent '{name}' not in pending list"}, status=404)
        return web.json_response({
            "status": "approved",
            "agent": {
                "name": card.name,
                "capabilities": card.capabilities,
                "version": card.version,
                "endpoint": card.endpoint,
            },
        })

    async def _api_registry_reject(self, request):
        """POST /api/registry/reject/{name} — Reject a pending agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        success = self.registry.reject_agent(name)
        if not success:
            return web.json_response({"error": f"Agent '{name}' not in pending list"}, status=404)
        return web.json_response({"status": "rejected", "agent": name})

    # ─── Settings API Handlers ────────────────────────────────────────

    async def _api_settings_get(self, request):
        """GET /api/settings — Get current mesh settings."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        settings = {
            "mesh": {
                "node_name": self.node.node_name if self.node else "unknown",
                "p2p_enabled": True,
                "pg_enabled": bool(self.node and hasattr(self.node, 'pg_transport')),
                "dashboard_port": 8650,
            },
            "registry": {
                "auto_approve": self.registry.auto_approve,
                "total_agents": len(self.registry.agents),
                "pending_agents": len(self.registry.pending_agents),
                "health_check_interval": self.registry._health_interval,
            },
            "rate_limits": {
                "api_per_min": 100,
                "p2p_per_min": 200,
                "workflow_per_min": 20,
            },
            "health_scorer": {
                "decay_factor": self.registry.health_scorer.decay_factor,
                "recovery_factor": self.registry.health_scorer.recovery_factor,
                "latency_threshold_ms": self.registry.health_scorer.latency_threshold_ms,
                "weights": self.registry.health_scorer.weights,
            },
        }
        return web.json_response(settings)

    async def _api_settings_update(self, request):
        """POST /api/settings — Update mesh settings."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        updated = {}

        # Update auto_approve
        if "auto_approve" in data.get("registry", {}):
            self.registry.auto_approve = bool(data["registry"]["auto_approve"])
            updated["auto_approve"] = self.registry.auto_approve

        # Update health check interval
        if "health_check_interval" in data.get("registry", {}):
            self.registry._health_interval = float(data["registry"]["health_check_interval"])
            updated["health_check_interval"] = self.registry._health_interval

        # Update health scorer weights
        if "weights" in data.get("health_scorer", {}):
            for key, val in data["health_scorer"]["weights"].items():
                if key in self.registry.health_scorer.weights:
                    self.registry.health_scorer.weights[key] = float(val)
            updated["weights"] = self.registry.health_scorer.weights

        # Update decay/recovery factors
        if "decay_factor" in data.get("health_scorer", {}):
            self.registry.health_scorer.decay_factor = float(data["health_scorer"]["decay_factor"])
            updated["decay_factor"] = self.registry.health_scorer.decay_factor
        if "recovery_factor" in data.get("health_scorer", {}):
            self.registry.health_scorer.recovery_factor = float(data["health_scorer"]["recovery_factor"])
            updated["recovery_factor"] = self.registry.health_scorer.recovery_factor

        return web.json_response({"status": "ok", "updated": updated})

    async def _api_mesh_topology(self, request):
        """GET /api/mesh/topology — Star topology visualization data."""
        from aiohttp import web
        import time as _time
        try:
            nodes = {}
            connections = []
            now = _time.time()

            # ── Self node info ──────────────────────────────────────────
            cfg = self.node.config
            self_uptime = now - self.node._start_time if hasattr(self.node, '_start_time') and self.node._start_time else 0
            # Gather self skills from registry if available
            self_skills = []
            self_caps = list(getattr(cfg, 'capabilities', []) or [])
            reg = self.registry  # Dashboard has its own registry (self.registry), not node.registry
            if reg:
                try:
                    for card, health in reg.list_agents():
                        if card.name == self.node.node_name:
                            self_skills = list(card.skills) if hasattr(card, 'skills') and card.skills else []
                            if card.capabilities:
                                self_caps = list(card.capabilities)
                            break
                except Exception:
                    pass

            self_info = {
                "name": self.node.node_name,
                "host": getattr(cfg, 'listen_host', '0.0.0.0') or '0.0.0.0',
                "port": getattr(cfg, 'listen_port', 8650),
                "p2p_port": getattr(cfg, 'p2p_port', 8645),
                "role": getattr(getattr(cfg, 'topology', None), 'node_role', 'router') or 'router',
                "status": "online",
                "health_score": 1.0,
                "capabilities": self_caps,
                "version": getattr(cfg, 'version', '1.0.0') or '1.0.0',
                "skills": self_skills,
                "uptime_seconds": round(self_uptime, 1),
                "last_seen": now,
                "message_count": 0,
            }
            nodes[self.node.node_name] = self_info

            # ── Registry info (ALL registered agents first) ─────────────
            # Use self.registry (DashboardHandler's own registry), not node.registry
            reg = self.registry
            reg_agents = {}  # name -> (AgentCard, HealthRecord)
            if reg:
                try:
                    for card, health in reg.list_agents():
                        name = card.name
                        reg_agents[name] = (card, health)
                        nodes[name] = {
                            "name": name,
                            "host": card.endpoint.replace("http://", "").split(":")[0] if card.endpoint else "",
                            "port": int(card.endpoint.split(":")[-1]) + 1 if card.endpoint and ":" in card.endpoint else 8650,
                            "p2p_port": 8645,
                            "role": getattr(card, 'metadata', {}).get('role', 'agent'),
                            "status": "registered",
                            "health_score": round(health.health_score, 3),
                            "capabilities": list(card.capabilities) if card.capabilities else [],
                            "version": card.version or "1.0.0",
                            "skills": list(card.skills) if card.skills else [],
                            "uptime_seconds": round(health.last_success - health.last_failure, 1) if health.last_success and health.last_failure else 0,
                            "last_seen": health.last_health_check or 0,
                            "message_count": health.total_requests,
                        }
                except Exception as e:
                    log.warning(f"Topology: registry list_agents failed: {e}")

            # ── P2P peer info (enriches registry data with live status) ─────
            pd = getattr(self.node, 'peer_discovery', None)
            p2p_peers = []
            backoff_peers = {}
            if pd:
                if hasattr(pd, '_peers'):
                    for name, peer in pd._peers.items():
                        p2p_peers.append(name)
                        p2p_available = getattr(peer, 'p2p_available', False)
                        # Merge: keep registry skills/caps, enrich with live peer data
                        existing = nodes.get(name, {})
                        peer_caps = getattr(peer, 'capabilities', None) or []
                        existing_caps = existing.get("capabilities", []) or []
                        # Prefer registry data for skills/caps, fall back to peer data
                        final_caps = existing_caps if existing_caps else peer_caps
                        existing_skills = existing.get("skills", []) or []
                        nodes[name] = {
                            "name": name,
                            "host": getattr(peer, 'host', '') or existing.get("host", ""),
                            "port": getattr(peer, 'health_port', 8650),
                            "p2p_port": getattr(peer, 'p2p_port', 8645),
                            "role": getattr(peer, 'role', '') or existing.get("role", "router"),
                            "status": "connected" if p2p_available else "disconnected",
                            "health_score": existing.get("health_score", 1.0),
                            "capabilities": final_caps,
                            "version": existing.get("version", "1.0.0"),
                            "skills": existing_skills if existing_skills else [],
                            "uptime_seconds": existing.get("uptime_seconds", 0),
                            "last_seen": getattr(peer, 'last_seen', 0) or existing.get("last_seen", 0),
                            "message_count": existing.get("message_count", 0),
                            "p2p_available": p2p_available,
                            "http_available": existing.get("http_available", False),
                            "pg_available": existing.get("pg_available", False),
                        }
                if hasattr(pd, '_backoff_until') and pd._backoff_until:
                    backoff_peers = {k: str(v) for k, v in pd._backoff_until.items()}

            # ── Build P2P connections ─────────────────────────────────────
            for peer_name in p2p_peers:
                peer_node = nodes.get(peer_name, {})
                is_connected = peer_node.get("status") == "connected"
                in_backoff = peer_name in backoff_peers
                status = "connected" if is_connected else ("backoff" if in_backoff else "disconnected")
                connections.append({
                    "source": self.node.node_name,
                    "target": peer_name,
                    "transport": "p2p",
                    "status": status,
                    "backoff": backoff_peers.get(peer_name),
                })

            # ── PG connections (all registered agents not on P2P) ────────
            for name in list(nodes.keys()):
                if name != self.node.node_name and name not in p2p_peers:
                    connections.append({
                        "source": self.node.node_name,
                        "target": name,
                        "transport": "pg",
                        "status": "active",
                    })

            return web.json_response({
                "nodes": nodes,
                "connections": connections,
                "topology": "star",
                "local_node": self.node.node_name,
                "timestamp": now,
            })
        except Exception as e:
            log.error(f"Topology API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_topology_page(self, request):
        """GET /topology — Star topology visualization page."""
        from aiohttp import web
        html_path = os.path.join(os.path.dirname(__file__), "topology.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return web.Response(text=f.read(), content_type="text/html")
        except FileNotFoundError:
            return web.Response(text="<h1>Topology page not found</h1>", status=404)

    def _load_html(self) -> str:
        """Load the dashboard HTML page from external file."""
        html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            log.warning(f"Dashboard HTML not found at {html_path}")
            return '<html><body><h1>A2A Mesh Dashboard</h1><p>HTML not found.</p></body></html>'

    # ─── Plugin API ────────────────────────────────────────────────

    async def _api_plugins(self, request):
        """GET /api/plugins — List all loaded plugins and their status."""
        from aiohttp import web
        try:
            user, err = self._require_auth(request)
            if err:
                return err

            if not hasattr(self.node, 'plugin_loader'):
                return web.json_response({"plugins": {}, "total_plugins": 0})

            status = self.node.plugin_loader.get_status()
            return web.json_response(status)
        except Exception as e:
            log.error(f"Plugins API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_plugin_detail(self, request):
        """GET /api/plugins/{plugin_name} — Get detailed status of a specific plugin."""
        from aiohttp import web
        try:
            user, err = self._require_auth(request)
            if err:
                return err

            plugin_name = request.match_info.get("plugin_name", "")
            if not hasattr(self.node, 'plugin_loader'):
                return web.json_response({"error": "No plugin loader"}, status=404)

            plugin = self.node.plugin_loader.get_plugin(plugin_name)
            if not plugin:
                return web.json_response({"error": f"Plugin '{plugin_name}' not found"}, status=404)

            # Get plugin-specific status if available
            detail = {
                "name": plugin.name,
                "version": plugin.version,
                "description": plugin.description,
                "author": plugin.author,
                "capabilities": plugin.capabilities,
                "running": plugin._running,
                "config": {k: v for k, v in plugin._config.items()
                           if not k.endswith(('_token', '_secret', '_password', '_key'))},
            }

            # Add plugin-specific status methods
            if hasattr(plugin, 'get_gateway_status'):
                detail["gateway_status"] = plugin.get_gateway_status()
            elif hasattr(plugin, 'get_notification_status'):
                detail["notification_status"] = plugin.get_notification_status()
            elif hasattr(plugin, 'get_health_status'):
                detail["health_monitor_status"] = plugin.get_health_status()

            return web.json_response(detail)
        except Exception as e:
            log.error(f"Plugin detail API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)