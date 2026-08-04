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
from .dashboard_public import DashboardPublicMixin
from .dashboard_auth import DashboardAuthMixin
from .dashboard_diagnostics import DashboardDiagnosticsMixin
from .dashboard_delegations import DashboardDelegationsMixin
from .dashboard_agents import DashboardAgentsMixin
from .dashboard_files import DashboardFilesMixin
from .dashboard_chat import DashboardChatMixin
from .dashboard_admin import DashboardAdminMixin
from .dashboard_skills import DashboardSkillsMixin

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


class DashboardHandler(DashboardPublicMixin, DashboardAuthMixin, DashboardDiagnosticsMixin, DashboardDelegationsMixin, DashboardAgentsMixin, DashboardFilesMixin, DashboardChatMixin, DashboardAdminMixin, DashboardSkillsMixin):
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
        # Alert manager
        from .alert_manager import AlertManager
        self.alert_manager = AlertManager()
        self.rate_limiter = RateLimiter(max_requests=300, window_seconds=60)
        self._users: Dict[str, DashboardUser] = {}
        self._message_history: List[dict] = []
        self._max_history = 100
        self._html_cache: Optional[str] = None  # Cached dashboard HTML
        self._last_wake_agent_time: float = 0.0  # Rate limit: last wake-agent call
        self._wake_agent_cooldown: float = 30.0  # Min seconds between wake-agent calls
        self._wake_agent_in_progress: bool = False  # Prevent concurrent wake-agent calls

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
        app.router.add_get("/api/health/nodes", self._api_health_nodes)
        app.router.add_post("/api/health/record-success/{name}", self._api_health_success)
        app.router.add_post("/api/health/record-failure/{name}", self._api_health_failure)
        # Task cleanup endpoint
        app.router.add_post("/api/tasks/cleanup", self._api_tasks_cleanup)
        # Debug logs endpoints
        app.router.add_get("/api/debug/logs", self._api_debug_logs)
        app.router.add_post("/api/debug/log", self._api_debug_log_create)
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
        # Diagnostics endpoints
        app.router.add_get("/api/diagnostics", self._api_diagnostics)
        app.router.add_get("/api/diagnostics/reports", self._api_diagnostic_reports)
        app.router.add_get("/api/diagnostics/suggestions", self._api_diagnostic_suggestions)
        app.router.add_post("/api/diagnostics/report", self._api_diagnostic_report_generate)
        app.router.add_post("/api/diagnostics/suggest", self._api_diagnostic_suggest)
        app.router.add_patch("/api/diagnostics/suggestions/{id}", self._api_diagnostic_suggestion_update)
        app.router.add_post("/api/diagnostics/auto-implement", self._api_diagnostic_auto_implement)
        # Queue management endpoints
        app.router.add_post("/api/queue/flush", self._api_queue_flush)
        app.router.add_post("/api/queue/cleanup", self._api_queue_cleanup)
        app.router.add_get("/api/queue/stats", self._api_queue_stats)
        # Delegation endpoints
        app.router.add_get("/api/delegations", self._api_delegations_list)
        app.router.add_post("/api/delegations", self._api_delegations_create)
        app.router.add_get("/api/delegations/stats", self._api_delegations_stats)
        app.router.add_get("/api/delegations/available", self._api_delegations_available)
        app.router.add_get("/api/delegations/{task_id}", self._api_delegations_status)
        app.router.add_post("/api/delegations/{task_id}/cancel", self._api_delegations_cancel)
        app.router.add_post("/api/delegations/{task_id}/claim", self._api_delegations_claim)
        app.router.add_post("/api/delegations/{task_id}/reassign", self._api_delegations_reassign)
        app.router.add_post("/api/delegations/{task_id}/note", self._api_delegations_note)
        app.router.add_post("/api/delegations/{task_id}/progress", self._api_delegations_progress)
        app.router.add_get("/api/delegations/{task_id}/files", self._api_delegations_files)
        # Log viewer API
        app.router.add_get("/api/logs", self._api_logs)
        # Shared context API
        app.router.add_get("/api/context", self._api_context_list)
        app.router.add_get("/api/context/{key}", self._api_context_get)
        app.router.add_post("/api/context", self._api_context_set)
        app.router.add_delete("/api/context/{key}", self._api_context_delete)
        # Image generation API (Pollinations.ai proxy)
        app.router.add_post("/api/image/generate", self._api_image_generate)
        app.router.add_get("/api/image/proxy", self._api_image_proxy)
        # Public health endpoint (no auth required)
        app.router.add_get("/api/health", self._api_public_health)
        # Prometheus-compatible metrics endpoint (no auth)
        app.router.add_get("/metrics", self._api_prometheus_metrics)
        # Skills marketplace
        app.router.add_get("/api/skills", self._api_skills_list)
        app.router.add_get("/api/skills/search", self._api_skills_search)
        app.router.add_get("/api/skills/best", self._api_skills_best)
        app.router.add_post("/api/skills/advertise", self._api_skills_advertise)
        app.router.add_delete("/api/skills/{skill_id}", self._api_skills_delete)
        app.router.add_post("/api/skills/{skill_id}/delegate", self._api_skills_delegate)
        app.router.add_post("/api/skills/{skill_id}/rate", self._api_skills_rate)
        # Alert rules
        app.router.add_get("/api/alerts", self._api_alerts_status)
        app.router.add_post("/api/alerts/rules", self._api_alerts_add_rule)
        app.router.add_delete("/api/alerts/rules/{rule_id}", self._api_alerts_delete_rule)
        app.router.add_post("/api/alerts/rules/{rule_id}/toggle", self._api_alerts_toggle_rule)
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
            elif isinstance(obj, float):
                # JSON doesn't support Infinity/-Infinity/NaN — replace with None
                if obj != obj or obj == float('inf') or obj == float('-inf'):
                    return None
                return obj
            elif isinstance(obj, (str, int, bool, type(None))):
                return obj
            elif hasattr(obj, '__dataclass_fields__'):
                return sanitize(obj.__dict__)
            elif hasattr(obj, '__dict__'):
                return sanitize(obj.__dict__)
            else:
                return str(obj)

        return web.json_response(sanitize(status))

    def get_stats(self) -> dict:
        return {
            "connected_users": len(self._users),
            "users": [u.to_dict() for u in self._users.values()],
            "message_history_size": len(self._message_history),
        }

    def _require_owner(self, request):
        """Verify user is owner (admin). Returns (user, error_response)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return user, err
        if user.role != "owner":
            return user, web.json_response({"error": "Owner access required"}, status=403)
        return user, None

    def _load_html(self) -> str:
        """Load the dashboard HTML page from external file (cached)."""
        # Cache HTML in memory — avoid reading file every request
        if self._html_cache is not None:
            return self._html_cache
        html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                self._html_cache = f.read()
                return self._html_cache
        except FileNotFoundError:
            log.warning(f"Dashboard HTML not found at {html_path}")
            return '<html><body><h1>A2A Mesh Dashboard</h1><p>HTML not found.</p></body></html>'

