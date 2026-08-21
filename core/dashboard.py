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
from .dashboard_config import ConfigSyncMixin
from .dashboard_recovery import RecoveryNotesMixin
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



def _serialize_pg_rows(rows):
    """Convert asyncpg Record rows to JSON-safe dicts (datetime → isoformat, UUID → str)."""
    import datetime, uuid
    result = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, (datetime.datetime, datetime.date)):
                d[k] = v.isoformat()
            elif isinstance(v, bytes):
                d[k] = v.decode('utf-8', errors='replace')
            elif isinstance(v, uuid.UUID):
                d[k] = str(v)
            elif isinstance(v, (list, tuple)):
                d[k] = [str(x) if isinstance(x, uuid.UUID) else x for x in v]
            elif v is not None and not isinstance(v, (str, int, float, bool, dict, list)):
                d[k] = str(v)
        result.append(d)
    return result

class DashboardHandler(DashboardPublicMixin, DashboardAuthMixin, DashboardDiagnosticsMixin, DashboardDelegationsMixin, DashboardAgentsMixin, DashboardFilesMixin, DashboardChatMixin, DashboardAdminMixin, DashboardSkillsMixin, ConfigSyncMixin, RecoveryNotesMixin):
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
        self.rate_limiter = RateLimiter(max_requests=600, window_seconds=60)
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
        app.router.add_get("/api/chat/messages", self._api_messages)  # alias for frontend
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
        # Node onboarding
        app.router.add_post("/api/onboard", self._api_onboard_node)
        app.router.add_post("/api/onboard/scan", self._api_onboard_scan)
        app.router.add_post("/api/onboard/reject", self._api_onboard_reject)
        app.router.add_route("GET", "/ws", self._websocket_handler)
        # Agent reply endpoint — agents call this to send replies to the mesh chat
        app.router.add_post("/api/agent-reply", self._api_agent_reply)
        # Wake-agent endpoint — peer nodes call this to wake the local agent
        app.router.add_post("/api/wake-agent", self._api_wake_agent)
        # Agent-to-agent direct messaging — agents send messages to each other
        app.router.add_post("/api/agent-message", self._api_agent_message)
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
        app.router.add_delete("/api/workflow/{wf_id}", self._api_workflow_delete)
        # Pending agent approval endpoints
        app.router.add_get("/api/registry/pending", self._api_registry_pending)
        app.router.add_post("/api/registry/approve/{name}", self._api_registry_approve)
        app.router.add_post("/api/registry/reject/{name}", self._api_registry_reject)
        app.router.add_get("/api/settings", self._api_settings_get)
        app.router.add_post("/api/settings", self._api_settings_update)
        app.router.add_get("/api/mesh/topology", self._api_mesh_topology)
        app.router.add_get("/topology", self._api_topology_page)
        # Lab — project showcase
        app.router.add_get("/lab", self._lab_page)
        # Skills marketplace
        app.router.add_get("/skills", self._skills_page)
        # Kanban — task management
        app.router.add_get("/kanban", self._kanban_page)
        # Marveen Engine — visual dashboard
        app.router.add_get("/marveen", self._marveen_page)
        # Project CRUD API
        app.router.add_get("/api/projects", self._api_projects_list)
        app.router.add_post("/api/projects", self._api_projects_create)
        app.router.add_put("/api/projects/{pid}", self._api_projects_update)
        app.router.add_delete("/api/projects/{pid}", self._api_projects_delete)
        # Project health check
        app.router.add_get("/api/projects/health", self._api_projects_health)
        # Auto-discovery — scan LAN for services
        app.router.add_get("/api/projects/discover", self._api_projects_discover)
        # Sync projects from peer node
        app.router.add_post("/api/projects/sync", self._api_projects_sync)
        # Kanban API
        app.router.add_get("/api/kanban", self._api_kanban_boards)
        app.router.add_post("/api/kanban", self._api_kanban_create_board)
        app.router.add_get("/api/kanban/cards/{card_id}", self._api_kanban_get_card_by_id)
        app.router.add_post("/api/kanban/cards/{card_id}/approve", self._api_kanban_approve_by_card_id)
        app.router.add_delete("/api/kanban/{board_id}", self._api_kanban_delete_board)
        app.router.add_get("/api/kanban/{board_id}", self._api_kanban_get_board)
        app.router.add_post("/api/kanban/{board_id}/cards", self._api_kanban_add_card)
        app.router.add_put("/api/kanban/{board_id}/cards/{card_id}", self._api_kanban_update_card)
        app.router.add_delete("/api/kanban/{board_id}/cards/{card_id}", self._api_kanban_delete_card)
        app.router.add_post("/api/kanban/{board_id}/cards/{card_id}/breakdown", self._api_kanban_breakdown)
        app.router.add_post("/api/kanban/{board_id}/cards/{card_id}/approve", self._api_kanban_approve)
        app.router.add_get("/api/kanban/audit", self._api_kanban_audit)
        # PreCompact audit
        app.router.add_get("/api/precompact/audit", self._api_precompact_audit)
        # Dream Engine
        app.router.add_get("/api/dream", self._api_dream_run)
        app.router.add_get("/api/dream/latest", self._api_dream_latest)
        # Context Guard
        app.router.add_get("/api/context-guard", self._api_context_guard)
        # CostOps
        app.router.add_get("/api/costops/summary", self._api_costops_summary)
        app.router.add_post("/api/costops/budget", self._api_costops_budget)
        app.router.add_get("/api/costops/alerts", self._api_costops_alerts)
        # Team Trust
        app.router.add_get("/api/trust", self._api_trust_graph)
        app.router.add_get("/api/trust/{agent}", self._api_trust_agent)
        app.router.add_post("/api/trust", self._api_trust_set)
        # Prompt Safety
        app.router.add_post("/api/prompt-safety/check", self._api_prompt_safety_check)
        # Model Fallback
        app.router.add_get("/api/model-fallback/{node}", self._api_model_fallback)
        app.router.add_post("/api/model-fallback/error", self._api_model_fallback_error)
        # Pending Retries
        app.router.add_get("/api/pending-retries", self._api_pending_retries)
        # Tool Timeouts
        app.router.add_get("/api/tool-timeouts", self._api_tool_timeouts)
        # Process Lock
        app.router.add_get("/api/process-lock", self._api_process_lock)
        # Remote Enrollment
        app.router.add_get("/api/remote-enroll", self._api_remote_enroll)
        # Auto-Restart
        app.router.add_get("/api/auto-restart", self._api_auto_restart)
        # Context Gate
        app.router.add_get("/api/context-gate", self._api_context_gate)
        # LLM Breakdown
        app.router.add_post("/api/llm-breakdown", self._api_llm_breakdown)
        # Worker Liveness
        app.router.add_get("/api/worker-liveness", self._api_worker_liveness)
        # Stuck Watcher
        app.router.add_get("/api/stuck-watcher", self._api_stuck_watcher)
        # Token Usage
        app.router.add_get("/api/token-usage", self._api_token_usage)
        # Update Preflight
        app.router.add_get("/api/update-preflight", self._api_update_preflight)
        # Store Watcher
        app.router.add_get("/api/store-watcher", self._api_store_watcher)
        # Vault
        app.router.add_get("/api/vault", self._api_vault_status)
        app.router.add_get("/api/vault/list", self._api_vault_list)
        app.router.add_post("/api/vault/store", self._api_vault_store)
        app.router.add_delete("/api/vault/{entry_id}", self._api_vault_delete)
        # Login Throttle
        app.router.add_get("/api/login-throttle", self._api_login_throttle)
        # CSRF Gate
        app.router.add_get("/api/csrf", self._api_csrf_status)
        # Channel Health
        app.router.add_get("/api/channel-health", self._api_channel_health)
        # Federation
        app.router.add_get("/api/federation", self._api_federation_status)
        app.router.add_post("/api/federation/peer", self._api_federation_add)
        app.router.add_delete("/api/federation/peer/{name}", self._api_federation_remove)
        app.router.add_post("/api/federation/connect", self._api_federation_connect)
        app.router.add_post("/api/federation/discover", self._api_federation_discover)
        app.router.add_get("/api/federation/capabilities/{name}", self._api_federation_capabilities)
        app.router.add_post("/api/federation/trust/{name}", self._api_federation_trust)
        app.router.add_get("/api/federation/health/{name}", self._api_federation_health)
        # Model Suggest
        app.router.add_get("/api/model-suggest", self._api_model_suggest)
        app.router.add_get("/api/model-suggest/{node}", self._api_model_suggest_node)
        # Voice Directive
        app.router.add_get("/api/voice", self._api_voice_status)
        app.router.add_post("/api/voice/parse", self._api_voice_parse)
        # Inbox Nudge
        app.router.add_get("/api/inbox-nudge", self._api_inbox_nudge)
        # Memory Boundary
        app.router.add_get("/api/memory-boundary", self._api_memory_boundary)
        # Message Router
        app.router.add_get("/api/message-router", self._api_message_router)
        # Agent Team
        app.router.add_get("/api/team", self._api_team_status)
        app.router.add_post("/api/team/update", self._api_team_update)
        # Cron Scheduler
        app.router.add_get("/api/cron", self._api_cron_status)
        app.router.add_post("/api/cron/add", self._api_cron_add)
        # Update Checker
        app.router.add_get("/api/update-checker", self._api_update_checker)
        # Network Info
        app.router.add_get("/api/network-info", self._api_network_info)
        # Password Hash
        app.router.add_get("/api/auth-status", self._api_auth_status)
        # Sanitize
        app.router.add_get("/api/sanitize", self._api_sanitize)
        # Fleet Transfer
        app.router.add_get("/api/fleet/status", self._api_fleet_status)
        app.router.add_get("/api/fleet/export", self._api_fleet_export)
        app.router.add_post("/api/fleet/import", self._api_fleet_import)
        # Marveen DB
        app.router.add_get("/api/marveen-db/status", self._api_marveen_db_status)
        # Channel Monitor Watchdog
        app.router.add_get("/api/watchdog/status", self._api_watchdog_status)
        # Governance
        app.router.add_get("/api/governance/rules", self._api_governance_rules)
        app.router.add_get("/api/governance/audit", self._api_governance_audit)
        # Desired State
        app.router.add_get("/api/desired-state", self._api_desired_state)
        app.router.add_post("/api/desired-state/add", self._api_desired_state_add)
        app.router.add_post("/api/desired-state/remove", self._api_desired_state_remove)
        # Process Lock
        app.router.add_get("/api/process-lock", self._api_process_lock)
        # Daily Summary
        app.router.add_post("/api/daily-summary/generate", self._api_generate_daily_summary)
        app.router.add_get("/api/task-runs", self._api_task_runs)
        app.router.add_post("/api/kanban/comments", self._api_kanban_add_comment)
        app.router.add_get("/api/kanban/comments/{card_id}", self._api_kanban_get_comments)
        app.router.add_get("/api/kanban/events/{card_id}", self._api_kanban_get_events)
        app.router.add_get("/api/labels", self._api_labels_list)
        app.router.add_post("/api/labels", self._api_labels_create)
        app.router.add_get("/api/daily-logs", self._api_daily_logs)
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
        app.router.add_delete("/api/delegations/{task_id}", self._api_delegations_delete)
        app.router.add_post("/api/delegations/{task_id}/redispatch", self._api_delegations_redispatch)
        # Deploy API
        app.router.add_post("/api/deploy", self._api_deploy)
        # Smart routing API
        app.router.add_post("/api/route", self._api_smart_route)
        # Recovery notes API
        app.router.add_get("/api/recovery-notes", self._api_recovery_notes)
        app.router.add_post("/api/recovery-notes", self._api_recovery_notes)
        app.router.add_post("/api/recovery-notes/{id}/read", self._api_recovery_note_read)
        # Code Review API
        app.router.add_post("/api/code-review", self._api_code_review)
        # Marveen insights API — cost, inbox, context gate, conversation log, dream engine
        app.router.add_get("/api/insights/cost", self._api_insights_cost)
        app.router.add_get("/api/insights/inbox", self._api_insights_inbox)
        app.router.add_get("/api/insights/context-gate", self._api_insights_context_gate)
        app.router.add_get("/api/insights/conversations/{agent}", self._api_insights_conversations)
        app.router.add_get("/api/insights/dream", self._api_insights_dream)
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
        app.router.add_get("/api/skills/stats", self._api_skills_stats)
        app.router.add_get("/api/skills/search", self._api_skills_search)
        app.router.add_get("/api/skills/best", self._api_skills_best)
        app.router.add_post("/api/skills/advertise", self._api_skills_advertise)
        # Broadcast all skills + capabilities to peers
        app.router.add_post("/api/skills/broadcast", self._api_skills_broadcast)
        # Gitea webhook → auto-deploy
        app.router.add_post("/api/webhook/deploy", self._api_webhook_deploy)
        app.router.add_delete("/api/skills/{skill_id}", self._api_skills_delete)
        app.router.add_post("/api/skills/{skill_id}/delegate", self._api_skills_delegate)
        app.router.add_post("/api/skills/{skill_id}/rate", self._api_skills_rate)
        # Skill replication API
        app.router.add_post("/api/skills/{skill_id}/publish", self._api_skills_publish)
        app.router.add_get("/api/skills/{skill_id}/files", self._api_skills_pull)
        app.router.add_post("/api/skills/sync", self._api_skills_sync)
        app.router.add_post("/api/skills/auto-sync", self._api_skills_auto_sync)
        # Config sync API
        app.router.add_get("/api/config/shared", self._api_config_shared_get)
        app.router.add_post("/api/config/shared", self._api_config_shared_set)
        app.router.add_post("/api/config/sync", self._api_config_sync)
        # Alert rules
        app.router.add_get("/api/alerts", self._api_alerts_status)
        app.router.add_get("/api/alerts/delegation", self._api_alerts_delegation)
        # P2P status endpoint
        app.router.add_get("/api/p2p/status", self._api_p2p_status)
        # Memory sync status endpoint
        app.router.add_get("/api/memory/sync/status", self._api_memory_sync_status)
        app.router.add_post("/api/alerts/rules", self._api_alerts_add_rule)
        app.router.add_delete("/api/alerts/rules/{rule_id}", self._api_alerts_delete_rule)
        app.router.add_post("/api/alerts/rules/{rule_id}/toggle", self._api_alerts_toggle_rule)
        # ── Marveen menu API endpoints ──
        app.router.add_get("/api/approvals", self._api_approvals)
        app.router.add_get("/api/activity", self._api_activity)
        app.router.add_get("/api/research", self._api_research)
        # Ideas board (Ötletláda) — submit, vote, status, list
        app.router.add_get("/api/ideas", self._api_ideas_list)
        app.router.add_post("/api/ideas", self._api_ideas_submit)
        app.router.add_post("/api/ideas/{id}/vote", self._api_ideas_vote)
        app.router.add_post("/api/ideas/{id}/status", self._api_ideas_status)
        app.router.add_delete("/api/ideas/{id}", self._api_ideas_delete)
        app.router.add_get("/api/docs", self._api_docs)
        app.router.add_get("/api/connectors", self._api_connectors)
        app.router.add_get("/api/mcp-registry", self._api_mcp_registry)
        app.router.add_post("/api/mcp-install", self._api_mcp_install)
        app.router.add_get("/api/migrate", self._api_migrate)
        app.router.add_get("/api/overview", self._api_overview)
        app.router.add_get("/api/agents-page", self._api_agents_page)
        app.router.add_get("/api/messages-page", self._api_messages_page)
        app.router.add_get("/api/tasks", self._api_tasks)
        app.router.add_get("/api/memory-page", self._api_memory_page)
        app.router.add_get("/api/logs-page", self._api_logs_page)
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
    # ── Marveen menu API handlers ──────────────────────────────

    async def _api_approvals(self, request):
        """Pending node approvals + kanban card approvals."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"pending_nodes": [], "pending_cards": []}
        try:
            if hasattr(self, 'registry'):
                pending = self.registry.list_pending() if hasattr(self.registry, 'list_pending') else []
                result["pending_nodes"] = pending
        except Exception as e:
            result["pending_nodes_error"] = str(e)
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                try:
                    rows = await pool.fetch(
                        "SELECT id, board_id, title, assignee, priority, created_at "
                        "FROM mesh.kanban_cards WHERE status = 'pending_approval' ORDER BY created_at DESC LIMIT 20"
                    )
                    result["pending_cards"] = _serialize_pg_rows(rows)
                except Exception:
                    result["pending_cards"] = []
        except Exception as e:
            result["pending_cards_error"] = str(e)
        return web.json_response(result)

    async def _api_activity(self, request):
        """Recent mesh activity feed from PG — last 30 non-heartbeat messages."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"activities": []}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                rows = await pool.fetch(
                    "SELECT sender, recipient, msg_type, priority, created_at, status "
                    "FROM mesh.mesh_messages "
                    "WHERE msg_type NOT IN ('heartbeat','skills_announcement','diagnostic_report') "
                    "ORDER BY created_at DESC LIMIT 30"
                )
                result["activities"] = _serialize_pg_rows(rows)
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_research(self, request):
        """Research/ideas board — mesh_suggestions + pending delegations."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"ideas": [], "suggestions": []}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                rows = await pool.fetch(
                    "SELECT suggestion_id, node, category, priority, title, description, status, created_at "
                    "FROM mesh.mesh_suggestions ORDER BY created_at DESC LIMIT 20"
                )
                result["suggestions"] = _serialize_pg_rows(rows)
                try:
                    ideas = await pool.fetch(
                        "SELECT id, sender, receiver, task_desc, status, created_at "
                        "FROM shared_delegations WHERE status = 'pending' ORDER BY created_at DESC LIMIT 10"
                    )
                    result["ideas"] = _serialize_pg_rows(ideas)
                except Exception:
                    pass
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_docs(self, request):
        """Documentation viewer."""
        from aiohttp import web
        import os as _os
        user, err = self._require_auth(request)
        if err:
            return err
        docs = []
        mesh_docs_dir = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), "docs")
        if _os.path.isdir(mesh_docs_dir):
            for f in sorted(_os.listdir(mesh_docs_dir)):
                if f.endswith('.md'):
                    docs.append({"name": f, "source": "a2a-mesh", "path": f"{mesh_docs_dir}/{f}"})
        marveen_docs_dir = _os.path.expanduser("~/marveen_repo/docs")
        if _os.path.isdir(marveen_docs_dir):
            for f in sorted(_os.listdir(marveen_docs_dir)):
                if f.endswith('.md'):
                    docs.append({"name": f, "source": "marveen", "path": f"{marveen_docs_dir}/{f}"})
        return web.json_response({"docs": docs, "total": len(docs)})

    async def _api_connectors(self, request):
        """MCP connectors — Registry capabilities + plugin_config PG table."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"connectors": [], "total": 0}
        try:
            caps = set()
            if hasattr(self, 'registry') and self.registry:
                agents = self.registry.list_agents() if hasattr(self.registry, 'list_agents') else []
                for a in agents:
                    if isinstance(a, dict):
                        for c in a.get('capabilities', []):
                            if any(x in c.lower() for x in ['mcp', 'plugin', 'connector', 'tool']):
                                caps.add(c)
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                try:
                    rows = await pool.fetch("SELECT plugin_name FROM plugin_config")
                    for r in rows:
                        caps.add(r['plugin_name'])
                except Exception:
                    pass
            result["connectors"] = list(caps)
            result["total"] = len(caps)
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_mcp_registry(self, request):
        """MCP Registry — collects MCP servers from all mesh nodes.
        
        Reads local config.yaml mcp_servers section, then queries other nodes
        via their health/agent-card endpoints to discover their MCP servers.
        Returns a unified registry grouped by node.
        """
        from aiohttp import web
        import yaml, os as _os
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"nodes": [], "total_servers": 0, "total_tools": 0}
        try:
            # 1. Collect local MCP servers from config.yaml
            local_node = getattr(getattr(self.node, 'config', None), 'node_name', '') or self.node.get_status().get('node', 'unknown')
            local_servers = []
            config_path = _os.path.expanduser("~/.hermes/config.yaml")
            if _os.path.exists(config_path):
                try:
                    with open(config_path) as f:
                        cfg = yaml.safe_load(f) or {}
                    mcp_servers = cfg.get("mcp_servers", {}) or {}
                    for name, conf in mcp_servers.items():
                        if not isinstance(conf, dict):
                            continue
                        entry = {
                            "name": name,
                            "enabled": conf.get("enabled", True),
                            "transport": "streamable_http" if "url" in conf else "stdio",
                            "url": conf.get("url", ""),
                            "command": conf.get("command", ""),
                            "args": conf.get("args", []),
                            "env_keys": list(conf.get("env", {}).keys()) if isinstance(conf.get("env"), dict) else [],
                            "has_credentials": bool(conf.get("env") or conf.get("headers")),
                            "source": "local"
                        }
                        local_servers.append(entry)
                except Exception as e:
                    local_servers.append({"name": "_error", "error": str(e)})
            
            result["nodes"].append({
                "node": local_node,
                "host": getattr(getattr(self.node, 'config', None), 'listen_host', '') or getattr(getattr(self.node, 'config', None), 'host', ''),
                "status": "local",
                "mcp_servers": local_servers
            })
            result["total_servers"] += len([s for s in local_servers if s.get("name") != "_error"])

            # 2. Query other nodes via P2P for their MCP configs
            if hasattr(self.node, 'pg_pool') and self.node.pg_pool:
                try:
                    pool = self.node.pg_pool
                    rows = await pool.fetch("""
                        SELECT DISTINCT ON (sender_agent) sender_agent, content
                        FROM shared_a2a_memory
                        WHERE subject = 'mcp_registry' AND sender_agent != $1
                        ORDER BY sender_agent, created_at DESC
                    """, local_node)
                    for row in rows:
                        try:
                            import json as _json
                            data = _json.loads(row['content']) if isinstance(row['content'], str) else row['content']
                            result["nodes"].append({
                                "node": row['sender_agent'],
                                "host": data.get("host", ""),
                                "status": "remote",
                                "mcp_servers": data.get("servers", [])
                            })
                            result["total_servers"] += len(data.get("servers", []))
                        except Exception:
                            pass
                except Exception:
                    pass

            # 3. Publish our MCP servers to the registry (for other nodes)
            try:
                import json as _json2
                payload = _json2.dumps({
                    "host": getattr(getattr(self.node, 'config', None), 'listen_host', '') or getattr(getattr(self.node, 'config', None), 'host', ''),
                    "servers": [{"name": s["name"], "enabled": s["enabled"], "transport": s["transport"],
                                 "url": s["url"], "command": s["command"], "args": s.get("args", []),
                                 "env_keys": s.get("env_keys", []), "has_credentials": s.get("has_credentials", False)}
                                for s in local_servers if s.get("name") != "_error"]
                })
                if hasattr(self.node, '_publish_memory'):
                    await self.node._publish_memory(local_node, "any", "mcp_registry", payload, priority=1)
            except Exception:
                pass

            result["total_tools"] = result["total_servers"]  # approximate
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_mcp_install(self, request):
        """Install an MCP server config into local config.yaml.
        
        Body: {"name": "server-name", "config": {...mcp config...}}
        """
        from aiohttp import web
        import yaml, os as _os, json as _json, tempfile, shutil
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            body = await request.json()
            name = body.get("name", "").strip()
            config = body.get("config", {})
            if not name or not config:
                return web.json_response({"error": "name and config required"}, status=400)
            
            config_path = _os.path.expanduser("~/.hermes/config.yaml")
            if not _os.path.exists(config_path):
                return web.json_response({"error": "config.yaml not found"}, status=404)
            
            # Backup
            backup_path = config_path + ".mcp-backup"
            shutil.copy2(config_path, backup_path)
            
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
            
            mcp_servers = cfg.setdefault("mcp_servers", {})
            mcp_servers[name] = config
            
            with open(config_path, "w") as f:
                yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)
            
            return web.json_response({
                "success": True, 
                "name": name, 
                "backup": backup_path,
                "message": f"MCP '{name}' added to config.yaml. Restart Hermes to activate."
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_migrate(self, request):
        """Migration tools — node fleet status from PG."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"fleet_status": {}, "capabilities": ["export", "import", "node_migration"]}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                rows = await pool.fetch(
                    "SELECT node_name, host, p2p_port, status, last_heartbeat "
                    "FROM mesh.mesh_nodes ORDER BY node_name"
                )
                result["fleet_status"]["nodes"] = _serialize_pg_rows(rows)
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_overview(self, request):
        """Overview page — node stats, peer count, task summary, recent activity."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"nodes": [], "peers": 0, "tasks_total": 0, "tasks_pending": 0, "recent_activity": []}
        try:
            if hasattr(self, 'node') and self.node:
                status = self.node.get_status()
                result["peers"] = status.get("peers", {}).get("connected", 0)
                result["peers_total"] = status.get("peers", {}).get("known", 0)
                result["uptime"] = status.get("uptime_seconds", 0)
                result["node_name"] = status.get("node", "")
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                try:
                    rows = await pool.fetch(
                        "SELECT node_name, status, last_heartbeat FROM mesh.mesh_nodes ORDER BY node_name"
                    )
                    result["nodes"] = _serialize_pg_rows(rows)
                except Exception:
                    pass
                try:
                    row = await pool.fetchrow("SELECT count(*) as c FROM shared_delegations")
                    result["tasks_total"] = row["c"] if row else 0
                    row2 = await pool.fetchrow("SELECT count(*) as c FROM shared_delegations WHERE status='pending'")
                    result["tasks_pending"] = row2["c"] if row2 else 0
                except Exception:
                    pass
                try:
                    act = await pool.fetch(
                        "SELECT sender, recipient, msg_type, created_at FROM mesh.mesh_messages "
                        "WHERE msg_type NOT IN ('heartbeat','skills_announcement','diagnostic_report') "
                        "ORDER BY created_at DESC LIMIT 10"
                    )
                    result["recent_activity"] = _serialize_pg_rows(act)
                except Exception:
                    pass
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_agents_page(self, request):
        """Agents page — mesh node list with capabilities, status, skills (Marveen menu)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"agents": []}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                rows = await pool.fetch(
                    "SELECT node_name, role, status, host, p2p_port, last_heartbeat, "
                    "capabilities, skills, version "
                    "FROM mesh.mesh_nodes ORDER BY node_name"
                )
                result["agents"] = _serialize_pg_rows(rows)
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_messages_page(self, request):
        """Messages page — browse A2A messages with filters (metadata only, no payload)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"messages": [], "total": 0}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                rows = await pool.fetch(
                    "SELECT id, sender, recipient, msg_type, priority, status, created_at "
                    "FROM mesh.mesh_messages ORDER BY created_at DESC LIMIT 50"
                )
                result["messages"] = _serialize_pg_rows(rows)
                result["total"] = len(rows)
                try:
                    counts = await pool.fetch(
                        "SELECT msg_type, count(*) as c FROM mesh.mesh_messages GROUP BY msg_type ORDER BY c DESC"
                    )
                    result["type_counts"] = _serialize_pg_rows(counts)
                except Exception:
                    pass
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_skills(self, request):
        """Skills page — mesh skill registry from PG."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"skills": [], "total": 0}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                rows = await pool.fetch(
                    "SELECT skill_name, display_name, agent_name, status, tags, "
                    "cost, avg_latency_ms, success_rate, description "
                    "FROM mesh.mesh_skills ORDER BY agent_name, skill_name"
                )
                all_skills = []
                for r in rows:
                    row = dict(r)  # Convert Record to dict for safe access
                    ag = row.get("agent_name") or row.get("agent") or "—"
                    sk = row.get("skill_name") or row.get("skill") or "—"
                    dn = row.get("display_name") or sk
                    all_skills.append({
                        "node": ag,
                        "skill": sk,
                        "display_name": dn,
                        "description": row.get("description") or "",
                        "status": row.get("status") or "active",
                        "tags": list(row["tags"]) if row.get("tags") else [],
                        "cost": row.get("cost") or 0,
                        "avg_latency_ms": row.get("avg_latency_ms") or 0,
                        "success_rate": row.get("success_rate") or 0
                    })
                result["skills"] = all_skills
                result["total"] = len(all_skills)
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_tasks(self, request):
        """Scheduled tasks page — cron jobs + task_runs from PG."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"tasks": [], "total": 0}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                try:
                    rows = await pool.fetch(
                        "SELECT id, from_agent, to_agent, subject, status, priority, "
                        "created_at, accepted_at, completed_at, task_type "
                        "FROM shared_delegations ORDER BY created_at DESC LIMIT 30"
                    )
                    result["tasks"] = _serialize_pg_rows(rows)
                    result["total"] = len(rows)
                except Exception as te:
                    result["error"] = "tasks query: " + str(te)
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_memory_page(self, request):
        """Memory page — shared_a2a_memory grouped by type (Marveen-style cards)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        result = {"memories": [], "categories": {}, "total": 0}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool:
                # Get non-heartbeat memories, grouped by memory_type
                rows = await pool.fetch(
                    "SELECT id, sender_agent, recipient_agent, subject, content, "
                    "memory_type, priority, status, created_at, read_at, message_type, metadata "
                    "FROM shared_a2a_memory "
                    "WHERE memory_type != 'heartbeat' AND message_type != 'heartbeat' "
                    "ORDER BY created_at DESC LIMIT 100"
                )
                serialized = _serialize_pg_rows(rows)
                # Group by memory_type
                categories = {}
                for m in serialized:
                    cat = m.get("memory_type") or m.get("message_type") or "other"
                    if cat not in categories:
                        categories[cat] = []
                    categories[cat].append(m)
                result["memories"] = serialized
                result["categories"] = {}
                for cat, items in categories.items():
                    result["categories"][cat] = {"count": len(items), "items": items}
                result["total"] = len(serialized)
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_logs_page(self, request):
        """Logs page — delegation history + node health, clickable entries."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        log_type = request.query.get("type", "all")
        limit = min(int(request.query.get("limit", "50")), 200)
        result = {"logs": [], "total": 0, "type": log_type}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool:
                entries = []
                if log_type in ("delegation", "all"):
                    rows = await pool.fetch(
                        "SELECT id, from_agent, to_agent, subject, status, priority, "
                        "retry_count, assigned_agent, created_at, completed_at, task_type "
                        "FROM shared_delegations ORDER BY created_at DESC LIMIT $1",
                        limit
                    )
                    for r in _serialize_pg_rows(rows):
                        r["log_type"] = "delegation"
                        entries.append(r)
                if log_type in ("health", "all"):
                    try:
                        rows = await pool.fetch(
                            "SELECT node_name, status, cpu_pct, memory_pct, disk_pct, "
                            "last_seen, updated_at "
                            "FROM mesh_node_health ORDER BY updated_at DESC LIMIT $1",
                            limit
                        )
                        for r in _serialize_pg_rows(rows):
                            r["log_type"] = "health"
                            entries.append(r)
                    except Exception:
                        pass  # mesh_node_health may not exist
                # Sort by created_at/updated_at descending
                entries.sort(key=lambda x: x.get("created_at") or x.get("updated_at") or "", reverse=True)
                result["logs"] = entries[:limit]
                result["total"] = len(result["logs"])
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)
