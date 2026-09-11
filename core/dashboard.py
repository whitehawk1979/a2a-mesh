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

from .capsules import (
    retrieve_capsules, format_capsules_for_prompt, store_capsule,
    extract_topic_from_prompt, summarize_conversation, TOPIC_SWITCH_MARKERS,
    retrieve_engramms, format_engramms_for_prompt,
    increment_capsule_retrieval_count, check_and_promote_capsules,
    check_and_generate_skills,
)

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
from .dashboard_chat import handle_chat_send  # chat handler functions
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

class DashboardHandler(DashboardPublicMixin, DashboardAuthMixin, DashboardDiagnosticsMixin, DashboardDelegationsMixin, DashboardAgentsMixin, DashboardFilesMixin, DashboardAdminMixin, DashboardSkillsMixin, ConfigSyncMixin, RecoveryNotesMixin):
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
        self._wake_agent_cooldown: float = 8.0  # Anti-spam: 8s between agent replies
        self._wake_agent_in_progress: bool = False  # Prevent concurrent wake-agent calls
        
        # ── In-memory directive counters (real-time, not PG-dependent) ──
        self._agent_msg_counts: Dict[str, int] = {}  # {agent_name: count} per topic
        self._total_agent_msgs: int = 0  # Total agent messages in current topic
        self._current_topic_id: Optional[str] = None  # Topic ID for resetting counters

    def register_routes(self, app):
        """Register dashboard routes on an existing aiohttp app."""
        app.router.add_get("/", self._dashboard_page)
        app.router.add_get("/dashboard", self._dashboard_page)
        app.router.add_get("/dashboard.js", self._serve_dashboard_js)
        app.router.add_get("/api/status", self._api_status)
        app.router.add_get("/api/messages", self._api_messages)
        app.router.add_get("/api/messages/incoming", self._api_messages_incoming)
        app.router.add_get("/api/agents", self._api_agents)
        app.router.add_post("/api/send", self._api_send)
        app.router.add_post("/api/send-file", self._api_send_file)
        # Per-user chat system
        app.router.add_post("/api/chat/send", self._api_chat_send)
        # Agent-to-agent DM (proactive)
        app.router.add_post("/api/agent-dm", self._api_agent_dm)
        app.router.add_get("/api/chat/messages", self._api_chat_messages)
        app.router.add_get("/api/chat/inbox", self._api_chat_inbox)
        app.router.add_post("/api/chat/read", self._api_chat_mark_read)
        app.router.add_get("/api/chat/contacts", self._api_chat_contacts)
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
        # Session management (owner only)
        app.router.add_get("/api/auth/sessions", self._api_auth_sessions)
        app.router.add_get("/api/auth/session-timeout", self._api_auth_session_timeout)
        app.router.add_post("/api/auth/session-timeout", self._api_auth_session_timeout)
        app.router.add_post("/api/auth/revoke-session", self._api_auth_revoke_session)
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
        app.router.add_get("/api/vault/mesh", self._api_vault_mesh)
        app.router.add_get("/api/vault/remote/{node}", self._api_vault_remote_list)
        app.router.add_post("/api/vault/remote/{node}/get", self._api_vault_remote_get)
        app.router.add_post("/api/vault/share", self._api_vault_share)
        app.router.add_post("/api/vault/remote/{node}/delete", self._api_vault_remote_delete)
        app.router.add_post("/api/vault/remote/{node}/store", self._api_vault_remote_store)
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
        app.router.add_get("/api/memory-search", self._api_memory_search)
        app.router.add_get("/api/memory-stats", self._api_memory_stats)
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
        app.router.add_post("/api/update-pull", self._api_update_pull)
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
        # Reflections (eszmefuttatasok)
        app.router.add_get("/api/reflections", self._api_reflections)
        app.router.add_get("/api/reflections/export", self._api_reflections_export)
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
        app.router.add_post("/api/insights/dream/trigger", self._api_insights_dream_trigger)
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
        app.router.add_get("/api/ideas/{id}/comments", self._api_idea_comments)
        app.router.add_post("/api/ideas/{id}/comments", self._api_idea_comment_add)
        app.router.add_post("/api/ideas/{id}/promote-agent", self._api_idea_promote_agent)
        app.router.add_post("/api/ideas/import-diagnostics", self._api_ideas_diagnostic_import)
        app.router.add_post("/api/ideas/{id}/implement", self._api_idea_implement)
        app.router.add_get("/api/docs", self._api_docs)
        app.router.add_get("/api/docs/content", self._api_docs_content)
        app.router.add_get("/api/connectors", self._api_connectors)
        app.router.add_get("/api/mcp-registry", self._api_mcp_registry)
        app.router.add_post("/api/mcp-install", self._api_mcp_install)
        app.router.add_get("/api/migrate", self._api_migrate)
        app.router.add_get("/api/overview", self._api_overview)
        app.router.add_get("/api/agents-page", self._api_agents_page)
        app.router.add_get("/api/messages-page", self._api_messages_page)
        app.router.add_get("/api/messages/detail/{id}", self._api_message_detail)
        app.router.add_get("/api/tasks", self._api_tasks)
        app.router.add_get("/api/memory-page", self._api_memory_page)
        app.router.add_get("/api/logs-page", self._api_logs_page)
    def _require_auth(self, request):
        """Extract and verify auth token from request. Returns (user, error_response).

        Supports X-Mesh-Token header for mesh-internal API calls (transport, wake-agent).
        This bypasses user auth — the shared secret authenticates mesh nodes only.
        """
        from aiohttp import web

        # ── Mesh-internal bypass: X-Mesh-Token header ──
        mesh_token = request.headers.get("X-Mesh-Token", "")
        if mesh_token == "mesh-wake-secret-2026":
            # Return a synthetic system user for mesh-internal calls
            class MeshSystemUser:
                user_id = 0
                username = "mesh"
                role = "admin"
                is_system = True
                def to_dict(self):
                    return {"username": "mesh", "role": "admin", "is_system": True}
            return MeshSystemUser(), None

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

    async def _serve_dashboard_js(self, request):
        """Serve the dashboard JS file (cacheable)."""
        from aiohttp import web
        import os as _os
        js_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "dashboard.js")
        try:
            with open(js_path, "r", encoding="utf-8") as f:
                js = f.read()
            return web.Response(
                text=js,
                content_type="application/javascript",
                headers={
                    "Cache-Control": "public, max-age=3600",
                    "X-Content-Type-Options": "nosniff",
                },
            )
        except FileNotFoundError:
            return web.Response(text="// JS file not found", status=404)

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
        """Documentation viewer — A2A Mesh docs (repo docs/ + root .md files)."""
        from aiohttp import web
        import os as _os
        user, err = self._require_auth(request)
        if err:
            return err
        docs = []
        base = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        mesh_docs_dir = _os.path.join(base, "docs")
        if _os.path.isdir(mesh_docs_dir):
            for f in sorted(_os.listdir(mesh_docs_dir)):
                if f.endswith('.md'):
                    docs.append({"name": f, "source": "a2a-mesh", "path": f"docs/{f}"})
        # Root-level A2A Mesh documentation (README, STATUS, plans)
        for f in sorted(_os.listdir(base)):
            if f.endswith('.md') and f not in ('CLAUDE.md',) and 'marveen' not in f.lower():
                docs.append({"name": f, "source": "a2a-mesh (root)", "path": f})
        return web.json_response({"docs": docs, "total": len(docs)})

    async def _api_docs_content(self, request):
        """Serve a documentation file's content as text (A2A Mesh sources only)."""
        from aiohttp import web
        import os as _os
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.query.get('name', '')
        source = request.query.get('source', '')
        if not name or '..' in name or '/' in name:
            return web.json_response({"error": "invalid name"}, status=400)
        base = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        if source.startswith('a2a-mesh'):
            candidates = [_os.path.join(base, "docs", name), _os.path.join(base, name)]
            for path in candidates:
                if _os.path.isfile(path):
                    try:
                        with open(path, 'r', errors='replace') as fh:
                            content = fh.read()
                        return web.json_response({"name": name, "source": source, "content": content[:200000]})
                    except Exception as e:
                        return web.json_response({"error": str(e)}, status=500)
        return web.json_response({"error": "not found"}, status=404)

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
                        "SELECT node_name, status, last_heartbeat, version FROM mesh.mesh_nodes ORDER BY node_name"
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


    async def _api_messages(self, request):
        """Return recent messages — local history + PG messages from other agents.

        Supports channel filtering via ?channel=general|dm:<agent_name>
        """
        from aiohttp import web
        import traceback as tb
        try:
            limit = min(int(request.query.get("limit", 50)), 200)
            channel = request.query.get("channel", None)

            # Local messages
            local_messages = self._message_history[-limit:]
            safe_local = []
            for i, m in enumerate(local_messages):
                try:
                    if not isinstance(m, dict):
                        continue
                    mid = m.get("id")
                    if mid is not None:
                        m["id"] = str(mid)
                    else:
                        m["id"] = f"local_{i}"
                    mts = m.get("timestamp")
                    if mts is None:
                        m["timestamp"] = ""
                    elif not isinstance(mts, str):
                        m["timestamp"] = str(mts)
                    safe_local.append(m)
                except Exception:
                    continue
            local_messages = safe_local

            # Fetch from PG
            pg_messages = []
            try:
                pool = getattr(self.node, '_pg_pool', None)
                if pool and hasattr(pool, 'is_connected') and pool.is_connected():
                    where_clauses = []
                    params = []
                    where_clauses.append("msg_type NOT IN ('heartbeat', 'memory_sync', 'diagnostic_report', 'skills_announcement', 'config_suggestion', 'ack', 'peer_offline', 'peer_online', 'node_join', 'node_leave')")
                    if channel == "general":
                        where_clauses.append("(recipient = 'broadcast' OR msg_type IN ('agent_reply', 'directive'))")
                    elif channel and channel.startswith("dm:"):
                        dm_agent = channel[3:]
                        where_clauses.append("(recipient = %s OR sender = %s)")
                        params.extend([dm_agent, dm_agent])
                    where_sql = " AND ".join(where_clauses)
                    rows = await pool.fetch(
                        f"SELECT id, sender, recipient, msg_type, priority, payload, created_at, status "
                        f"FROM mesh.mesh_messages WHERE {where_sql} ORDER BY created_at DESC LIMIT %s",
                        params + [limit]
                    )
                    import json as _json
                    for row in rows:
                        row_dict = dict(row) if hasattr(row, 'keys') else {}
                        if not row_dict:
                            # asyncpg Record
                            row_dict = {k: row[i] for i, k in enumerate(row.keys())} if hasattr(row, 'keys') else {}
                        msg_id = row_dict.get('id', '')
                        sender = row_dict.get('sender', '')
                        recipient = row_dict.get('recipient', '')
                        msg_type = row_dict.get('msg_type', '')
                        payload = row_dict.get('payload', '')
                        created_at = row_dict.get('created_at')
                        status = row_dict.get('status', 'unknown')
                        if isinstance(payload, bytes):
                            payload = payload.decode("utf-8", errors="replace")
                        try:
                            payload_data = _json.loads(payload) if isinstance(payload, str) else payload
                        except (ValueError, TypeError):
                            payload_data = {"text": str(payload)}
                        pg_messages.append({
                            "id": str(msg_id),
                            "sender": sender,
                            "recipient": recipient,
                            "type": msg_type,
                            "priority": row_dict.get('priority', 5),
                            "content": payload_data.get("text", "") if isinstance(payload_data, dict) else str(payload),
                            "username": payload_data.get("username", sender) if isinstance(payload_data, dict) else sender,
                            "timestamp": created_at.isoformat() if created_at and hasattr(created_at, 'isoformat') else str(created_at or ""),
                            "status": status,
                            "source": "mesh",
                        })
            except Exception as e:
                import logging
                logging.getLogger('a2a_mesh').warning(f"Failed to fetch PG messages: {e}")

            # Filter local messages by channel
            def matches_channel(msg, ch):
                msg_type = msg.get("type", "")
                if msg_type in ("heartbeat", "memory_sync"):
                    return False
                if msg_type in ("agent_processing", "agent_timeout"):
                    return True
                if ch is None:
                    return True
                recip = msg.get("recipient", "broadcast")
                sender = msg.get("sender", "")
                if ch == "general":
                    return recip == "broadcast" or msg_type in ("agent_reply", "directive")
                elif ch.startswith("dm:"):
                    agent = ch[3:]
                    return sender == agent or recip == agent
                return True

            filtered_local = [m for m in local_messages if matches_channel(m, channel)]

            # Merge + dedup
            all_messages = {m.get("id") or f"local_{i}": m for i, m in enumerate(filtered_local)}
            for m in pg_messages:
                msg_id = m.get("id", "")
                if msg_id and msg_id not in all_messages:
                    all_messages[msg_id] = m

            msg_list = list(all_messages.values())
            for m in msg_list:
                ts = m.get("timestamp")
                if ts is None or not isinstance(ts, str):
                    m["timestamp"] = str(ts) if ts is not None else ""
            msg_list.sort(key=lambda m: m.get("timestamp", "") or "")
            result = msg_list[-limit:]

            return web.json_response({"messages": result, "total": len(msg_list)})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_messages_incoming(self, request):
        """GET /api/messages/incoming — Return messages from other mesh agents."""
        from aiohttp import web
        try:
            since = float(request.query.get("since", 0))
            limit = min(int(request.query.get("limit", 50)), 200)
            sender_filter = request.query.get("sender", None)

            messages = []
            for m in self._message_history:
                try:
                    if not isinstance(m, dict):
                        continue
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
                    if msg_time and msg_time < since:
                        continue
                    if sender_filter and msg_sender != sender_filter:
                        continue
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

    async def _api_delete_message(self, request):
        """Delete a message by ID — requires auth, admin only.

        Deletes from both local history and PG mesh_messages.
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if not getattr(user, "is_admin", False):
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

        # ── In-memory directive counter update ──
        # Topic switch detection — reset counters + create capsule from previous topic
        is_new_topic = any(marker in content for marker in TOPIC_SWITCH_MARKERS)
        if is_new_topic:
            log.info(f"🔔 Topic switch detected in on_mesh_message — creating capsule from previous conversation")
            # Create capsule from previous conversation BEFORE resetting
            try:
                prev_history = self._fetch_chat_history(limit=20, channel="general")
                prev_msgs = []
                for h in prev_history:
                    h_content = h.get('content', '')
                    if any(marker in h_content for marker in TOPIC_SWITCH_MARKERS):
                        break
                    prev_msgs.append(h)
                
                if len(prev_msgs) >= 4:
                    # Extract topic
                    prev_topic = "ismeretlen téma"
                    for h in reversed(prev_msgs):
                        topic = extract_topic_from_prompt(h.get('content', ''))
                        if topic:
                            prev_topic = topic
                            break
                    if not prev_topic or prev_topic == "ismeretlen téma":
                        texts = [h.get('content', '')[:100] for h in prev_msgs[:3]]
                        prev_topic = ' '.join(texts)[:200]
                    
                    summary_msgs = [{'sender': h.get('sender', '?'), 'text': h.get('content', '')} for h in prev_msgs]
                    summary = summarize_conversation(summary_msgs)
                    
                    agent_names_tmp = set()
                    try:
                        for name, _ in self.node.peer_discovery.get_all_peers().items():
                            agent_names_tmp.add(name.lower())
                    except Exception:
                        pass
                    agent_names_tmp.add(self.node.node_name.lower())
                    agents_involved = list(set(h.get('sender', '') for h in prev_msgs if h.get('sender', '').lower() in agent_names_tmp))
                    
                    pg_pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
                    if pg_pool:
                        msg_ids = [h.get('id', 0) for h in prev_msgs]
                        asyncio.ensure_future(store_capsule(
                            pg_pool, prev_topic, summary, agents_involved,
                            min(msg_ids) if msg_ids else 0,
                            max(msg_ids) if msg_ids else 0,
                        ))
                        log.info(f"📚 Capsule creation triggered for topic '{prev_topic[:50]}' ({len(prev_msgs)} msgs)")
            except Exception as e:
                log.warning(f"Capsule creation failed (non-blocking): {e}")

            self._agent_msg_counts = {}
            self._total_agent_msgs = 0
            log.info("📊 Directive counters reset (topic switch detected)")

        # Count agent messages (not human)
        agent_names_set = set()
        try:
            for name, _ in self.node.peer_discovery.get_all_peers().items():
                agent_names_set.add(name.lower())
        except Exception:
            pass
        agent_names_set.add(self.node.node_name.lower())

        if message.sender.lower() in agent_names_set and msg_type not in ("heartbeat", "ack", "memory_sync", "skills_announcement", "ssh_key_sync", "ssh_key_bundle", "config_suggestion", "peer_offline", "peer_online", "node_join", "node_leave", "diagnostic_report"):
            self._agent_msg_counts[message.sender.lower()] = self._agent_msg_counts.get(message.sender.lower(), 0) + 1
            self._total_agent_msgs += 1
            log.info(f"📊 Directive counter: {message.sender}={self._agent_msg_counts[message.sender.lower()]} total={self._total_agent_msgs}")

            # ── Reflection cycle — dynamic interval based on conversation state ──
            from .reflection import run_reflection_cycle, retrieve_reflections, format_past_reflections_for_prompt, get_dynamic_interval
            # Get current dynamic interval (default 5, changes based on last reflection)
            current_interval = getattr(self, '_reflection_interval', 5)
            if self._total_agent_msgs % current_interval == 0 and self._total_agent_msgs >= 3:
                try:
                    # Get recent conversation history
                    recent_history = self._fetch_chat_history(limit=15, channel="general")
                    if recent_history and len(recent_history) >= 3:
                        # Extract topic from conversation (new auto-extraction)
                        from .reflection import extract_topic_from_conversation
                        ref_msgs_raw = [{'sender': h.get('sender', ''), 'content': h.get('content', '')} for h in recent_history]
                        ref_topic = extract_topic_from_conversation(ref_msgs_raw)

                        # Get agents involved
                        ref_agents = list(set(h.get('sender', '') for h in recent_history if h.get('sender', '')))

                        pg_pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
                        if pg_pool:
                            # Run reflection cycle (deterministic + optional deep LLM)
                            ref_prompt, ref_id = await run_reflection_cycle(
                                pg_pool, ref_msgs_raw, ref_topic, ref_agents,
                                ollama_url="http://localhost:11434",
                                enable_deep=True,
                            )
                            if ref_prompt:
                                # Store for injection into next agent wake
                                self._current_reflection = ref_prompt
                                # Update dynamic interval based on reflection findings
                                from .reflection import analyze_conversation
                                findings = analyze_conversation(ref_msgs_raw, ref_topic)
                                self._reflection_interval = get_dynamic_interval(findings)
                                log.info(f"🔍 Reflection cycle complete: id={ref_id}, topic='{ref_topic[:40]}', next interval={self._reflection_interval}")
                except Exception as e:
                    log.warning(f"Reflection cycle failed (non-blocking): {e}")

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

        # ── Persist agent replies to mesh_chat_messages ──
        # Without this, agent replies (tor/morzsa/runa/nova) appear live via WS but
        # VANISH after a page reload — the history pull reads PG only. The history
        # query is per-USER (username = the human who chatted), so store under the
        # chat_username from the payload (falls back to broadcast viewers = sender).
        try:
            _pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if _pool and msg_type in ("agent_reply", "a2a_message") and content:
                _chat_user = payload.get("chat_username") or payload.get("username") or ""
                if _chat_user and _chat_user != message.sender:
                    # Reply to a human chat user — store under their view (DM + broadcast)
                    await _pool.execute(
                        """INSERT INTO mesh.mesh_chat_messages
                           (message_uuid, username, sender, recipient, content, msg_type, status)
                           VALUES ($1, $2, $3, $4, $5, 'chat', 'sent')
                           ON CONFLICT (message_uuid) DO NOTHING""",
                        str(message.id),
                        _chat_user,
                        message.sender or "unknown",
                        payload.get("reply_to") or message.recipient or "broadcast",
                        content[:4000],
                    )
        except Exception as e:
            log.debug(f"Agent reply persist failed (non-blocking): {e}")

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
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if not pool:
                log.warning("Mesh insert failed: no PG pool")
                return
            payload = message.payload if isinstance(message.payload, dict) else {"text": str(message.payload)}
            username = (auth_user.display_name if auth_user else "web_user")
            safe_sender = (message.sender or "unknown")
            payload_json = json.dumps(payload, ensure_ascii=True)

            await pool.execute(
                """INSERT INTO mesh.mesh_messages
                   (id, sender, recipient, msg_type, priority, payload, routing_mode, status, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                   ON CONFLICT (id) DO NOTHING""",
                str(message.id), safe_sender, message.recipient or "broadcast",
                message.type, message.priority, payload_json, "hybrid", "sent"
            )
            log.info(f"Dashboard message {str(message.id)[:8]} inserted into mesh_messages")
        except Exception as e:
            log.warning(f"Mesh insert failed: {e}")

    def _fetch_chat_history(self, limit: int = 10, channel: str = "general") -> list:
        """Fetch recent chat messages from PG for context injection.

        Returns a list of {sender, content, timestamp} dicts — the last N
        non-heartbeat messages from the given channel.
        """
        import psycopg2, json as _json
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
        
        Self-regulating approach: the agent sees the full conversation and
        decides for itself whether to reply, how much to say, and whether
        its point has already been made by someone else.
        """
        # Topic switch detection — clear history to break echo chamber loops
        is_topic_switch = any(marker in content for marker in TOPIC_SWITCH_MARKERS)
        # Use 20 messages for richer context — agent needs to see what's already been said
        history_limit = 0 if is_topic_switch else 20
        history = self._fetch_chat_history(limit=history_limit, channel=channel) if history_limit > 0 else []

        # Known agent names — needed for prompt
        agent_names = set()
        try:
            for name, _ in self.node.peer_discovery.get_all_peers().items():
                agent_names.add(name.lower())
        except Exception:
            pass
        agent_names.add(self.node.node_name.lower())

        # NOTE: Capsule creation is handled in on_mesh_message (async) — not here (sync)

        # Build conversation context — show full history so agent can check for duplicates
        if history:
            chat_lines = []
            for h in history:
                h_sender = h.get('sender', '?')
                h_content = h.get('content', '')[:300]
                if h_sender.lower() in agent_names or h_sender.lower() in ('nova', 'morzsa', 'runa'):
                    chat_lines.append(f"  [{h_sender} 🤖] {h_content}")
                else:
                    chat_lines.append(f"  [{h_sender} 👤] {h_content}")
            chat_context = "\n".join(chat_lines)
        else:
            chat_context = "(nincs előzmény — új téma)"

        is_human = sender.lower() not in agent_names and sender.lower() not in ('nova', 'morzsa', 'runa')
        sender_tag = f"{sender} 👤 emberi felhasználó" if is_human else f"{sender} 🤖 agent"

        # Count how many times this agent has already spoken (from history)
        my_msgs = [h for h in history if h.get('sender', '').lower() == agent_name.lower()]
        my_msg_count = len(my_msgs)

        # ── Self-regulating system prompt ──
        # No external hard limit — the agent decides based on context quality
        self_regulation = (
            "ÖNSZABÁLYOZÁS — Te döntöd el, válaszolsz-e:\n"
            "1. OLVASD EL a fenti beszélgetést figyelmesen.\n"
            "2. DUPLÁZÁS-ELLENŐRZÉS: Ha valaki már említette az érvedet vagy gondolatodat, "
            "NE ismételd el. Csak akkor szólj hozzá, ha ÚJ szempontot, ellenvetést vagy "
            "következtetést tudsz hozzátenni. 'Igen, és pont ezért...' nem új érv.\n"
            "3. RELEVANCIA: Ha a beszélgetés már lefutott az adróddal kapcsolatban és "
            "nincs mit hozzátenned, NE válaszolj. Csend is válasz.\n"
            "4. TÉMAVÁLTÁS: Ha az új üzenet konkrét témát és szerepeket tartalmaz, "
            "kövesd azokat. Ne hivatkozz korábbi témákra.\n"
            "5. Ha úgy érzed, hogy már eleget mondtál és a többi agent tovább vitte "
            "a gondolatot, egy rövid 'NEM VÁLASZTOLSZ' választ adj.\n"
            f"6. Eddig {my_msg_count} üzenetet írtél ebben a témában. "
            f"{'⚠️ Ha már 5+ üzeneted van, CSAK kritikus új információ esetén válaszolj. Ha a vita konklúzió felé tart, NE folytasd.' if my_msg_count >= 5 else 'Ha már 3+ üzeneted van, csak kritikus új információ esetén válaszolj.' if my_msg_count >= 3 else ''}"
            "\n7. KONKLÚZIÓ: Ha a beszélgetés láthatóan lezárult vagy konklúziót ért, "
            "NE adj hozzá újabb érvet — írd: 'NEM VÁLASZTOLSZ'."
        )

        # Anti-echo rule — always active
        anti_echo = (
            "SZABÁLY: Tilos 'igazad van', 'jó pont', 'egyetértek', 'pontosan' "
            "üres értelés. Csak ÚJ érvet, ellenvetést vagy konkrét javaslatot írj. "
            "Ha nincs új mondanivalód, írd: 'NEM VÁLASZTOLSZ'."
        )

        # System instruction
        if is_topic_switch:
            topic_instruction = (
                "⚠️ EZ ÚJ TÉMA — a korábbi beszélgetés LEZÁRVA. "
                "Kövesd az üzenetben megadott szerepeket és szabályokat. "
                "Ne érts egyet a többiekkel — hozz saját, új érveket.\n\n"
                f"{self_regulation}\n{anti_echo}"
            )
        else:
            topic_instruction = (
                "Ha az üzenet emberi felhasználótól van, neki válaszolj. "
                "Ha egy másik agent írt és nem hozzád szól, nem kell válaszolnod. "
                "Ha nem kell válaszolnod, írd: 'NEM VÁLASZTOLSZ'.\n\n"
                f"{self_regulation}\n{anti_echo}"
            )

        # ── Agent DM capability — proactive direct messaging ──
        dm_instruction = (
            "ÜGYNÖK DM (proaktív közvetlen üzenet): Ha egy specifikus agenthez akarsz szólni "
            "(nem mindenkihez), írd a válaszod így: 'DM:célagent:üzenet'. "
            "Például: 'DM:morzsa:ezt a részt neked szánom'. "
            "A rendszer csak a célagentnek küldi el. "
            "Ha a DM után folytatod a broadcast választ, új sorba írd a többi tartalmat.\n\n"
            "FEJLESZTÉSI JAVASLAT (v0.40+): Ha a beszélgetés során felismeresz egy "
            "fejlesztési lehetőséget vagy hibát a mesh-ben, küldj javaslatot így: "
            "'SUGGESTION: cím | leírás | prioritás(low/medium/high)'. "
            "Például: 'SUGGESTION: Kapszula decay túl gyors | A 30 napos decay túl agresszív, 60 javasolt | medium'. "
            "A rendszer PG-be tárolja és DM-ben értesíti Novát. Csak érdemi javaslatokat küldj!"
        )

        # ── Retrieve relevant memory capsules + engramms + reflections (sync — pre-fetched by caller) ──
        capsule_context = getattr(self, '_current_capsules', '')
        engramm_context = getattr(self, '_current_engramms', '')
        reflection_context = getattr(self, '_current_reflection', '')
        memory_block = ""
        if engramm_context:
            memory_block += f"{engramm_context}\n\n"
        if capsule_context:
            memory_block += f"{capsule_context}\n\n"
        if reflection_context:
            memory_block += f"{reflection_context}\n\n"
        prompt = (
            f"Te {agent_name} 🤖 vagy, egy A2A Mesh chat résztvevője. "
            f"Ez egy közös chat session, mint egy Telegram csoport. "
            f"Válaszolj röviden, természetesen, magyarul (max 500 karakter). "
            f"{topic_instruction}\n\n"
            f"{dm_instruction}\n\n"
            f"{memory_block}"
            f"── Beszélgetés eddig ──\n{chat_context}\n\n"
            f"── Új üzenet ──\n[{sender_tag}] {content[:4000]}\n\n"
            f"Válaszodat sima szövegként írd (stdout). "
            f"NE használj curl-t vagy tool-okat — a rendszer automatikusan elküldi."
        )
        return prompt


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
        from .dashboard import DashboardUser
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

    async def _api_send(self, request):
        """Send an A2A message to a peer or broadcast."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
            recipient = data.get("recipient", "broadcast")
            msg_type = data.get("msg_type", "a2a_message")
            # Accept both "text" and "content" (JS sends content)
            text = data.get("text", "") or data.get("content", "")
            priority = int(data.get("priority", 5))
            if not text:
                return web.json_response({"error": "text is required"}, status=400)
            # Include sender info from authenticated user
            sender = getattr(user, "display_name", "dashboard") if user else "dashboard"
            payload = {"text": text, "subject": data.get("subject", text[:80]), "sender_display": sender}
            if recipient == "broadcast":
                result = await self.node.broadcast(msg_type, payload, priority=priority)
            else:
                result = await self.node.send_direct(recipient, msg_type, payload, priority=priority)
            return web.json_response({
                "ok": True,
                "message_id": getattr(result, "message_id", ""),
                "recipient": recipient,
                "status": str(getattr(result, "status", "sent"))
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # NOTE: _api_send_file lives in DashboardFilesMixin (multipart upload).
    # The old JSON-body variant here shadowed it via MRO — removed.

    async def _api_message_detail(self, request):
        """Get full message detail by ID (including payload)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        msg_id = request.match_info.get("id", "")
        result = {"message": None}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool:
                row = await pool.fetchrow(
                    "SELECT id, sender, recipient, msg_type, priority, status, "
                    "payload, created_at FROM mesh.mesh_messages WHERE id = $1",
                    msg_id
                )
                if row:
                    result["message"] = _serialize_pg_rows([row])[0]
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
        """Logs page — delegation history + node health, with filtering + export."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        log_type = request.query.get("type", "all")
        node_filter = request.query.get("node", "").strip()
        status_filter = request.query.get("status", "").strip()
        search_query = request.query.get("q", "").strip()
        date_from = request.query.get("from", "").strip()  # ISO date
        date_to = request.query.get("to", "").strip()
        export = request.query.get("export", "")  # "csv" or "json"
        limit = min(int(request.query.get("limit", "50")), 500)
        result = {"logs": [], "total": 0, "type": log_type, "filters": {
            "node": node_filter, "status": status_filter, "q": search_query,
            "from": date_from, "to": date_to
        }}
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if pool:
                entries = []
                # Build delegation query with filters
                if log_type in ("delegation", "all"):
                    conditions = []
                    params = []
                    idx = 1
                    if node_filter:
                        conditions.append("(from_agent = $" + str(idx) + " OR to_agent = $" + str(idx) + " OR assigned_agent = $" + str(idx) + ")")
                        params.append(node_filter)
                        idx += 1
                    if status_filter:
                        conditions.append("status = $" + str(idx))
                        params.append(status_filter)
                        idx += 1
                    if search_query:
                        conditions.append("subject ILIKE $" + str(idx))
                        params.append("%" + search_query + "%")
                        idx += 1
                    if date_from:
                        conditions.append("created_at >= $" + str(idx))
                        params.append(date_from)
                        idx += 1
                    if date_to:
                        conditions.append("created_at <= $" + str(idx))
                        params.append(date_to + " 23:59:59")
                        idx += 1
                    where_clause = (" WHERE " + " AND ".join(conditions)) if conditions else ""
                    params.append(limit)
                    sql = ("SELECT id, from_agent, to_agent, subject, status, priority, "
                           "retry_count, assigned_agent, created_at, completed_at, task_type "
                           "FROM shared_delegations" + where_clause +
                           " ORDER BY created_at DESC LIMIT $" + str(idx))
                    rows = await pool.fetch(sql, *params)
                    for r in _serialize_pg_rows(rows):
                        r["log_type"] = "delegation"
                        entries.append(r)
                # Build health query with filters
                if log_type in ("health", "all"):
                    try:
                        conditions = []
                        params = []
                        idx = 1
                        if node_filter:
                            conditions.append("node_name = $" + str(idx))
                            params.append(node_filter)
                            idx += 1
                        if status_filter:
                            conditions.append("status = $" + str(idx))
                            params.append(status_filter)
                            idx += 1
                        if date_from:
                            conditions.append("updated_at >= $" + str(idx))
                            params.append(date_from)
                            idx += 1
                        if date_to:
                            conditions.append("updated_at <= $" + str(idx))
                            params.append(date_to + " 23:59:59")
                            idx += 1
                        where_clause = (" WHERE " + " AND ".join(conditions)) if conditions else ""
                        params.append(limit)
                        sql = ("SELECT node_name, status, cpu_pct, memory_pct, disk_pct, "
                               "last_seen, updated_at "
                               "FROM mesh_node_health" + where_clause +
                               " ORDER BY updated_at DESC LIMIT $" + str(idx))
                        rows = await pool.fetch(sql, *params)
                        for r in _serialize_pg_rows(rows):
                            r["log_type"] = "health"
                            entries.append(r)
                    except Exception:
                        pass  # mesh_node_health may not exist
                # Sort by created_at/updated_at descending
                entries.sort(key=lambda x: x.get("created_at") or x.get("updated_at") or "", reverse=True)
                # If node_filter applied to health but not delegation, filter mixed results
                if node_filter and log_type == "all":
                    entries = [e for e in entries if e.get("log_type") == "delegation" or e.get("node_name") == node_filter]
                result["logs"] = entries[:limit]
                result["total"] = len(result["logs"])
                # Export
                if export == "csv":
                    import csv, io
                    buf = io.StringIO()
                    writer = csv.writer(buf)
                    writer.writerow(["type", "id", "from", "to", "subject", "status", "priority", "created_at", "completed_at"])
                    for e in result["logs"]:
                        writer.writerow([
                            e.get("log_type", ""),
                            e.get("id", e.get("node_name", "")),
                            e.get("from_agent", ""),
                            e.get("to_agent", e.get("node_name", "")),
                            e.get("subject", ""),
                            e.get("status", ""),
                            e.get("priority", ""),
                            e.get("created_at", e.get("updated_at", "")),
                            e.get("completed_at", "")
                        ])
                    return web.Response(text=buf.getvalue(), content_type="text/csv",
                                         headers={"Content-Disposition": "attachment; filename=mesh_logs.csv"})
                elif export == "json":
                    return web.json_response(result, headers={
                        "Content-Disposition": "attachment; filename=mesh_logs.json"
                    })
            else:
                result["error"] = "PG pool not available"
        except Exception as e:
            result["error"] = str(e)
        return web.json_response(result)

    async def _api_memory_search(self, request):
        """GET /api/memory-search?q=query&limit=10 — Vector search across mesh_memory + agent_memory."""
        from aiohttp import web
        import urllib.request as urlreq
        import urllib.parse as urlparse
        user, err = self._require_auth(request)
        if err:
            return err
        q = request.query.get("q", "").strip()
        limit = int(request.query.get("limit", "10"))
        if not q:
            return web.json_response({"results": [], "error": "query required"})
        brain_host = "192.168.1.8"
        brain_port = 3322
        mesh_results = []
        agent_results = []
        try:
            url = f"http://{brain_host}:{brain_port}/mesh/memory/vector?query={urlparse.quote(q)}&limit={limit}"
            r = urlreq.Request(url, method="GET")
            resp = urlreq.urlopen(r, timeout=10)
            data = json.loads(resp.read())
            mesh_results = data.get("results", [])
        except Exception as e:
            mesh_results = [{"error": str(e)}]
        try:
            url2 = f"http://{brain_host}:{brain_port}/memory/vector?query={urlparse.quote(q)}&limit={limit}"
            r2 = urlreq.Request(url2, method="GET")
            resp2 = urlreq.urlopen(r2, timeout=10)
            data2 = json.loads(resp2.read())
            agent_results = data2.get("results", [])
        except Exception as e:
            agent_results = [{"error": str(e)}]
        return web.json_response({
            "query": q,
            "mesh_results": mesh_results,
            "agent_results": agent_results,
            "total": len(mesh_results) + len(agent_results)
        })

    async def _api_memory_stats(self, request):
        """GET /api/memory-stats — Statistics about mesh_memory table."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
        if not pool:
            return web.json_response({"error": "DB not available"}, status=503)
        try:
            stats = await pool.fetchrow(
                "SELECT count(*) as total, count(CASE WHEN embedding IS NOT NULL THEN 1 END) as embedded, "
                "count(CASE WHEN access_count > 0 THEN 1 END) as accessed, "
                "max(access_count) as max_access, min(created_at) as oldest, max(created_at) as newest "
                "FROM mesh.mesh_memory"
            )
            by_type = await pool.fetch(
                "SELECT memory_type, count(*) as cnt FROM mesh.mesh_memory GROUP BY memory_type ORDER BY cnt DESC"
            )
            # Convert datetimes to strings for JSON
            stats_dict = dict(stats) if stats else {}
            for k, v in stats_dict.items():
                if hasattr(v, 'isoformat'):
                    stats_dict[k] = v.isoformat()
            return web.json_response({
                "stats": stats_dict,
                "by_type": [dict(r) for r in by_type] if by_type else [],
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
