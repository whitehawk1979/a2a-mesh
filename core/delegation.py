"""
Task Delegation System for A2A Mesh.

Two modes:
  1. DIRECT: Sender assigns task to a specific agent (to_agent)
  2. AVAILABLE: Task posted for any agent to claim (to_agent='any', status='available')

Features:
  - Agent assignment/reassignment
  - Progress tracking (0-100)
  - Notes/journal for task execution details
  - File attachment support via result_file
  - A2A message notification on delegation

Flow:
  DIRECT: from_agent → to_agent (specific agent)
  AVAILABLE: from_agent → 'any' (any agent can claim via status='available')
"""

import asyncio
import random
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union
from core.json_logger import set_trace_id, get_trace_id, clear_trace_id

log = logging.getLogger("a2a.delegation")

# Delegation statuses
STATUS_PENDING = "pending"
STATUS_AVAILABLE = "available"  # Any agent can claim
STATUS_ACCEPTED = "accepted"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"
STATUS_EXPIRED = "expired"


def _safe_ascii(text: str) -> str:
    """Make text safe for SQL_ASCII databases.
    First normalizes diacritics (írj → irj, fájl → fajl),
    then encodes remaining non-ASCII as \\uXXXX."""
    import unicodedata
    # Step 1: Normalize diacritics where possible (írj → irj)
    nfkd = unicodedata.normalize('NFKD', text)
    ascii_friendly = ''.join(c for c in nfkd if not unicodedata.combining(c))
    # Step 2: Encode any remaining non-ASCII as \\uXXXX
    return ascii_friendly


class DelegationManager:
    """Manages task delegation between mesh nodes via shared_delegations table."""

    MAX_CONCURRENT_TASKS = 8  # Max concurrent delegation tasks per node
    MAX_WIP_TASKS = 3        # WIP-limit: max claimed-but-unfinished tasks per node (P2)

    def __init__(self, pg_pool, node_name: str):
        self.pg_pool = pg_pool  # AsyncDBPool instance
        self.node_name = node_name
        self._running = False
        self._poll_task = None
        self._active_tasks: Dict[str, Dict] = {}
        self._handlers: Dict[str, callable] = {}
        self._poll_interval = 5.0
        self._on_result_callback = None
        # Semaphore to limit concurrent task execution
        self._task_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_TASKS)
        # Fan-out dedup: track (from_agent, subject) combos we've already claimed
        self._claimed_subjects: set = set()
        self._claimed_subjects_timestamps: Dict[tuple, float] = {}  # TTL tracking
        # Race condition protection for claim_task
        self._claim_lock = asyncio.Lock()
        # Circuit breaker per peer: {peer_name: {"failures": int, "last_fail": float, "open": bool}}
        self._circuit_breakers: Dict[str, Dict] = {}
        self._circuit_breaker_threshold = 3   # consecutive failures before opening
        self._circuit_breaker_cooldown = 60.0  # seconds to wait before retrying
        # ── Marveen-inspired: delegation retry + handoff failure tracking ──
        # Per-task consecutive execution failure counter (transient errors only)
        self._task_failures: Dict[str, int] = {}  # task_id → fail count
        self._max_task_retries = 3  # Max retries before marking task as permanently failed
        # Per-agent handoff failure notification cooldown (avoid alert spam)
        self._handoff_alert_cooldown: Dict[str, float] = {}  # agent → last alert time
        # ── Per-tick message budget (Marveen-inspired) ──
        # Prevents a large backlog from monopolizing a single poll tick.
        self._max_poll_per_tick = 25  # Max tasks to poll per tick
        self._max_notify_per_tick = 5  # Max P2P notifications per tick
        # ── Stuck delegation tracking ──
        self._alerted_stuck: set = set()  # task_ids already alerted (avoid repeat)
        # ── Result callback dedup: avoid calling callback for same completed task repeatedly ──
        self._results_seen: set = set()  # task_ids already passed to callback
        self._results_seen_timestamps: Dict[str, float] = {}  # TTL tracking
        self._results_seen_ttl = 1800.0  # 30 min (matches query window)

    async def start(self):
        """Start polling for delegated tasks."""
        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        log.info(f"Delegation manager started for {self.node_name}, polling every {self._poll_interval}s")

    async def stop(self):
        """Stop polling."""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        log.info("Delegation manager stopped")

    def register_handler(self, task_type: str, handler):
        """Register a handler function for a task type."""
        self._handlers[task_type] = handler
        log.info(f"Registered delegation handler for task type: {task_type}")

    def on_result(self, callback):
        """Register callback for when a delegated task completes."""
        self._on_result_callback = callback

    # ── Send side: delegate a task ──

    async def delegate_task(
        self,
        to_agent: str,
        subject: str,
        description: str = "",
        task_type: str = "generic",
        priority: int = 5,
        context: Optional[Dict] = None,
        timeout_minutes: int = 30,
        available: bool = False,
        fan_out: int = 0,
        max_retries: int = 2,
        eligible_agents: Optional[List[str]] = None,
        distribute_mode: bool = False,
        depends_on: Optional[str] = None,
    ) -> Union[str, List[str]]:
        """Delegate a task to another agent or make it available for any agent.
        
        Args:
            to_agent: Target agent name, or 'any' for available tasks
            subject: Task subject/title
            description: Task description
            task_type: Type of task (generic, monitoring, code, research, analysis)
            priority: Priority (1=low, 5=normal, 7=high, 10=critical)
            context: Additional context data
            timeout_minutes: Timeout in minutes (for available tasks, minimum 120 = 2h)
            available: If True, task is available for any agent to claim
            fan_out: If > 0, creates N identical tasks (one per available agent),
                     first to complete wins, others are cancelled. No duplicate work.
            eligible_agents: Optional list of agent names that can claim this task
                            (only used when available=True)
            distribute_mode: If True with fan_out, each child goes to a DIFFERENT agent
                            (round-robin). All children must complete — no race/cancel.
                            Use for parallel task breakdown where every piece matters.
            depends_on: task_id of a parent task. This task stays PENDING until the
                        parent completes, then auto-activates to AVAILABLE.
                        Enables dependency chains: A → B → C.
        """
        # ── Input validation ──
        if not subject or not subject.strip():
            raise ValueError("delegate_task: subject is required")
        if not to_agent or not to_agent.strip():
            raise ValueError("delegate_task: to_agent is required")
        subject = subject.strip()[:500]  # Limit subject length
        to_agent = to_agent.strip()
        if priority < 1:
            priority = 1
        elif priority > 10:
            priority = 10
        if timeout_minutes < 1:
            timeout_minutes = 30
        # Available tasks: minimum 2h expiry — no agent might be online yet
        if available and timeout_minutes < 120:
            timeout_minutes = 120
            log.info(f"Available task timeout raised to 120min minimum (no agent may be online yet)")
        # Prevent self-delegation
        if to_agent == self.node_name and not available:
            log.warning(f"Skipping self-delegation: {self.node_name} → {to_agent} (use available=True instead)")
            return ""
        # Check circuit breaker for target agent
        if not available and to_agent != "any":
            cb = self._circuit_breakers.get(to_agent)
            if cb and cb.get("open"):
                if time.time() - cb.get("last_fail", 0) < self._circuit_breaker_cooldown:
                    log.warning(f"Circuit breaker OPEN for {to_agent}: skipping delegation (failures={cb.get('failures',0)})")
                    return ""
                else:
                    # Cooldown expired, half-open — allow one attempt
                    log.info(f"Circuit breaker HALF-OPEN for {to_agent}: allowing one attempt")
                    cb["open"] = False
        task_id = str(uuid.uuid4())
        status = STATUS_AVAILABLE if available else STATUS_PENDING
        actual_to = "any" if available else to_agent
        trace_id = f"trace-{self.node_name}-{task_id[:8]}"
        set_trace_id(trace_id)
        log.info(f"Delegating task {task_id} to {actual_to}: {subject} (P{priority}, {status})")
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=timeout_minutes)
        # Parse description: if it's already a valid JSON with "type", use it as-is
        try:
            parsed_desc = json.loads(description) if isinstance(description, str) else description
            if isinstance(parsed_desc, dict) and "type" in parsed_desc:
                desc_json = json.dumps(parsed_desc)
            else:
                desc_json = json.dumps({"type": task_type, "description": description, "context": context or {}})
        except (json.JSONDecodeError, TypeError):
            desc_json = json.dumps({"type": task_type, "description": str(description), "context": context or {}})
        # Add eligible_agents to description JSON if specified
        if eligible_agents and available:
            try:
                desc_data = json.loads(desc_json)
                desc_data["eligible_agents"] = eligible_agents
                desc_json = json.dumps(desc_data)
            except (json.JSONDecodeError, TypeError):
                pass

        await self.pg_pool.execute(
            """INSERT INTO shared_delegations 
               (task_id, from_agent, to_agent, subject, description, status, priority, expires_at, assigned_agent, max_retries, task_type)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
            task_id, self.node_name, actual_to, subject, desc_json,
            status, priority, expires_at, None, max_retries, task_type or "generic",
        )

        log.info(f"Delegated task {task_id} to {actual_to}: {subject} (P{priority}, {status})")
        clear_trace_id()

        # ── Auto-create Kanban card for this delegation ──
        kanban_card_id = None
        try:
            from .kanban import KanbanManager, _load_boards, _save_boards
            km = KanbanManager(self.node_name)
            boards = _load_boards()
            if boards:
                priority_str = "high" if priority >= 7 else ("medium" if priority >= 4 else "low")
                col = "todo" if status == STATUS_AVAILABLE else "todo"
                card = km.add_card(
                    boards[0]["id"], subject[:80], column=col,
                    description=description if isinstance(description, str) else str(description),
                    priority=priority_str,
                    assigned_to=actual_to if actual_to != "any" else "",
                )
                if card:
                    kanban_card_id = card["id"]
                    # Find the card in our boards list (add_card uses its own load)
                    boards = _load_boards()  # Reload to get the saved card
                    for b in boards:
                        for c in b.get("cards", []):
                            if c["id"] == kanban_card_id:
                                c["delegation_task_id"] = task_id
                                c["delegation_status"] = status
                                c["from_agent"] = self.node_name
                                c["to_agent"] = actual_to
                                c["agent_history"] = [{
                                    "agent": self.node_name,
                                    "role": "delegator",
                                    "action": "created task",
                                    "timestamp": time.time(),
                                }]
                                c["updated_at"] = time.time()
                                break
                    _save_boards(boards)
                    # Update PG with kanban_card_id
                    await self.pg_pool.execute(
                        "UPDATE shared_delegations SET kanban_card_id = $1 WHERE task_id = $2",
                        kanban_card_id, task_id,
                    )
                    log.info(f"Auto-created Kanban card {kanban_card_id} for delegation '{subject[:40]}'")
        except Exception as kb_err:
            log.debug(f"Kanban auto-card error: {kb_err}")

        # ── Fan-out / Distribute: create N tasks ──
        if fan_out > 0:
            task_ids = [task_id]
            # DISTRIBUTE mode: each child gets a unique subject suffix + no race cancellation
            mode_label = "DISTRIBUTE" if distribute_mode else "RACE"
            
            # In distribute mode, try to assign to different agents round-robin
            known_agents = []
            if distribute_mode:
                try:
                    known_agents = [r['node_name'] for r in await self.pg_pool.fetch(
                        "SELECT DISTINCT node_name FROM mesh_node_health WHERE status = 'active' AND node_name != $1",
                        self.node_name,
                    )] if hasattr(self, 'pg_pool') else []
                except Exception:
                    known_agents = []
            
            for i in range(1, fan_out):
                fan_id = str(uuid.uuid4())
                # In distribute mode, give each child a unique subject
                child_subject = f"[{i+1}/{fan_out}] {subject}" if distribute_mode else subject
                # In distribute mode, try targeting different agents
                child_to = actual_to
                if distribute_mode and known_agents and i-1 < len(known_agents):
                    child_to = known_agents[i-1]
                
                child_status = status
                child_expires = expires_at
                child_desc = desc_json
                
                await self.pg_pool.execute(
                    """INSERT INTO shared_delegations 
                       (task_id, from_agent, to_agent, subject, description, status, priority, expires_at, assigned_agent, max_retries, task_type)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                    fan_id, self.node_name, child_to, child_subject, child_desc,
                    child_status, priority, child_expires, None, max_retries, task_type or "generic",
                )
                task_ids.append(fan_id)
                # Auto-create Kanban card for fan-out child
                try:
                    from .kanban import KanbanManager, _load_boards, _save_boards
                    km2 = KanbanManager(self.node_name)
                    boards2 = _load_boards()
                    if boards2:
                        card_title = f"Distribute: [{i+1}/{fan_out}] {subject}"[:80] if distribute_mode else f"Fanout: {subject}"[:80]
                        child_card = km2.add_card(
                            boards2[0]["id"], card_title, column="todo",
                            description=description if isinstance(description, str) else str(description),
                            priority="high" if priority >= 7 else "medium",
                            assigned_to=child_to if child_to != "any" else "",
                        )
                        if child_card:
                            child_card["delegation_task_id"] = fan_id
                            child_card["delegation_status"] = child_status
                            child_card["from_agent"] = self.node_name
                            child_card["to_agent"] = child_to
                            child_card["agent_history"] = [{
                                "agent": self.node_name,
                                "role": "delegator",
                                "action": "created fan-out task",
                                "timestamp": time.time(),
                            }]
                            child_card["distribute_mode"] = distribute_mode
                            _save_boards(boards2)
                            await self.pg_pool.execute(
                                "UPDATE shared_delegations SET kanban_card_id = $1 WHERE task_id = $2",
                                child_card["id"], fan_id,
                            )
                except Exception as fc_err:
                    log.debug(f"Fan-out Kanban card error: {fc_err}")
            log.info(f"{mode_label}: created {len(task_ids)} tasks for '{subject}' (P{priority})")
            
            # Mark parent task with distribute_mode flag in notes
            if distribute_mode:
                await self.add_note(task_id, f"[DISTRIBUTE] Parent task — {fan_out} children must all complete", "system")
            
            return task_ids
        
        # ── Dependency chain: task stays pending until parent completes ──
        if depends_on:
            try:
                # Check parent status
                parent_status = await self.pg_pool.fetchval(
                    "SELECT status FROM shared_delegations WHERE task_id = $1",
                    depends_on,
                )
                if parent_status == STATUS_COMPLETED:
                    # Parent already done — activate immediately
                    await self.pg_pool.execute(
                        "UPDATE shared_delegations SET status = $1 WHERE task_id = $2",
                        STATUS_AVAILABLE, task_id,
                    )
                    log.info(f"Dependency: parent {depends_on[:8]} already completed — activating {task_id[:8]}")
                else:
                    # Parent not done — stay pending
                    await self.pg_pool.execute(
                        "UPDATE shared_delegations SET status = $1 WHERE task_id = $2",
                        STATUS_PENDING, task_id,
                    )
                    # Store depends_on in notes
                    await self.add_note(task_id, f"[DEPENDS_ON] {depends_on}", "system")
                    log.info(f"Dependency: {task_id[:8]} waiting for parent {depends_on[:8]} to complete")
            except Exception as dep_err:
                log.warning(f"Dependency setup failed: {dep_err}")

        # Notify via A2A message + PG NOTIFY
        try:
            from .message import A2AMessage
            recipient = "broadcast" if available else to_agent
            msg = A2AMessage.create(
                sender=self.node_name,
                recipient=recipient,
                msg_type="delegation",
                payload={
                    "text": f"New {'available' if available else ''} task: {subject}",
                    "subject": subject,
                    "task_id": task_id,
                },
                priority=priority,
            )
            # Send via router (P2P/PG transports)
            if hasattr(self, 'router') and self.router:
                await self.router.send(msg)
                log.info(f"Delegation message sent to {recipient} via router")
                # Trace via message_router
                try:
                    from .message_router import create_message
                    create_message(self.node_name, recipient,
                        f"[delegation] {subject}", msg_type="delegation",
                        trace_id=f"trace-{self.node_name}-{task_id[:8]}")
                except Exception:
                    pass
            else:
                log.debug("No router available — delegation stored in DB only")
            # Also send PG NOTIFY for immediate pickup by listeners
            if hasattr(self, 'pg_pool') and self.pg_pool:
                await self.pg_pool.execute(
                    "SELECT pg_notify('delegation_channel', $1)",
                    json.dumps({"task_id": task_id, "to_agent": actual_to, "subject": subject, "from_agent": self.node_name})
                )
                log.debug(f"PG NOTIFY sent on delegation_channel for task {task_id}")
        except Exception as e:
            log.warning(f"Could not send delegation notification: {e}")

        return task_id

    async def claim_task(self, task_id: str, agent_name: Optional[str] = None) -> bool:
        """Claim an available task. Agent can claim tasks marked as 'available'.
        If eligible_agents is set in the task description, only those agents can claim.
        Uses asyncio.Lock to prevent race conditions between concurrent polls."""
        async with self._claim_lock:
            agent = agent_name or self.node_name
            # Check eligible_agents constraint
            task_row = await self.pg_pool.fetchrow(
                "SELECT description FROM shared_delegations WHERE task_id = $1 AND status = $2",
                task_id, STATUS_AVAILABLE,
            )
            if task_row:
                try:
                    desc = json.loads(task_row["description"]) if isinstance(task_row["description"], str) else task_row["description"]
                    eligible = desc.get("eligible_agents") if isinstance(desc, dict) else None
                    if eligible and isinstance(eligible, list) and agent not in eligible:
                        log.warning(f"Agent {agent} not in eligible_agents for task {task_id}: {eligible}")
                        return False
                except (json.JSONDecodeError, TypeError):
                    pass
            result = await self.pg_pool.execute(
                """UPDATE shared_delegations 
                   SET status = $1, assigned_agent = $2, accepted_at = NOW()
                   WHERE task_id = $3 AND status = $4""",
                STATUS_ACCEPTED, agent, task_id, STATUS_AVAILABLE,
            )
            if "UPDATE 1" in result:
                log.info(f"Agent {agent} claimed task {task_id}")
                return True
            return False

    async def reassign_task(self, task_id: str, new_agent: str) -> bool:
        """Reassign a task to a different agent."""
        result = await self.pg_pool.execute(
            """UPDATE shared_delegations 
               SET to_agent = $1, assigned_agent = $1
               WHERE task_id = $2 AND status IN ($3, $4)""",
            new_agent, task_id, STATUS_ACCEPTED, STATUS_PENDING,
        )
        return "UPDATE 1" in result

    async def add_note(self, task_id: str, note: str, agent: Optional[str] = None) -> bool:
        """Add a progress note to a task."""
        who = agent or self.node_name
        timestamp = datetime.now(timezone.utc).isoformat()
        note_entry = json.dumps({"agent": who, "note": _safe_ascii(note)[:500], "time": timestamp})
        result = await self.pg_pool.execute(
            """UPDATE shared_delegations 
               SET notes = COALESCE(notes, '[]'::jsonb) || $1::jsonb
               WHERE task_id = $2""",
            note_entry, task_id,
        )
        return "UPDATE 1" in result

    async def update_progress(self, task_id: str, progress: int, note: Optional[str] = None) -> bool:
        """Update task progress (0-100) with optional note."""
        if note:
            await self.add_note(task_id, note)
        result = await self.pg_pool.execute(
            "UPDATE shared_delegations SET progress = $1 WHERE task_id = $2",
            min(100, max(0, progress)), task_id,
        )
        return "UPDATE 1" in result

    async def get_task_status(self, task_id: str) -> Optional[Dict]:
        """Check the status of a delegated task."""
        row = await self.pg_pool.fetchrow(
            "SELECT * FROM shared_delegations WHERE task_id = $1", task_id
        )
        if row:
            return dict(row)
        return None

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending delegation."""
        result = await self.pg_pool.execute(
            """UPDATE shared_delegations SET status = $1 
               WHERE task_id = $2 AND status IN ($3, $4, $5)""",
            STATUS_CANCELLED, task_id, STATUS_PENDING, STATUS_AVAILABLE, STATUS_ACCEPTED,
        )
        return "UPDATE 1" in result

    # ── Receive side: poll and execute tasks ──

    async def _poll_loop(self):
        """Poll shared_delegations for tasks assigned to this node."""
        _heartbeat_counter = 0
        while self._running:
            try:
                _heartbeat_counter += 1
                if _heartbeat_counter % 12 == 1:  # Every ~60s
                    log.info(f"Delegation poll heartbeat #{_heartbeat_counter} (pg_pool={'connected' if (self.pg_pool and self.pg_pool.is_connected()) else 'DISCONNECTED'})")
                # Guard: if pg_pool is None or not connected, skip polling
                # (health monitor will attempt reconnect every 30s)
                # Guard: if pg_pool is None, skip
                if self.pg_pool is None:
                    log.warning("Delegation poll skipped: pg_pool is None")
                    await asyncio.sleep(self._poll_interval)
                    continue

                # Health check: try simple query — force reconnect on failure
                try:
                    await self.pg_pool.fetchval("SELECT 1")
                except Exception as hc_err:
                    log.warning(f"Delegation poll: PG health check failed — forcing reconnect: {hc_err}")
                    try:
                        if self.pg_pool._pool and not self.pg_pool._pool._closed:
                            await self.pg_pool._pool.close()
                        self.pg_pool._pool = None
                        if await self.pg_pool.connect():
                            log.info("Delegation: PG reconnected after health check")
                        else:
                            await asyncio.sleep(self._poll_interval)
                            continue
                    except Exception as re_err:
                        log.warning(f"Delegation: PG reconnect failed: {re_err}")
                        await asyncio.sleep(self._poll_interval)
                        continue

                # Cleanup expired dedup entries (older than 1 hour)
                now = time.time()
                expired = [k for k, v in self._claimed_subjects_timestamps.items() if now - v > 3600]
                for k in expired:
                    self._claimed_subjects.discard(k)
                    del self._claimed_subjects_timestamps[k]
                if expired:
                    log.debug(f"Cleaned up {len(expired)} expired claimed subjects")

                await self._check_expired()
                await self._cleanup_stale_active_tasks()
                await self._poll_pending()
                await self._poll_available()
                await self._check_results()
                await self._check_dependencies()
                await self._cleanup_old_tasks()
                await self._archive_orphaned_kanban_cards()
                await self._reconcile_kanban_cards()
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Delegation poll error: {e}")
                # If it's a connection error, try to reconnect the pool
                err_str = str(e).lower()
                if any(k in err_str for k in ('connection', 'timeout', 'closed', 'reset', 'refused', 'eof')):
                    log.warning("Delegation poll: connection error detected — attempting PG pool reconnect")
                    try:
                        if self.pg_pool is not None:
                            # Force reconnect: close stale pool and create new
                            if self.pg_pool._pool and not self.pg_pool._pool._closed:
                                try:
                                    await self.pg_pool._pool.close()
                                except Exception:
                                    pass
                            self.pg_pool._pool = None
                            if await self.pg_pool.connect():
                                log.info("Delegation: PG pool reconnected after error")
                    except Exception as re_err:
                        log.error(f"Delegation: reconnect failed: {re_err}")
                # Exponential backoff: double the poll interval, max 60s
                backoff = min(self._poll_interval * 2, 60.0)
                log.warning(f"Backing off delegation poll to {backoff:.1f}s after error")
                await asyncio.sleep(backoff)
                continue

    async def _check_expired(self):
        """Mark expired tasks — including running tasks that exceeded their TTL.
        
        Available tasks are auto-renewed if no peer has free capacity to claim them.
        This is the core of resource sharing: tasks wait until an agent with capacity
        becomes available, rather than expiring and losing the work request.
        
        Also detects stuck delegations (accepted but no progress for >10min)
        and alerts the leader node — inspired by Marveen's stuck-session detection.
        """
        # ── Stuck Delegation Detection (Marveen-inspired) ──
        # A task accepted but stuck in accepted/running for >10min with no progress
        # update → alert the leader (from_agent) so the work isn't silently lost.
        try:
            stuck_rows = await self.pg_pool.fetch(
                """SELECT task_id, from_agent, to_agent, assigned_agent, subject, 
                          status, progress, accepted_at, updated_at
                   FROM shared_delegations
                   WHERE status IN ($1, $2)
                   AND accepted_at IS NOT NULL
                   AND (updated_at IS NULL OR updated_at < NOW() - INTERVAL '10 minutes')
                   AND accepted_at < NOW() - INTERVAL '10 minutes'
                   LIMIT 10""",
                STATUS_ACCEPTED, STATUS_RUNNING,
            )
            for row in stuck_rows:
                task = dict(row)
                task_id = task.get("task_id", "")
                from_agent = task.get("from_agent", "")
                assigned = task.get("assigned_agent", "?")
                subject = task.get("subject", "?")
                log.warning(f"⏰ STUCK DELEGATION: {task_id} '{subject}' stuck on {assigned} for >10min (from {from_agent})")
                await self.add_note(task_id, f"[STUCK ALERT] Task stuck on {assigned} for >10min — auto-reassigning", "system")
                
                # ── AUTO-REASSIGN ──
                # Reset task to available so another agent can claim it
                try:
                    current_retry = await self.pg_pool.fetchval(
                        "SELECT retry_count FROM shared_delegations WHERE task_id = $1",
                        task_id,
                    )
                    max_retries = await self.pg_pool.fetchval(
                        "SELECT max_retries FROM shared_delegations WHERE task_id = $1",
                        task_id,
                    )
                    current_retry = current_retry or 0
                    max_retries = max_retries or 2
                    
                    if current_retry < max_retries:
                        # Reassign: reset to available, increment retry, clear assigned_agent
                        await self.pg_pool.execute(
                            """UPDATE shared_delegations 
                               SET status = $1, assigned_agent = NULL, accepted_at = NULL,
                                   updated_at = NOW(), expires_at = NOW() + INTERVAL '120 minutes',
                                   retry_count = $2, progress = 0
                               WHERE task_id = $3""",
                            STATUS_AVAILABLE, current_retry + 1, task_id,
                        )
                        log.info(f"🔄 AUTO-REASSIGN: {task_id} '{subject}' reset to available (retry {current_retry+1}/{max_retries})")
                        await self.add_note(task_id, f"[REASSIGN] Auto-reassigned (retry {current_retry+1}/{max_retries}) — was stuck on {assigned}", "system")
                    else:
                        # Exhausted retries — escalate priority and alert
                        log.warning(f"⚠️ REASSIGN EXHAUSTED: {task_id} '{subject}' after {max_retries} retries — escalating to P9")
                        await self.pg_pool.execute(
                            """UPDATE shared_delegations 
                               SET status = $1, assigned_agent = NULL, accepted_at = NULL,
                                   updated_at = NOW(), expires_at = NOW() + INTERVAL '120 minutes',
                                   priority = 9, progress = 0
                               WHERE task_id = $2""",
                            STATUS_AVAILABLE, task_id,
                        )
                        await self.add_note(task_id, f"[ESCALATE] Retries exhausted ({max_retries}) — escalated to P9, available for any agent", "system")
                except Exception as re_err:
                    log.error(f"Auto-reassign failed for {task_id}: {re_err}")
                
                # Notify via P2P message if router available
                try:
                    from .message import A2AMessage
                    msg = A2AMessage.create(
                        sender="system",
                        recipient=from_agent,
                        msg_type="delegation_alert",
                        payload={
                            "text": f"[STUCK] Delegation '{subject}' (task_id={task_id}) stuck on {assigned} — auto-reassigned.",
                            "task_id": task_id,
                            "stuck_agent": assigned,
                            "subject": subject,
                            "trace_id": f"trace-{assigned}-{task_id[:8]}",
                        },
                        priority=8,
                    )
                    if hasattr(self, 'router') and self.router:
                        await self.router.send(msg)
                except Exception as e:
                    log.debug(f"Stuck alert P2P send failed: {e}")
        except Exception as e:
            log.debug(f"Stuck delegation check failed: {e}")
        # Check if any peer has been active recently AND has capacity
        # Heuristic: if peers completed tasks recently, they're alive and processing
        try:
            # Count peers that completed a task in the last 30 min (active = has capacity)
            active_peers = await self.pg_pool.fetchval(
                """SELECT COUNT(DISTINCT assigned_agent) FROM shared_delegations 
                   WHERE assigned_agent IS NOT NULL 
                   AND assigned_agent != $1
                   AND completed_at IS NOT NULL 
                   AND completed_at > NOW() - INTERVAL '30 minutes'""",
                self.node_name,
            )
        except Exception:
            active_peers = 1  # If we can't check, don't hold tasks forever

        # Also check: are there currently RUNNING tasks by peers? (they're busy but alive)
        try:
            busy_peers = await self.pg_pool.fetchval(
                """SELECT COUNT(DISTINCT assigned_agent) FROM shared_delegations 
                   WHERE assigned_agent IS NOT NULL 
                   AND assigned_agent != $1
                   AND status = $2""",
                self.node_name, STATUS_RUNNING,
            )
        except Exception:
            busy_peers = 0

        peers_alive = (active_peers and active_peers > 0) or (busy_peers and busy_peers > 0)
        
        if not peers_alive:
            # No peers active recently — only expire PENDING (directed) tasks,
            # keep AVAILABLE tasks alive for when an agent with capacity comes online
            await self.pg_pool.execute(
                """UPDATE shared_delegations SET status = $1 
                   WHERE status IN ($2, $3, $4) AND expires_at < NOW()""",
                STATUS_EXPIRED, STATUS_PENDING, STATUS_ACCEPTED, STATUS_RUNNING,
            )
            # Auto-renew: extend expiry for available tasks by 2h
            renewed_count = await self.pg_pool.fetchval(
                """WITH renewed AS (
                       UPDATE shared_delegations 
                       SET expires_at = NOW() + INTERVAL '120 minutes'
                       WHERE status = $1 AND expires_at < NOW() + INTERVAL '10 minutes'
                       RETURNING task_id
                   )
                   SELECT COUNT(*) FROM renewed""",
                STATUS_AVAILABLE,
            )
            if renewed_count and renewed_count > 0:
                log.info(f"⏳ No peers with capacity — auto-renewed {renewed_count} available task(s) by 2h (resource sharing: waiting for capacity)")
        else:
            # Peers are active — normal expiry for all statuses
            await self.pg_pool.execute(
                """UPDATE shared_delegations SET status = $1 
                   WHERE status IN ($2, $3, $4, $5) AND expires_at < NOW()""",
                STATUS_EXPIRED, STATUS_PENDING, STATUS_AVAILABLE, STATUS_ACCEPTED, STATUS_RUNNING,
            )

    async def _cleanup_stale_active_tasks(self):
        """Remove stale entries from _active_tasks that are no longer running in PG."""
        if not self._active_tasks:
            return
        stale = []
        for task_id in list(self._active_tasks.keys()):
            row = await self.pg_pool.fetchrow(
                "SELECT status FROM shared_delegations WHERE task_id = $1",
                task_id,
            )
            if not row or row['status'] != STATUS_RUNNING:
                stale.append(task_id)
        for task_id in stale:
            self._active_tasks.pop(task_id, None)
            log.info(f"🧹 Cleaned stale active task: {task_id}")

    async def _reconcile_kanban_cards(self):
        """PG-first reconcile: kanban kártyák követik a shared_delegations valós állapotát.

        Két régi hibát gyógyít:
        1. Árva kártyák (duplikált create, retry) — a kanban_card_id már más
           kártyára mutat, a régi todo-ban ragad. Match: delegation_task_id.
        2. Kimaradt szinkron (node restart a 30 perces _check_results ablakban) —
           a task completed, de a kártya in_progress/todo maradt.

        Szabályok (determinisztikus, PG a SSOT):
        - card.delegation_task_id → PG status completed/failed/cancelled/expired
          → kártya done-ba (review delegálás nélkül — a review a kanban_card_id
          útvonalon fut le).
        - PG status accepted/running → in_progress.
        - PG-ből törölt task → done (az _archive_orphaned már archiválja).
        Throttle: 60s-enként fut.
        """
        try:
            now = time.time()
            if now - getattr(self, "_last_kanban_reconcile", 0) < 60:
                return
            self._last_kanban_reconcile = now

            if not self.pg_pool or not self.pg_pool.is_connected():
                return

            import os as _os, json as _json
            kanban_path = _os.path.expanduser(
                "~/.hermes/scripts/a2a_mesh/data/kanban.json"
            )
            if not _os.path.isfile(kanban_path):
                return
            with open(kanban_path) as _f:
                boards = _json.load(_f)

            # Aktív oszlopokban lévő, delegation-hez kötött kártyák task-id-i
            active = []  # (board_idx, card_idx, task_id)
            for bi, b in enumerate(boards):
                if b.get("id", "").startswith("board-archive"):
                    continue
                for ci, c in enumerate(b.get("cards", [])):
                    tid = c.get("delegation_task_id")
                    if tid and c.get("column") in ("todo", "in_progress", "review"):
                        active.append((bi, ci, str(tid)))
            if not active:
                return

            task_ids = list({t for _, _, t in active})
            # PG valós státuszok egy batch query-ben
            rows = await self.pg_pool.fetch(
                """SELECT task_id, status FROM shared_delegations
                   WHERE task_id = ANY($1)""",
                task_ids,
            )
            status_map = {str(r["task_id"]): str(r["status"]) for r in rows}

            moved = 0
            for bi, ci, tid in active:
                st = status_map.get(tid)
                card = boards[bi]["cards"][ci]
                if st in ("completed", "failed", "cancelled", "expired"):
                    card["column"] = "done"
                    card["delegation_status"] = st
                    card["updated_at"] = time.time()
                    moved += 1
                elif st in ("accepted", "running") and card["column"] == "todo":
                    card["column"] = "in_progress"
                    card["delegation_status"] = st
                    card["updated_at"] = time.time()
                    moved += 1
                elif st is None:
                    # Task már nincs PG-ben (retention cleanup) → done
                    card["column"] = "done"
                    card["delegation_status"] = "deleted"
                    card["updated_at"] = time.time()
                    moved += 1

            if moved:
                with open(kanban_path, "w") as _f:
                    _json.dump(boards, _f, indent=2)
                log.info(
                    f"Kanban reconcile: {moved} kártya PG-státusz szerint igazítva"
                )
        except Exception as e:
            log.debug(f"Kanban reconcile skipped: {e}")

    async def _archive_orphaned_kanban_cards(self, interval_hours: int = 1):
        """Archive kanban cards whose delegation no longer exists in PG (deleted by the
        7-day retention cleanup) — prevents orphaned cards piling up in the todo column.
        Throttled to run at most once per hour."""
        try:
            now = time.time()
            last = getattr(self, "_last_kanban_orphan_sweep", 0)
            if now - last < interval_hours * 3600:
                return
            self._last_kanban_orphan_sweep = now

            if not self.pg_pool or not self.pg_pool.is_connected():
                return

            import os as _os, json as _json
            kanban_path = _os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/kanban.json")
            if not _os.path.isfile(kanban_path):
                return

            with open(kanban_path) as f:
                boards = _json.load(f)

            # Collect all live delegation task_ids from PG
            rows = await self.pg_pool.fetch("SELECT task_id FROM shared_delegations")
            live_ids = {str(r["task_id"]) for r in rows}

            arch_id = "board-archive-orphaned-" + time.strftime("%Y%m%d")
            arch = next((b for b in boards if b.get("id") == arch_id), None)
            moved = 0
            for b in boards:
                if b.get("id") == arch_id:
                    continue
                keep = []
                for c in b.get("cards", []):
                    deleg_id = c.get("delegation_task_id")
                    is_active_col = c.get("column") in ("todo", "in_progress", "review")
                    if (
                        is_active_col
                        and deleg_id
                        and deleg_id not in live_ids
                        and not c.get("archived_reason")
                    ):
                        # Delegation gone from PG → orphan card → archive
                        if not arch:
                            arch = {"id": arch_id, "name": "Archive — orphaned cards (auto)", "cards": [], "created_at": now}
                            boards.append(arch)
                        c["archived_reason"] = "orphaned: delegation deleted from PG (retention cleanup)"
                        c["archived_at"] = now
                        arch["cards"].append(c)
                        moved += 1
                    else:
                        keep.append(c)
                b["cards"] = keep

            if moved:
                with open(kanban_path, "w") as f:
                    _json.dump(boards, f, indent=2, ensure_ascii=False)
                log.info(f"🧹 Kanban orphan sweep: archived {moved} cards whose delegations no longer exist")
        except Exception as e:
            log.debug(f"Kanban orphan sweep error: {e}")

    async def _cleanup_old_tasks(self, retention_days: int = 7):
        """Auto-cleanup completed/failed/cancelled/expired tasks older than retention_days.
        Also removes associated files from shared_files table.
        Runs on every poll cycle but only logs when it actually deletes something."""
        if not self.pg_pool or not self.pg_pool.is_connected():
            return
        try:
            # Delete old files first (references via result_file column)
            deleted_files = await self.pg_pool.fetchval(
                """DELETE FROM shared_files
                   WHERE created_at < NOW() - ($1 || ' days')::INTERVAL
                   AND status = 'ready'
                   RETURNING id""",
                str(retention_days),
            )
            # Delete old completed/failed/cancelled/expired delegations
            deleted_tasks = await self.pg_pool.fetchval(
                """DELETE FROM shared_delegations
                   WHERE status IN ($1, $2, $3, $4)
                   AND completed_at IS NOT NULL
                   AND completed_at < NOW() - ($5 || ' days')::INTERVAL
                   RETURNING task_id""",
                STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED, STATUS_EXPIRED,
                str(retention_days),
            )
            if deleted_tasks and deleted_tasks > 0:
                log.info(f"🧹 Auto-cleanup: removed {deleted_tasks} old tasks + {deleted_files or 0} files (>{retention_days}d retention)")
        except Exception as e:
            log.debug(f"Cleanup old tasks error: {e}")


    async def _update_skill_stats(self, task: Dict, success: bool, elapsed: float):
        """Update skill marketplace success_rate + avg_latency_ms after delegation completes.
        
        Uses exponential moving average for smooth updates.
        """
        try:
            # Extract skill name from subject (format: [skill_name] task_text)
            subject = task.get("subject", "")
            skill_name = None
            if subject.startswith("["):
                end = subject.find("]")
                if end > 0:
                    skill_name = subject[1:end].strip()
            
            if not skill_name:
                # Try from description
                desc = task.get("description", "{}")
                if isinstance(desc, str):
                    import json as _json
                    try:
                        d = _json.loads(desc)
                        skill_name = d.get("type") or d.get("skill_name")
                    except Exception:
                        pass
            
            if not skill_name:
                return
            
            agent = task.get("assigned_agent") or self.node_name
            skill_id = f"skill-{agent}-{skill_name}"
            
            # Fetch current stats
            row = await self.pg_pool.fetchrow(
                "SELECT success_rate, avg_latency_ms FROM mesh.mesh_skills WHERE skill_id = $1",
                skill_id,
            )
            if not row:
                # Skill not in marketplace — skip silently
                return
            
            # Exponential moving average (alpha=0.3)
            alpha = 0.3
            old_sr = row['success_rate'] if row['success_rate'] is not None else 1.0
            old_lat = row['avg_latency_ms'] if row['avg_latency_ms'] is not None else 0.0
            
            new_sr = old_sr * (1 - alpha) + (1.0 if success else 0.0) * alpha
            new_lat = old_lat * (1 - alpha) + elapsed * alpha
            
            await self.pg_pool.execute(
                """UPDATE mesh.mesh_skills 
                   SET success_rate = $1, avg_latency_ms = $2, updated_at = NOW()
                   WHERE skill_id = $3""",
                round(new_sr, 4), round(new_lat, 1), skill_id,
            )
            log.debug(f"📊 Skill stats updated: {skill_id} sr={new_sr:.2f} lat={new_lat:.0f}ms")
        except Exception as e:
            log.warning(f"Failed to update skill stats: {e}")

    async def _poll_pending(self):
        """Poll for pending tasks specifically assigned to this node.
        
        Marveen-inspired per-tick budget: max _max_poll_per_tick tasks per poll
        to prevent a large backlog from monopolizing a single tick.
        """
        rows = await self.pg_pool.fetch(
            """SELECT * FROM shared_delegations 
               WHERE to_agent = $1 AND status = $2 
               ORDER BY priority DESC, created_at ASC LIMIT $3""",
            self.node_name, STATUS_PENDING, self._max_poll_per_tick,
        )

        for task in rows:
            task_dict = dict(task)
            task_id = task_dict.get("task_id", "")
            log.info(f"Found pending task {task_id}: {task_dict.get('subject', '?')}")

            # Mark as accepted with assigned_agent
            await self.pg_pool.execute(
                """UPDATE shared_delegations 
                   SET status = $1, accepted_at = NOW(), assigned_agent = $2
                   WHERE task_id = $3""",
                STATUS_ACCEPTED, self.node_name, task_id,
            )

            # Execute in background
            asyncio.create_task(self._execute_task(task_dict))

    async def _poll_available(self):
        """Poll for available tasks that this node can claim.
        Only claim tasks where: (a) we have a handler for the task_type,
        (b) the task was NOT sent by us (avoid claiming our own tasks),
        (c) we haven't already claimed a fan-out sibling with same from+subject.
        Priority-aware: high-priority tasks (7-10) only claimed if CPU load is low.

        WIP-limit (P2, Marveen kanban pattern): if this node is already
        running MAX_WIP_TASKS tasks, claim NOTHING — leave tasks for peers
        instead of hoarding them (prevents claim-queue congestion)."""
        # ── WIP-limit: cap in-flight work before considering new claims ──
        active_count = len(self._active_tasks)
        if active_count >= self.MAX_WIP_TASKS:
            log.debug(f"_poll_available: WIP-limit reached ({active_count}/{self.MAX_WIP_TASKS}) — not claiming")
            return
        # Check our own load for priority-aware claiming
        cpu_load = 0.0
        try:
            import psutil
            cpu_load = psutil.cpu_percent(interval=0.1)
        except ImportError:
            # Fallback: use os.getloadavg() if psutil not available
            try:
                import os
                load1, _, _ = os.getloadavg()
                # Approximate CPU% from load average (rough heuristic)
                import multiprocessing
                cpu_count = multiprocessing.cpu_count() or 1
                cpu_load = min((load1 / cpu_count) * 100, 100.0)
            except Exception:
                cpu_load = 50.0  # Unknown load, assume moderate

        rows = await self.pg_pool.fetch(
            """SELECT * FROM shared_delegations 
               WHERE status = $1 AND (expires_at IS NULL OR expires_at > NOW())
               ORDER BY priority DESC, created_at ASC LIMIT 5""",
            STATUS_AVAILABLE,
        )
        
        if rows:
            log.info(f"_poll_available: found {len(rows)} available tasks (CPU {cpu_load:.0f}%)")

        for task in rows:
            task_dict = dict(task)
            task_id = task_dict.get("task_id", "")
            from_agent = task_dict.get("from_agent", "")
            priority = int(task_dict.get("priority", 5))

            # Distribute-mode targeting: if the task is aimed at a SPECIFIC
            # agent (to_agent != 'any'), only that agent may claim it.
            # Without this, the fastest poller hoards all distribute children.
            row_to_agent = task_dict.get("to_agent", "") or "any"
            if row_to_agent not in ("", "any", self.node_name):
                log.debug(f"Skipping task {task_id}: distribute-targeted at '{row_to_agent}', not us ({self.node_name})")
                continue
            
            # Don't claim our own tasks — let other agents handle them
            # EXCEPTION: local_maintenance tasks MUST be executed by the owner node
            task_type_poll = "generic"
            try:
                desc_poll = task_dict.get("description", "{}")
                if isinstance(desc_poll, str):
                    desc_poll = json.loads(desc_poll)
                task_type_poll = desc_poll.get("type", "generic") if isinstance(desc_poll, dict) else "generic"
            except Exception:
                pass
            
            if from_agent == self.node_name and task_type_poll != "local_maintenance":
                # ── Local Fallback: if task is available for >5min and no peers
                # are online to claim it, execute locally as fallback. ──
                created_age = time.time() - (task_dict.get("created_at").timestamp() if hasattr(task_dict.get("created_at"), 'timestamp') else 0)
                if created_age > 300:  # 5 minutes
                    # Check if any peers are connected
                    peer_count = 0
                    if hasattr(self, '_registry') and hasattr(self._registry, '_nodes'):
                        peer_count = len([n for n in self._registry._nodes.values() if n.get("status") == "online" and n.get("name", "").lower() != self.node_name])
                    if peer_count == 0:
                        log.info(f"📦 Local fallback: claiming own task {task_id} (no peers online, {created_age:.0f}s old)")
                        # Fall through to claim logic below
                    else:
                        log.debug(f"Skipping own task {task_id} ({peer_count} peers online)")
                        continue
                else:
                    log.debug(f"Skipping own task {task_id} (only {created_age:.0f}s old)")
                    continue
            
            # local_maintenance: only the target node should claim it
            if task_type_poll == "local_maintenance" and from_agent != self.node_name:
                log.debug(f"Skipping local_maintenance task {task_id}: not our task (from={from_agent})")
                continue
            
            # Fan-out dedup: don't claim a fan-out sibling we already claimed
            subject_key = (from_agent, task_dict.get("subject", ""))
            if subject_key in self._claimed_subjects:
                log.debug(f"Skipping fan-out sibling {task_id}: already claimed same subject from {from_agent}")
                continue
            
            # Priority-aware: skip tasks if we're overloaded
            # P7+: skip if CPU > 95% (was 80% — too strict for Proxmox hosts)
            # P4-P6: skip if CPU > 98% (was 90% — peer nodes run at 97% normally)
            # P1-P3: always claim (low priority = easy tasks)
            if priority >= 7 and cpu_load > 95:
                log.debug(f"Skipping P{priority} task {task_id}: CPU load {cpu_load:.0f}% > 95%")
                continue
            elif priority >= 4 and cpu_load > 98:
                log.debug(f"Skipping P{priority} task {task_id}: CPU load {cpu_load:.0f}% > 98%")
                continue
            
            # Check if we have a handler for this task type
            description_data = task_dict.get("description", "{}")
            try:
                if isinstance(description_data, str):
                    desc = json.loads(description_data)
                else:
                    desc = description_data
            except (json.JSONDecodeError, TypeError):
                desc = {"type": "generic"}
            task_type = desc.get("type", "generic")
            
            # Only claim if we have a handler (or it's a generic type)
            if task_type not in self._handlers and "generic" not in self._handlers:
                log.debug(f"Skipping task {task_id}: no handler for type '{task_type}' (handlers: {list(self._handlers.keys())})")
                continue

            # Pre-filter: skip tasks where eligible_agents excludes us
            # (avoids WARNING log spam from claim_task on every poll cycle)
            eligible = desc.get("eligible_agents") if isinstance(desc, dict) else None
            if eligible and isinstance(eligible, list) and self.node_name not in eligible:
                log.debug(f"Skipping task {task_id}: eligible_agents={eligible} excludes {self.node_name}")
                continue

            # Try to claim it — add jitter to spread claims across nodes
            await asyncio.sleep(random.uniform(0.1, 0.5))
            claimed = await self.claim_task(task_id)
            if claimed:
                # Track fan-out dedup: remember we claimed this (from_agent, subject)
                self._claimed_subjects.add(subject_key)
                self._claimed_subjects_timestamps[subject_key] = time.time()
                log.info(f"Claimed available task {task_id}: {task_dict.get('subject', '?')} (P{priority}, CPU {cpu_load:.0f}%)")
                asyncio.create_task(self._execute_task(task_dict))

    async def _execute_task(self, task: Dict):
        """Execute a delegated task using registered handlers.
        
        Uses a semaphore to limit concurrent task execution to MAX_CONCURRENT_TASKS.
        Implements Marveen-inspired retry logic: transient failures are retried
        up to _max_task_retries times before the task is permanently failed.
        """
        async with self._task_semaphore:
            task_id = str(task.get("task_id", ""))
            for attempt in range(1, self._max_task_retries + 1):
                try:
                    await self._execute_task_inner(task)
                    # Success — reset failure counter
                    self._task_failures.pop(task_id, None)
                    return
                except Exception as e:
                    fail_count = self._task_failures.get(task_id, 0) + 1
                    self._task_failures[task_id] = fail_count
                    if fail_count < self._max_task_retries:
                        log.warning(f"Task {task_id} attempt {attempt}/{self._max_task_retries} failed: {e} — retrying")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    # Final failure — mark task as failed + notify leader
                    log.error(f"Task {task_id} FAILED after {fail_count} attempts: {e}")
                    try:
                        await self.pg_pool.execute(
                            "UPDATE shared_delegations SET status = $1, result = $2, completed_at = NOW() WHERE task_id = $3",
                            STATUS_FAILED, _safe_ascii(f"Failed after {fail_count} attempts: {str(e)[:500]}"), task_id,
                        )
                    except Exception:
                        pass
                    # ── Handoff Failure Notification (Marveen-inspired) ──
                    # Never let a task fail silently — alert the leader
                    await self._notify_handoff_failure(task, str(e))

    async def _notify_handoff_failure(self, task: Dict, error: str):
        """Notify the leader (from_agent) that a delegated task failed permanently.
        
        Marveen-inspired: never let a handoff fail silently.
        Rate-limited per agent to avoid alert spam (1 alert per 60s per agent).
        Propagates trace_id for distributed tracing.
        """
        task_id = str(task.get("task_id", ""))
        from_agent = task.get("from_agent", "")
        subject = task.get("subject", "?")
        trace_id = f"trace-{self.node_name}-{task_id[:8]}"
        
        if not from_agent or from_agent == self.node_name:
            return  # Don't notify self
        
        # Rate limit: 1 alert per 60s per agent
        now = time.time()
        last_alert = self._handoff_alert_cooldown.get(from_agent, 0)
        if now - last_alert < 60.0:
            log.debug(f"Handoff failure alert for {from_agent} rate-limited (last {now - last_alert:.0f}s ago)")
            return
        self._handoff_alert_cooldown[from_agent] = now
        
        log.warning(f"🚨 HANDOFF FAILURE: task {task_id} '{subject}' from {from_agent} failed on {self.node_name}: {error[:200]}")
        
        # Write alert note on the task
        try:
            await self.add_note(task_id, f"[HANDOFF FAILURE] Task failed on {self.node_name}: {error[:300]}", "system")
        except Exception:
            pass
        
        # Send P2P alert to the leader
        try:
            from .message import A2AMessage
            msg = A2AMessage.create(
                sender="system",
                recipient=from_agent,
                msg_type="delegation_alert",
                payload={
                    "text": f"[HANDOFF FAILURE] Delegation '{subject}' (task_id={task_id}) failed permanently on {self.node_name} after {self._max_task_retries} retries: {error[:200]}. Consider reassigning or handling locally.",
                    "task_id": task_id,
                    "failed_agent": self.node_name,
                    "subject": subject,
                    "error": error[:500],
                    "trace_id": trace_id,
                },
                priority=9,
            )
            if hasattr(self, 'router') and self.router:
                await self.router.send(msg)
                try:
                    from .message_router import create_message
                    create_message("system", from_agent,
                        f"[HANDOFF FAIL] {subject} on {self.node_name}",
                        msg_type="delegation_alert",
                        trace_id=trace_id)
                except Exception:
                    pass
        except Exception as e:
            log.debug(f"Handoff failure P2P alert failed: {e}")

    async def _execute_task_inner(self, task: Dict):
        """Inner implementation of task execution (called under semaphore)."""
        task_id = str(task.get("task_id", ""))
        subject = task.get("subject", "unknown")
        trace_id = f"trace-{self.node_name}-{task_id[:8]}"
        set_trace_id(trace_id)
        log.info(f"Executing task {task_id}: {subject}")
        _exec_start = time.time()
        elapsed_ms = 0.0
        description_data = task.get("description", "{}")

        try:
            if isinstance(description_data, str):
                desc = json.loads(description_data)
            else:
                desc = description_data
        except (json.JSONDecodeError, TypeError):
            desc = {"type": "generic", "description": str(description_data), "context": {}}

        task_type = desc.get("type", "generic")
        context = desc.get("context", {})

        # --- FEATURE C: Explicit Context Attachment ---
        shared_keys = context.get("attach_shared")
        if isinstance(shared_keys, list):
            attachments = {}
            for key in shared_keys:
                try:
                    val = await self.get_context(key)
                    if val is not None:
                        attachments[key] = val
                except Exception as e:
                    log.debug(f"Failed to attach shared context {key}: {e}")
            if attachments:
                context = dict(context)
                context["shared_attachments"] = attachments
                log.info(f"Attached {len(attachments)} shared context items to task {task_id[:8]}")

        log.info(f"Executing task {task_id} of type {task_type}: {subject}")

        # Inject prior memory context for this subject
        try:
            from .hindsight_sync import HindsightSync
            hs = HindsightSync(None)  # No node object needed — Brain host is hardcoded fallback
            hs.set_pg_pool(self.pg_pool)
            prior_memory = await hs.get_context_for_prompt(subject, limit=3)
            if prior_memory:
                context = dict(context)
                context["prior_memory"] = prior_memory
                log.info(f"Injected {len(prior_memory)} chars of prior memory for '{subject[:40]}'")
        except Exception as mem_err:
            log.debug(f"Memory recall failed (non-fatal): {mem_err}")

        # Mark as running
        self._active_tasks[task_id] = task
        await self.pg_pool.execute(
            "UPDATE shared_delegations SET status = $1, assigned_agent = $2 WHERE task_id = $3",
            STATUS_RUNNING, self.node_name, task_id,
        )
        # Marveen-inspired: audit trail in task_runs
        _run_id = None
        try:
            from .marveen_db import start_task_run
            _run_id = await start_task_run(task_id, self.node_name, task_type)
        except Exception:
            pass
        # Add start note
        await self.add_note(task_id, f"Task started by {self.node_name}")

        try:
            handler = self._handlers.get(task_type)
            if handler:
                if asyncio.iscoroutinefunction(handler):
                    handler_result = await handler(task, context)
                else:
                    handler_result = handler(task, context)
                log.info(f"Task {task_id} completed: {str(handler_result)[:100]}")
            else:
                handler_result = f"[{self.node_name}] No handler for task type '{task_type}'. Available: {list(self._handlers.keys())}"
                log.warning(f"No handler for task type '{task_type}', task {task_id}")

            # Parse handler result — can be str or dict with {result, files, context_updates}
            result_text = ""
            result_file_id = None
            if isinstance(handler_result, dict):
                # 16k limit: PG result column is TEXT (unbounded); the old 4k cap
                # truncated research/analysis answers mid-sentence, which the
                # deterministic reviewer then correctly rejected as "csonkolt".
                result_text = _safe_ascii(str(handler_result.get("result", "")))[:16000]
                # Store files in shared_files table (base64-encoded to avoid SQL_ASCII issues)
                import base64
                files = handler_result.get("files", [])
                for f in files:
                    try:
                        raw_content = f.get("content", "")
                        file_encoding = f.get("encoding", "")
                        # If content is already base64-encoded (e.g. binary files like .pptx), use as-is
                        if file_encoding == "base64" and isinstance(raw_content, str):
                            encoded_content = raw_content
                        else:
                            encoded_content = base64.b64encode(raw_content.encode("utf-8") if isinstance(raw_content, str) else raw_content).decode("ascii")
                        file_id = await self.pg_pool.fetchval(
                            """INSERT INTO shared_files 
                               (sender_agent, recipient_agent, filename, content_type, file_size, encoding, content, description, status)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ready')
                               RETURNING id""",
                            self.node_name,
                            task.get("from_agent", ""),
                            f.get("filename", "result.txt"),
                            f.get("content_type", "text/plain"),
                            f.get("size", len(raw_content) if isinstance(raw_content, str) else len(raw_content.encode("utf-8"))),
                            "base64",
                            encoded_content,
                            f"Task result: {subject}",
                        )
                        # Store first file ID as result_file
                        if result_file_id is None:
                            result_file_id = str(file_id)
                    except Exception as file_err:
                        log.warning(f"File storage failed for {f.get('filename','?')}: {file_err}")
                # Apply context updates from handler result (non-fatal)
                ctx_updates = handler_result.get("context_updates", {})
                for key, value in ctx_updates.items():
                    try:
                        await self.set_context(f"task_{str(task_id)[:8]}_{key}", _safe_ascii(str(value)))
                    except Exception as ctx_err:
                        log.warning(f"Context update failed for {key}: {ctx_err}")
            else:
                result_text = _safe_ascii(str(handler_result))[:16000]

            # ── Governance/Egress Gate (Marveen-inspired) ──
            try:
                from .governance import get_gates, GateResult
                gates = get_gates()
                # Determine security profile from agent_team
                sec_profile = "default"
                try:
                    from .agent_team import resolve_security_profile
                    sec_profile = resolve_security_profile(self.node_name)
                except Exception:
                    pass
                gate_result = gates.check(result_text, self.node_name, sec_profile)
                if gate_result.action == GateResult.BLOCK:
                    log.warning(
                        f"Governance BLOCK for task {task_id}: {gate_result.blocked_items}"
                    )
                    result_text = f"[BLOCKED BY GOVERNANCE: {', '.join(gate_result.matched_rules)}]"
                else:
                    result_text = gate_result.cleaned_output
            except Exception as gov_err:
                log.debug(f"Governance check failed (non-fatal): {gov_err}")

            # Mark as completed with optional result_file
            if result_file_id:
                await self.pg_pool.execute(
                    """UPDATE shared_delegations 
                       SET status = $1, result = $2, result_file = $3, completed_at = NOW(), progress = 100
                       WHERE task_id = $4""",
                    STATUS_COMPLETED, result_text, result_file_id, task_id,
                )
            else:
                await self.pg_pool.execute(
                    """UPDATE shared_delegations 
                       SET status = $1, result = $2, completed_at = NOW(), progress = 100
                       WHERE task_id = $3""",
                    STATUS_COMPLETED, result_text, task_id,
                )
            await self.add_note(task_id, f"Task completed: {result_text[:200]}")

            # ── Ötletláda szinkron: ha a delegáció egy ötlethez tartozik (desc.idea_id),
            # az ötlet done-ba kerül, + Kanban kártya auto-promotion ──
            try:
                import json as _json_idea
                _idea_id = None
                try:
                    if isinstance(desc, dict):
                        _idea_id = desc.get("idea_id")
                        if not _idea_id and isinstance(desc.get("context"), dict):
                            _idea_id = desc["context"].get("idea_id")
                except Exception:
                    _idea_id = None
                if _idea_id:
                    # Beépítettség: a végrehajtó node a result-ban jelzi, ha a kód
                    # a repóba került. A jelző ékezetmentesített változatban is előfordulhat
                    # ("Repoba integralva"), mert a result a naplózott szövegből származik.
                    _res_norm = (result_text or "").lower().replace("ó", "o").replace("á", "a")
                    _integrated = ("repóba integrálva" in (result_text or "").lower()) or ("repoba integralva" in _res_norm)
                    _integrated_file = None
                    if _integrated:
                        import re as _re_int
                        _m = _re_int.search(r"[Rr]ep[óo]ba integr[áa]lva:\s*(ideas/[a-zA-Z0-9_.\-]+)", result_text or "")
                        _integrated_file = _m.group(1) if _m else None
                    await self.pg_pool.execute(
                        """UPDATE mesh.mesh_ideas
                           SET status = 'done', updated_at = NOW(), closed_at = NOW()
                           WHERE idea_id = $1 AND status IN ('in_progress', 'approved', 'idea')""",
                        _idea_id,
                    )
                    # Beépítettség külön UPDATE: soha nem rollbackol (false-ra nem ír
                    # felül true-t), és a done-státuszt is eléri — a fan-out race miatt
                    # több szinkron is futhat ugyanarra az ötletre.
                    await self.pg_pool.execute(
                        """UPDATE mesh.mesh_ideas
                           SET integrated = (integrated OR $2),
                               integrated_at = CASE WHEN $2 AND integrated_at IS NULL THEN NOW() ELSE integrated_at END,
                               integrated_file = COALESCE($3, integrated_file)
                           WHERE idea_id = $1""",
                        _idea_id, _integrated, _integrated_file,
                    )
                    log.info(f"💡 Ötletláda szinkron: idea {_idea_id} → done (delegáció {str(task_id)[:8]} completed, integrated={_integrated})")
                    # Telegram-jelzés Zsoltnak: a megvalósított ötlet beépült-e
                    try:
                        _row = await self.pg_pool.fetchrow(
                            "SELECT title, integrated, integrated_file FROM mesh.mesh_ideas WHERE idea_id = $1", _idea_id,
                        )
                        if _row and _row["integrated"]:
                            import subprocess as _sp_tg
                            _msg = (
                                f"✅ ÖTLET BEÉPÍTVE\n\n"
                                f"Ötlet: {_row['title'][:80]}\n"
                                f"Fájl: {_row['integrated_file'] or 'ideas/'}\n"
                                f"A megvalósítás ténylegesen bekerült a repóba (git commit)."
                            )
                            _sp_tg.Popen(
                                ["hermes", "send", "--telegram", "7796035659", _msg],
                                stdout=_sp_tg.DEVNULL, stderr=_sp_tg.DEVNULL,
                            )
                    except Exception as _tg_err:
                        log.debug(f"Telegram integrated-jelzés (non-fatal): {_tg_err}")
            except Exception as _idea_err:
                log.debug(f"Ötletláda sync (non-fatal): {_idea_err}")

            # ── Kanban auto-promotion: review → done when the delegation completes ──
            # Prevents cards stuck in review forever (the 'Ellenőrzés' pile-up bug).
            try:
                import os as _os, json as _json, time as _time
                _kanban_path = _os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/kanban.json")
                if _os.path.isfile(_kanban_path):
                    with open(_kanban_path) as _f:
                        _boards = _json.load(_f)
                    _moved_any = False
                    for _b in _boards:
                        for _c in _b.get("cards", []):
                            if (
                                _c.get("delegation_task_id") == str(task_id)
                                and _c.get("column") in ("review", "in_progress", "todo")
                            ):
                                _c["column"] = "done"
                                _c["approval_required"] = False
                                _c["approved_by"] = f"auto:{self.node_name}"
                                _c["approved_at"] = _time.time()
                                _c["updated_at"] = _time.time()
                                _c.setdefault("agent_history", []).append({
                                    "agent": self.node_name,
                                    "role": "executor",
                                    "action": "auto-promoted review→done (delegation completed)",
                                    "result": result_text[:200],
                                    "timestamp": _time.time(),
                                })
                                _moved_any = True
                                log.info(f"📋 Kanban auto-promotion: {_c['id']} review→done (delegation {task_id} completed)")
                    if _moved_any:
                        with open(_kanban_path, "w") as _f:
                            _json.dump(_boards, _f, indent=2, ensure_ascii=False)
            except Exception as _kan_err:
                log.warning(f"Kanban auto-promotion failed (non-fatal): {_kan_err}")

            # Save to mesh_memory for shared knowledge across agents
            try:
                from .hindsight_sync import HindsightSync
                hs = HindsightSync(None)  # No node object
                hs.set_pg_pool(self.pg_pool)
                await hs.save_delegation_result({
                    "task_id": str(task_id),
                    "from_agent": task.get("from_agent", self.node_name),
                    "assigned_agent": task.get("assigned_agent") or task.get("to_agent", ""),
                    "subject": task.get("subject", ""),
                    "result": result_text[:5000],
                    "status": "completed",
                })
            except Exception as mem_err:
                log.debug(f"Memory save failed (non-fatal): {mem_err}")

            # Marveen-inspired: complete task_run audit trail
            if _run_id:
                try:
                    from .marveen_db import complete_task_run
                    await complete_task_run(_run_id, "completed", result_summary=result_text[:500])
                except Exception:
                    pass

            # Record success in circuit breaker for the assigned agent
            assigned = task.get("assigned_agent") or task.get("to_agent", "") or self.node_name
            self.record_success(assigned)

            # Calculate elapsed time
            elapsed_ms = (time.time() - _exec_start) * 1000.0

            # Update skill marketplace stats (success_rate + latency)
            await self._update_skill_stats(task, success=True, elapsed=elapsed_ms)

            # ── Fan-out RACE: cancel sibling tasks with same subject from same sender ──
            # NOTE: DISTRIBUTE mode children have unique subjects ([1/N], [2/N]...) 
            # so they won't match this query — they complete independently.
            try:
                subject_val = task.get("subject", "")
                from_agent_val = task.get("from_agent", "")
                if subject_val and from_agent_val:
                    cancelled = await self.pg_pool.execute(
                        """UPDATE shared_delegations 
                           SET status = $1, result = $2, completed_at = NOW()
                           WHERE subject = $3 AND from_agent = $4 
                           AND status IN ($5, $6) AND task_id != $7""",
                        STATUS_CANCELLED, f"Fan-out: sibling completed by {self.node_name}",
                        subject_val, from_agent_val,
                        STATUS_AVAILABLE, STATUS_PENDING, task_id,
                    )
                    if cancelled and hasattr(cancelled, '__getitem__') and len(cancelled) > 0:
                        log.info(f"Fan-out RACE: cancelled {cancelled} sibling tasks for '{subject_val}'")
            except Exception as e:
                log.debug(f"Fan-out cancel check (non-critical): {e}")

        except Exception as e:
            log.error(f"Task {task_id} failed: {e}")
            # Record failure in circuit breaker for the assigned agent
            assigned = task.get("assigned_agent") or task.get("to_agent", "")
            if assigned:
                self.record_failure(assigned)
            # Check retry count — if under max_retries, re-queue for another node
            retry_count = task.get("retry_count", 0) if task.get("retry_count") is not None else 0
            max_retries = task.get("max_retries", 2) if task.get("max_retries") is not None else 2
            
            if retry_count < max_retries:
                new_retry = retry_count + 1
                log.info(f"Task {task_id} failed (attempt {new_retry}/{max_retries}), re-queuing as available")
                await self.pg_pool.execute(
                    """UPDATE shared_delegations 
                       SET status = $1, retry_count = $2, assigned_agent = NULL, 
                           result = NULL, completed_at = NULL, accepted_at = NULL
                       WHERE task_id = $3""",
                    STATUS_AVAILABLE, new_retry, task_id,
                )
                await self.add_note(task_id, f"Retry {new_retry}/{max_retries}: re-queued after failure: {str(e)[:150]}")
            else:
                log.warning(f"Task {task_id} failed after {max_retries} retries, marking as failed permanently")
                await self.pg_pool.execute(
                    """UPDATE shared_delegations 
                       SET status = $1, result = $2, completed_at = NOW()
                       WHERE task_id = $3""",
                    STATUS_FAILED, _safe_ascii(str(e))[:16000], task_id,
                )
                await self.add_note(task_id, f"Task failed permanently after {max_retries} retries: {str(e)[:150]}")

                # Marveen-inspired: record failure in task_run audit
                if _run_id:
                    try:
                        from .marveen_db import complete_task_run
                        await complete_task_run(_run_id, "failed", error=str(e)[:500])
                    except Exception:
                        pass

                # Update skill marketplace stats (failure)
                elapsed_ms = (time.time() - _exec_start) * 1000.0
                await self._update_skill_stats(task, success=False, elapsed=elapsed_ms)

        finally:
            self._active_tasks.pop(task_id, None)
            clear_trace_id()

    async def _check_results(self):
        """Check for completed tasks that we delegated out."""
        rows = await self.pg_pool.fetch(
            """SELECT * FROM shared_delegations 
               WHERE from_agent = $1 AND status IN ($2, $3, $4) 
               AND completed_at > NOW() - INTERVAL '30 minutes'
               ORDER BY completed_at DESC LIMIT 10""",
            self.node_name, STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED,
        )

        now = time.time()
        for row in rows:
            task_id = row.get("task_id", "")
            # Dedup: skip if callback already fired for this task
            if task_id in self._results_seen:
                continue
            if self._on_result_callback:
                try:
                    if asyncio.iscoroutinefunction(self._on_result_callback):
                        await self._on_result_callback(dict(row))
                    else:
                        self._on_result_callback(dict(row))
                    # Only mark as seen AFTER successful callback
                    self._results_seen.add(task_id)
                    self._results_seen_timestamps[task_id] = now
                except Exception as e:
                    log.warning(f"Result callback error (will retry next poll): {e}")
            else:
                self._results_seen.add(task_id)
                self._results_seen_timestamps[task_id] = now

            # ── Process review results ──
            if row["status"] == STATUS_COMPLETED and row.get("task_type") == "code_review":
                try:
                    result_text = row.get("result", "") or ""
                    asyncio.create_task(self._process_review_result(str(row["task_id"]), result_text))
                except Exception as re_err:
                    log.debug(f"Review result processing error: {re_err}")

            # ── Dependency chain: activate children waiting on this task ──
            if row["status"] == STATUS_COMPLETED:
                try:
                    task_id = row["task_id"]
                    # Find pending tasks that depend on this one (stored in notes)
                    children = await self.pg_pool.fetch(
                        """SELECT task_id, subject, notes FROM shared_delegations 
                           WHERE from_agent = $1 AND status = $2
                           AND notes::text LIKE $3""",
                        self.node_name, STATUS_PENDING, f'%[DEPENDS_ON] {task_id}%',
                    )
                    for child in children:
                        child_id = child["task_id"]
                        await self.pg_pool.execute(
                            "UPDATE shared_delegations SET status = $1, completed_at = NOW() WHERE task_id = $2",
                            STATUS_AVAILABLE, child_id,
                        )
                        log.info(f"🔗 DEPENDENCY: {child_id[:8]} '{child['subject'][:30]}' activated — parent {task_id[:8]} completed")
                        await self.add_note(child_id, f"[ACTIVATED] Parent {task_id[:8]} completed — now available", "system")
                except Exception as dep_err:
                    log.debug(f"Dependency trigger failed: {dep_err}")

            # ── Auto-move Kanban card based on delegation result ──
            try:
                kanban_card_id = row.get("kanban_card_id") or ""
                if kanban_card_id:
                    from .kanban import KanbanManager, _load_boards, _save_boards, _save_boards
                    km = KanbanManager(self.node_name)
                    boards = _load_boards()
                    for board in boards:
                        for c in board.get("cards", []):
                            if c["id"] == kanban_card_id:
                                result_text = row.get("result", "")[:500] if row.get("result") else ""
                                if row["status"] == STATUS_COMPLETED:
                                    # ── Agent-based review ──
                                    target_col = "review"
                                    
                                    c["delegation_result"] = result_text
                                    c["delegation_status"] = row["status"]
                                    c["result_file"] = row.get("result_file", "") if row.get("result_file") else ""
                                    c["completed_at"] = str(row.get("completed_at", ""))[:30]
                                    c["updated_at"] = time.time()
                                    c["review_status"] = "pending"
                                    # Agent history: executor entry is added by node.py callback
                                    # to avoid duplication. Only add review delegation here.
                                    if "agent_history" not in c:
                                        c["agent_history"] = []
                                    c["agent_history"].append({
                                        "agent": self.node_name,
                                        "role": "reviewer",
                                        "action": "review delegated",
                                        "review_task_id": str(row.get("task_id", ""))[:8],
                                        "timestamp": time.time(),
                                    })
                                    
                                    asyncio.create_task(self._delegate_review(
                                        str(row["task_id"]),
                                        row.get("subject", c.get("title", "")),
                                        result_text,
                                        row.get("assigned_agent", ""),
                                        kanban_card_id,
                                    ))
                                    # Keep old subtask logic below
                                    if False and analysis.get("subtasks"):
                                        # Create new Kanban cards for subtasks
                                        for sub in analysis["subtasks"]:
                                            sub_card = {
                                                "id": f"card-{int(time.time()*1000)}-{len(board.get('cards',[]))}",
                                                "title": sub["title"][:80],
                                                "column": "todo",
                                                "priority": sub.get("priority", c.get("priority", "medium")),
                                                "assigned_to": c.get("assigned_to", ""),
                                                "created_at": time.time(),
                                                "updated_at": time.time(),
                                                "description": sub.get("description", ""),
                                                "parent_card_id": kanban_card_id,
                                            }
                                            board.setdefault("cards", []).append(sub_card)
                                            log.info(f"Review: new subtask card '{sub_card['title']}' from '{c.get('title','')}'")
                                        target_col = "done"
                                else:
                                    # Cancelled/failed/expired → done (not actionable)
                                    target_col = "done"
                                    c["delegation_status"] = row["status"]
                                    c["delegation_result"] = result_text
                                    c["updated_at"] = time.time()
                                
                                km.move_card(board["id"], kanban_card_id, target_col)
                                # Re-save with result + analysis data
                                boards2 = _load_boards()
                                for b2 in boards2:
                                    for c2 in b2.get("cards", []):
                                        if c2["id"] == kanban_card_id:
                                            c2["delegation_result"] = c.get("delegation_result", "")
                                            c2["delegation_status"] = c.get("delegation_status", "")
                                            c2["result_file"] = c.get("result_file", "")
                                            c2["completed_at"] = c.get("completed_at", "")
                                            c2["updated_at"] = c.get("updated_at", time.time())
                                            if c.get("review_analysis"):
                                                c2["review_analysis"] = c["review_analysis"]
                                            if c.get("approval_required"):
                                                c2["approval_required"] = True
                                            if c.get("agent_history"):
                                                c2["agent_history"] = c["agent_history"]
                                            break
                                    # Also save subtask cards if any
                                    if analysis.get("subtasks") if row["status"] == STATUS_COMPLETED else False:
                                        for sub in analysis["subtasks"]:
                                            sub_card = {
                                                "id": f"card-{int(time.time()*1000)}-{len(b2.get('cards',[]))}",
                                                "title": sub["title"][:80],
                                                "column": "todo",
                                                "priority": sub.get("priority", "medium"),
                                                "assigned_to": c.get("assigned_to", ""),
                                                "created_at": time.time(),
                                                "updated_at": time.time(),
                                                "description": sub.get("description", ""),
                                                "parent_card_id": kanban_card_id,
                                            }
                                            b2.setdefault("cards", []).append(sub_card)
                                _save_boards(boards2)
                                log.info(f"Kanban auto-move: card '{c.get('title','')}' → {target_col} ({row['status']}) result={'yes' if c.get('delegation_result') else 'no'} approval={c.get('approval_required',False)}")
                                break
            except Exception as e:
                log.debug(f"Kanban auto-move skipped: {e}")

        # Cleanup expired result dedup entries (matches query 5-minute window)
        now2 = time.time()
        expired = [tid for tid, ts in self._results_seen_timestamps.items()
                   if now2 - ts > self._results_seen_ttl]
        for tid in expired:
            self._results_seen.discard(tid)
            self._results_seen_timestamps.pop(tid, None)

    async def _check_dependencies(self):
        """Activate pending tasks whose parent (depends_on) has completed.
        Runs every poll cycle — checks all pending tasks with [DEPENDS_ON] notes."""
        try:
            # Find pending tasks with dependency notes
            pending = await self.pg_pool.fetch(
                """SELECT task_id, subject, notes FROM shared_delegations
                   WHERE from_agent = $1 AND status = $2
                   AND notes::text LIKE '%[DEPENDS_ON]%'""",
                self.node_name, STATUS_PENDING,
            )
            if pending:
                log.info(f"Dependency check: found {len(pending)} pending tasks with dependencies")
            else:
                log.debug("Dependency check: no pending tasks with [DEPENDS_ON] notes")
            for row in pending:
                task_id = str(row["task_id"])
                notes = row.get("notes", [])
                # asyncpg returns JSONB as string — parse it
                if isinstance(notes, str) and notes:
                    try:
                        notes = json.loads(notes)
                    except (json.JSONDecodeError, TypeError):
                        pass
                if not notes:
                    continue
                # Find the depends_on task_id in notes
                for note_entry in (notes if isinstance(notes, list) else []):
                    note_text = note_entry.get("note", "") if isinstance(note_entry, dict) else str(note_entry)
                    if "[DEPENDS_ON]" in note_text:
                        parent_id = note_text.replace("[DEPENDS_ON]", "").strip()
                        # Check if parent is completed
                        parent_status = await self.pg_pool.fetchval(
                            "SELECT status FROM shared_delegations WHERE task_id = $1::text::uuid",
                            parent_id,
                        )
                        if parent_status == STATUS_COMPLETED:
                            await self.pg_pool.execute(
                                "UPDATE shared_delegations SET status = $1, completed_at = NOW() WHERE task_id = $2::text::uuid",
                                STATUS_AVAILABLE, task_id,
                            )
                            log.info(f"🔗 DEPENDENCY: {task_id[:8]} '{str(row['subject'])[:30]}' activated — parent {parent_id[:8]} completed")
                            await self.add_note(task_id, f"[ACTIVATED] Parent {parent_id[:8]} completed — now available", "system")
                        elif parent_status in (STATUS_CANCELLED, STATUS_FAILED, STATUS_EXPIRED):
                            # Parent failed — activate anyway so it can be reassigned or manually handled
                            await self.pg_pool.execute(
                                "UPDATE shared_delegations SET status = $1, completed_at = NOW() WHERE task_id = $2::text::uuid",
                                STATUS_AVAILABLE, task_id,
                            )
                            log.warning(f"🔗 DEPENDENCY: {task_id[:8]} activated — parent {parent_id[:8]} {parent_status} (cascading)")
                            await self.add_note(task_id, f"[CASCADE] Parent {parent_id[:8]} {parent_status} — activated for manual handling", "system")
                        break
        except Exception as e:
            log.warning(f"Dependency check failed: {e}")

    def _analyze_review(self, result_text: str, card_title: str) -> dict:
        """Analyze a delegation result to determine review action.
        
        Returns:
            {
                "target_col": "review" | "done" | "todo",
                "needs_approval": bool,
                "subtasks": [{"title": str, "description": str, "priority": str}],
                "reason": str,
                "summary": str,
            }
        """
        result_lower = result_text.lower() if result_text else ""
        
        # Default: clean completion → done
        analysis = {
            "target_col": "done",
            "needs_approval": False,
            "subtasks": [],
            "reason": "",
            "summary": result_text[:200] if result_text else "No result text",
        }
        
        if not result_text:
            analysis["target_col"] = "review"
            analysis["needs_approval"] = True
            analysis["reason"] = "No result text — needs human review"
            return analysis
        
        # ── Rule: approval needed keywords ──
        approval_keywords = [
            "needs approval", "requires approval", "pending approval",
            "needs review", "requires review", "awaiting decision",
            "needs human", "requires human", "needs decision",
            "jóváhagyás", "döntés szükséges", "emberi döntés",
            "needs your decision", "requires your approval",
        ]
        for kw in approval_keywords:
            if kw in result_lower:
                analysis["target_col"] = "review"
                analysis["needs_approval"] = True
                analysis["reason"] = f"Keyword '{kw}' found in result"
                return analysis
        
        # ── Rule: high-priority / security / production changes need approval ──
        high_risk_keywords = [
            "production", "prod deploy", "security", "credential",
            "password", "api key", "secret", "delete data", "drop table",
            "éles rendszer", "biztonsági", "jelszó", "titkos",
        ]
        for kw in high_risk_keywords:
            if kw in result_lower:
                analysis["target_col"] = "review"
                analysis["needs_approval"] = True
                analysis["reason"] = f"High-risk keyword '{kw}' — needs approval"
                return analysis
        
        # ── Rule: subtask detection ──
        # Look for TODO/FIXME/next step patterns
        import re
        todo_patterns = [
            r"(?:TODO|FIXME|NEXT|NEXT STEP|KÖVETKEZŐ)[\s:]+(.+)",
            r"(?:follow.up|follow-up|további feladat)[\s:]+(.+)",
            r"(?:remaining|outstanding|hátralévő)[\s:]+(.+)",
        ]
        subtasks = []
        for pattern in todo_patterns:
            matches = re.findall(pattern, result_text, re.IGNORECASE)
            for m in matches[:3]:  # Max 3 subtasks
                title = m.strip()[:80]
                if title and len(title) > 5:
                    subtasks.append({
                        "title": title,
                        "description": f"Auto-detected subtask from '{card_title}'",
                        "priority": "medium",
                    })
        
        if subtasks:
            analysis["subtasks"] = subtasks
            analysis["target_col"] = "done"  # Parent done, subtasks in todo
            analysis["reason"] = f"{len(subtasks)} subtask(s) detected"
        
        return analysis

    # ── Query helpers ──

    async def get_my_delegations(self, status: Optional[str] = None) -> List[Dict]:
        """Get tasks delegated BY this node."""
        if status:
            rows = await self.pg_pool.fetch(
                """SELECT * FROM shared_delegations WHERE from_agent = $1 AND status = $2 
                   ORDER BY created_at DESC LIMIT 50""",
                self.node_name, status,
            )
        else:
            rows = await self.pg_pool.fetch(
                """SELECT * FROM shared_delegations WHERE from_agent = $1 
                   ORDER BY created_at DESC LIMIT 50""",
                self.node_name,
            )
        return [dict(r) for r in rows]

    async def get_assigned_tasks(self, status: Optional[str] = None) -> List[Dict]:
        """Get tasks delegated TO this node (or claimed by this node)."""
        if status:
            rows = await self.pg_pool.fetch(
                """SELECT * FROM shared_delegations 
                   WHERE (to_agent = $1 OR assigned_agent = $1) AND status = $2 
                   ORDER BY priority DESC, created_at DESC LIMIT 50""",
                self.node_name, status,
            )
        else:
            rows = await self.pg_pool.fetch(
                """SELECT * FROM shared_delegations 
                   WHERE to_agent = $1 OR assigned_agent = $1
                   ORDER BY priority DESC, created_at DESC LIMIT 50""",
                self.node_name,
            )
        return [dict(r) for r in rows]

    async def get_available_tasks(self) -> List[Dict]:
        """Get tasks available for claiming."""
        rows = await self.pg_pool.fetch(
            """SELECT * FROM shared_delegations 
               WHERE status = $1 AND (expires_at IS NULL OR expires_at > NOW())
               ORDER BY priority DESC, created_at ASC LIMIT 20""",
            STATUS_AVAILABLE,
        )
        return [dict(r) for r in rows]

    async def get_all_delegations(self, limit: int = 50) -> List[Dict]:
        """Get all delegations (admin view)."""
        rows = await self.pg_pool.fetch(
            """SELECT * FROM shared_delegations 
               ORDER BY created_at DESC LIMIT $1""",
            limit,
        )
        return [dict(r) for r in rows]

    async def get_delegation_stats(self) -> Dict:
        """Get delegation statistics."""
        rows = await self.pg_pool.fetch(
            """SELECT status, count(*) as cnt 
               FROM shared_delegations 
               GROUP BY status"""
        )
        stats = {}
        for r in rows:
            stats[r["status"]] = r["cnt"]
        stats["total"] = sum(stats.values())
        return stats

    # ── Shared Context ──

    async def set_context(self, key: str, value: str, value_type: str = "text", expires_minutes: int = 0) -> bool:
        """Set a shared context value. Available to all agents."""
        expires_at = None
        if expires_minutes > 0:
            from datetime import datetime, timedelta, timezone
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)
        await self.pg_pool.execute(
            """INSERT INTO shared_context (agent, context_key, context_value, value_type, expires_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (context_key) DO UPDATE SET
               context_value = EXCLUDED.context_value,
               value_type = EXCLUDED.value_type,
               agent = EXCLUDED.agent,
               updated_at = NOW(),
               expires_at = EXCLUDED.expires_at""",
            self.node_name, key, value, value_type, expires_at,
        )
        return True

    async def get_context(self, key: str) -> Optional[str]:
        """Get a shared context value."""
        # Clean expired entries
        await self.pg_pool.execute(
            "DELETE FROM shared_context WHERE expires_at IS NOT NULL AND expires_at < NOW()"
        )
        row = await self.pg_pool.fetchrow(
            "SELECT context_value FROM shared_context WHERE context_key = $1", key
        )
        return row["context_value"] if row else None

    async def get_all_context(self, prefix: str = "") -> List[Dict]:
        """Get all context entries, optionally filtered by key prefix."""
        await self.pg_pool.execute(
            "DELETE FROM shared_context WHERE expires_at IS NOT NULL AND expires_at < NOW()"
        )
        if prefix:
            rows = await self.pg_pool.fetch(
                "SELECT * FROM shared_context WHERE context_key LIKE $1 ORDER BY updated_at DESC",
                prefix + "%",
            )
        else:
            rows = await self.pg_pool.fetch(
                "SELECT * FROM shared_context ORDER BY updated_at DESC LIMIT 50"
            )
        return [dict(r) for r in rows]

    async def delete_context(self, key: str) -> bool:
        """Delete a shared context entry."""
        result = await self.pg_pool.execute(
            "DELETE FROM shared_context WHERE context_key = $1", key
        )
        return "DELETE 1" in result

    # ── Circuit Breaker ──

    def record_success(self, peer: str):
        """Record a successful interaction with a peer — reset circuit breaker."""
        if peer in self._circuit_breakers:
            self._circuit_breakers[peer]["failures"] = 0
            self._circuit_breakers[peer]["open"] = False

    def record_failure(self, peer: str):
        """Record a failed interaction with a peer — increment circuit breaker."""
        if peer not in self._circuit_breakers:
            self._circuit_breakers[peer] = {"failures": 0, "last_fail": 0.0, "open": False}
        cb = self._circuit_breakers[peer]
        cb["failures"] = cb.get("failures", 0) + 1
        cb["last_fail"] = time.time()
        if cb["failures"] >= self._circuit_breaker_threshold:
            cb["open"] = True
            log.warning(f"Circuit breaker OPEN for {peer}: {cb['failures']} consecutive failures")

    def is_circuit_open(self, peer: str) -> bool:
        """Check if the circuit breaker is open for a peer."""
        cb = self._circuit_breakers.get(peer)
        if not cb:
            return False
        if cb.get("open"):
            # Check if cooldown has expired — half-open state
            if time.time() - cb.get("last_fail", 0) >= self._circuit_breaker_cooldown:
                log.info(f"Circuit breaker HALF-OPEN for {peer}: cooldown expired, allowing attempt")
                cb["open"] = False
                return False
            return True
        return False

    # ─────────────────────────────────────────────────────────────────
    #  Agent-based Review System
    # ─────────────────────────────────────────────────────────────────

    async def _select_reviewer(self, from_agent: str, assigned_agent: str) -> Optional[str]:
        """Select a reviewer agent: 3rd party if available, else delegator."""
        try:
            online_agents = await self.pg_pool.fetch(
                "SELECT DISTINCT node_name FROM mesh_node_health WHERE status = 'active'",
            )
            all_agents = [r["node_name"] for r in online_agents]
            candidates = [a for a in all_agents if a != from_agent and a != assigned_agent]
            
            if candidates:
                reviewer = None
                min_load = 999
                for agent in candidates:
                    load = await self.pg_pool.fetchval(
                        "SELECT COUNT(*) FROM shared_delegations WHERE to_agent = $1 AND task_type = 'code_review' AND status IN ($2, $3)",
                        agent, STATUS_AVAILABLE, STATUS_RUNNING,
                    )
                    if load is None:
                        load = 0
                    if load < min_load:
                        min_load = load
                        reviewer = agent
                
                if reviewer:
                    log.info(f"🔍 Review: selected 3rd-party reviewer '{reviewer}' (load={min_load})")
                    return reviewer
            
            log.info(f"🔍 Review: no 3rd-party agent — delegator '{from_agent}' will review")
            return from_agent
        except Exception as e:
            log.warning(f"Reviewer selection failed: {e} — falling back to delegator")
            return from_agent

    async def _delegate_review(self, original_task_id: str, original_subject: str,
                                result_text: str, assigned_agent: str, kanban_card_id: str):
        """Delegate review to a 3rd-party agent (or delegator as fallback)."""
        try:
            # Dedup: prevent concurrent duplicate reviews of the SAME execution.
            # The old in-memory set blocked re-review after a REJECT too — but a
            # rejected task is re-queued as available and, when another agent
            # completes it, THAT new result must be reviewed again. Track
            # (task_id, result-hash) pairs instead of task_id alone.
            if not hasattr(self, '_reviewed_results'):
                self._reviewed_results = set()
            import hashlib as _hl
            result_hash = _hl.md5((result_text or "")[:2000].encode("utf-8", "replace")).hexdigest()[:12]
            review_key = f"{str(original_task_id)}:{result_hash}"
            if review_key in self._reviewed_results:
                return
            self._reviewed_results.add(review_key)
            # Keep the set bounded
            if len(self._reviewed_results) > 200:
                self._reviewed_results = set(sorted(self._reviewed_results)[-100:])
            from_agent = self.node_name
            reviewer = await self._select_reviewer(from_agent, assigned_agent)
            if not reviewer:
                reviewer = from_agent
            
            # If reviewer is the same as delegator, do local review (no self-delegation)
            if reviewer == from_agent:
                analysis = self._analyze_review(result_text, original_subject[:60])
                verdict = "accept"
                reason = analysis.get("reason", "Auto-approved")
                if analysis.get("needs_approval"):
                    verdict = "reject"
                    reason = analysis.get("reason", "Needs human review")
                
                log.info(f"🔍 Local review (self): task={str(original_task_id)[:8]} verdict={verdict} reason={reason[:60]}")
                
                if kanban_card_id:
                    try:
                        from .kanban import _load_boards, _save_boards
                        boards = _load_boards()
                        for board in boards:
                            for c in board.get("cards", []):
                                if c["id"] == kanban_card_id:
                                    c["review_status"] = verdict
                                    c["review_reason"] = reason[:300]
                                    c["reviewed_at"] = time.time()
                                    c["updated_at"] = time.time()
                                    if verdict == "accept":
                                        c["column"] = "done"
                                    else:
                                        c["column"] = "todo"
                                        c["review_status"] = "rejected"
                                    if "agent_history" not in c:
                                        c["agent_history"] = []
                                    c["agent_history"].append({
                                        "agent": from_agent,
                                        "role": "reviewer",
                                        "action": f"review: {verdict}",
                                        "verdict": verdict,
                                        "reason": reason[:300],
                                        "timestamp": time.time(),
                                    })
                                    break
                        _save_boards(boards)
                    except Exception as ke:
                        log.warning(f"Local review Kanban update failed: {ke}")
                
                if verdict == "accept":
                    await self.add_note(str(original_task_id), f"[REVIEW_ACCEPTED] {reason[:200]}", "system")
                else:
                    await self.add_note(str(original_task_id), f"[REVIEW_REJECTED] {reason[:200]}", "system")
                    await self.pg_pool.execute(
                        "UPDATE shared_delegations SET status = $1 WHERE task_id = $2",
                        STATUS_AVAILABLE, original_task_id,
                    )
                return
            
            review_subject = f"[REVIEW] {original_subject[:60]}"
            review_desc = (
                f"You are reviewing a task result from agent '{assigned_agent}'.\n\n"
                f"Original task: {original_subject}\n\n"
                f"Result:\n{result_text[:2000]}\n\n"
                f"Evaluate the result. Respond in JSON:\n"
                f'{{"verdict": "accept" | "reject", "reason": "brief explanation"}}\n'
                f"- accept: result is correct and complete\n"
                f"- reject: result is wrong, incomplete, or needs rework\n"
            )
            
            review_task_id = str(uuid.uuid4())
            desc_json = json.dumps({
                "description": review_desc,
                "context": {"original_task_id": str(original_task_id), "kanban_card_id": kanban_card_id},
            })
            
            await self.pg_pool.execute(
                """INSERT INTO shared_delegations 
                   (task_id, from_agent, to_agent, subject, description, task_type, priority, status, created_at)
                   VALUES ($1, $2, $3, $4, $5, 'code_review', 3, $6, NOW())""",
                review_task_id, from_agent, reviewer, review_subject, desc_json, STATUS_AVAILABLE,
            )
            
            await self.add_note(review_task_id, f"[REVIEW_OF] {original_task_id}", "system")
            await self.add_note(review_task_id, f"[REVIEW_CARD] {kanban_card_id}", "system")
            
            if kanban_card_id:
                try:
                    from .kanban import _load_boards, _save_boards
                    boards = _load_boards()
                    for board in boards:
                        for c in board.get("cards", []):
                            if c["id"] == kanban_card_id:
                                if "agent_history" not in c:
                                    c["agent_history"] = []
                                c["agent_history"].append({
                                    "agent": reviewer,
                                    "role": "reviewer",
                                    "action": "review delegated",
                                    "review_task_id": review_task_id[:8],
                                    "timestamp": time.time(),
                                })
                                c["reviewer"] = reviewer
                                c["review_task_id"] = review_task_id
                                c["updated_at"] = time.time()
                                break
                    _save_boards(boards)
                except Exception as ke:
                    log.warning(f"Review delegation Kanban update failed: {ke}")

            log.info(f"🔍 Review delegated: task={review_task_id[:8]} reviewer={reviewer} original={str(original_task_id)[:8]}")
        except Exception as e:
            log.error(f"Review delegation failed: {e}")

    async def _process_review_result(self, review_task_id: str, review_result: str):
        """Process a completed review — accept or reject the original task."""
        try:
            import re as _re
            verdict = "accept"
            reason = ""
            
            json_match = _re.search(r'\{[^{}]*"verdict"[^{}]*\}', review_result, _re.DOTALL)
            if json_match:
                try:
                    verdict_data = json.loads(json_match.group())
                    verdict = verdict_data.get("verdict", "accept")
                    reason = verdict_data.get("reason", "")
                except json.JSONDecodeError:
                    pass
            else:
                result_lower = review_result.lower()
                if "reject" in result_lower or "redo" in result_lower or "újra" in result_lower:
                    verdict = "reject"
                if "accept" in result_lower or "correct" in result_lower or "rendben" in result_lower:
                    verdict = "accept"
            
            notes_row = await self.pg_pool.fetchrow(
                "SELECT notes FROM shared_delegations WHERE task_id = $1",
                review_task_id,
            )
            if not notes_row:
                return
            
            notes = notes_row.get("notes", [])
            if isinstance(notes, str):
                try:
                    notes = json.loads(notes)
                except json.JSONDecodeError:
                    notes = []
            
            original_task_id = None
            kanban_card_id = None
            for note_entry in (notes if isinstance(notes, list) else []):
                note_text = note_entry.get("note", "") if isinstance(note_entry, dict) else str(note_entry)
                if "[REVIEW_OF]" in note_text:
                    original_task_id = note_text.replace("[REVIEW_OF]", "").strip()
                elif "[REVIEW_CARD]" in note_text:
                    kanban_card_id = note_text.replace("[REVIEW_CARD]", "").strip()
            
            if not original_task_id:
                return
            
            log.info(f"🔍 Review result: task={str(original_task_id)[:8]} verdict={verdict} reason={reason[:60]}")
            
            if kanban_card_id:
                try:
                    from .kanban import _load_boards, _save_boards
                    boards = _load_boards()
                    for board in boards:
                        for c in board.get("cards", []):
                            if c["id"] == kanban_card_id:
                                c["review_status"] = verdict
                                c["review_reason"] = reason[:300]
                                c["reviewed_at"] = time.time()
                                c["updated_at"] = time.time()
                                if verdict == "accept":
                                    c["column"] = "done"
                                    log.info(f"🔍 Review ACCEPT: card → done")
                                else:
                                    c["column"] = "todo"
                                    c["review_status"] = "rejected"
                                    log.info(f"🔍 Review REJECT: card → todo (redispatch)")
                                if "agent_history" not in c:
                                    c["agent_history"] = []
                                c["agent_history"].append({
                                    "agent": c.get("reviewer", "unknown"),
                                    "role": "reviewer",
                                    "action": f"review: {verdict}",
                                    "verdict": verdict,
                                    "reason": reason[:300],
                                    "timestamp": time.time(),
                                })
                                break
                    _save_boards(boards)
                except Exception as ke:
                    log.warning(f"Review Kanban update failed: {ke}")
            
            if verdict == "accept":
                await self.add_note(original_task_id, f"[REVIEW_ACCEPTED] {reason[:200]}", "system")
                log.info(f"🔍 Review: {str(original_task_id)[:8]} ACCEPTED")
            else:
                await self.add_note(original_task_id, f"[REVIEW_REJECTED] {reason[:200]}", "system")
                await self.pg_pool.execute(
                    "UPDATE shared_delegations SET status = $1, completed_at = NOW() WHERE task_id = $2",
                    STATUS_AVAILABLE, original_task_id,
                )
                log.info(f"🔍 Review: {str(original_task_id)[:8]} REJECTED — redispatched")
        except Exception as e:
            log.error(f"Review result processing failed: {e}")
