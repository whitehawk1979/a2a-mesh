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
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

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
    return ascii_friendly.encode('ascii', 'backslashreplace').decode('ascii')


class DelegationManager:
    """Manages task delegation between mesh nodes via shared_delegations table."""

    def __init__(self, pg_pool, node_name: str):
        self.pg_pool = pg_pool  # AsyncDBPool instance
        self.node_name = node_name
        self._running = False
        self._poll_task = None
        self._active_tasks: Dict[str, Dict] = {}
        self._handlers: Dict[str, callable] = {}
        self._poll_interval = 5.0
        self._on_result_callback = None
        # Fan-out dedup: track (from_agent, subject) combos we've already claimed
        self._claimed_subjects: set = set()

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
    ) -> Union[str, List[str]]:
        """Delegate a task to another agent or make it available for any agent.
        
        Args:
            to_agent: Target agent name, or 'any' for available tasks
            subject: Task subject/title
            description: Task description
            task_type: Type of task (generic, monitoring, code, research, analysis)
            priority: Priority (1=low, 5=normal, 7=high, 10=critical)
            context: Additional context data
            timeout_minutes: Timeout in minutes
            available: If True, task is available for any agent to claim
            fan_out: If > 0, creates N identical tasks (one per available agent),
                     first to complete wins, others are cancelled. No duplicate work.
            eligible_agents: Optional list of agent names that can claim this task
                            (only used when available=True)
        """
        task_id = str(uuid.uuid4())
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
        status = STATUS_AVAILABLE if available else STATUS_PENDING
        actual_to = "any" if available else to_agent

        await self.pg_pool.execute(
            """INSERT INTO shared_delegations 
               (task_id, from_agent, to_agent, subject, description, status, priority, expires_at, assigned_agent, max_retries)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""",
            task_id, self.node_name, actual_to, subject, desc_json,
            status, priority, expires_at, None, max_retries,
        )

        log.info(f"Delegated task {task_id} to {actual_to}: {subject} (P{priority}, {status})")

        # ── Fan-out: create identical tasks for N agents ──
        if fan_out > 0:
            task_ids = [task_id]
            for i in range(1, fan_out):
                fan_id = str(uuid.uuid4())
                await self.pg_pool.execute(
                    """INSERT INTO shared_delegations 
                       (task_id, from_agent, to_agent, subject, description, status, priority, expires_at, assigned_agent, max_retries)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""",
                    fan_id, self.node_name, actual_to, subject, desc_json,
                    status, priority, expires_at, None, max_retries,
                )
                task_ids.append(fan_id)
            log.info(f"Fan-out: created {len(task_ids)} tasks for '{subject}' (P{priority})")
            return task_ids

        # Notify via A2A message
        try:
            from .a2a_message import A2AMessage
            recipient = "broadcast" if available else to_agent
            msg = A2AMessage.create(
                sender=self.node_name,
                recipient=recipient,
                msg_type="delegation",
                subject=subject,
                content=f"New {'available' if available else ''} task: {subject}",
                priority=priority,
            )
        except Exception as e:
            log.debug(f"Could not send delegation notification: {e}")

        return task_id

    async def claim_task(self, task_id: str, agent_name: Optional[str] = None) -> bool:
        """Claim an available task. Agent can claim tasks marked as 'available'.
        If eligible_agents is set in the task description, only those agents can claim."""
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
        while self._running:
            try:
                await self._check_expired()
                await self._poll_pending()
                await self._poll_available()
                await self._check_results()
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Delegation poll error: {e}")
                await asyncio.sleep(self._poll_interval * 2)

    async def _check_expired(self):
        """Mark expired tasks."""
        await self.pg_pool.execute(
            """UPDATE shared_delegations SET status = $1 
               WHERE status IN ($2, $3, $4) AND expires_at < NOW()""",
            STATUS_EXPIRED, STATUS_PENDING, STATUS_AVAILABLE, STATUS_ACCEPTED,
        )

    async def _poll_pending(self):
        """Poll for pending tasks specifically assigned to this node."""
        rows = await self.pg_pool.fetch(
            """SELECT * FROM shared_delegations 
               WHERE to_agent = $1 AND status = $2 
               ORDER BY priority DESC, created_at ASC LIMIT 5""",
            self.node_name, STATUS_PENDING,
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
        Priority-aware: high-priority tasks (7-10) only claimed if CPU load is low."""
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

        for task in rows:
            task_dict = dict(task)
            task_id = task_dict.get("task_id", "")
            from_agent = task_dict.get("from_agent", "")
            priority = int(task_dict.get("priority", 5))
            
            # Don't claim our own tasks — let other agents handle them
            if from_agent == self.node_name:
                continue
            
            # Fan-out dedup: don't claim a fan-out sibling we already claimed
            subject_key = (from_agent, task_dict.get("subject", ""))
            if subject_key in self._claimed_subjects:
                log.debug(f"Skipping fan-out sibling {task_id}: already claimed same subject from {from_agent}")
                continue
            
            # Priority-aware: skip tasks if we're overloaded
            # P7+: skip if CPU > 80%
            # P4-P6: skip if CPU > 90%
            # P1-P3: always claim (low priority = easy tasks)
            if priority >= 7 and cpu_load > 80:
                log.debug(f"Skipping P{priority} task {task_id}: CPU load {cpu_load:.0f}% > 80%")
                continue
            elif priority >= 4 and cpu_load > 90:
                log.debug(f"Skipping P{priority} task {task_id}: CPU load {cpu_load:.0f}% > 90%")
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
                continue
            
            # Try to claim it — add jitter to spread claims across nodes
            await asyncio.sleep(random.uniform(0.1, 0.5))
            claimed = await self.claim_task(task_id)
            if claimed:
                # Track fan-out dedup: remember we claimed this (from_agent, subject)
                self._claimed_subjects.add(subject_key)
                log.info(f"Claimed available task {task_id}: {task_dict.get('subject', '?')} (P{priority}, CPU {cpu_load:.0f}%)")
                asyncio.create_task(self._execute_task(task_dict))

    async def _execute_task(self, task: Dict):
        """Execute a delegated task using registered handlers."""
        task_id = str(task.get("task_id", ""))
        subject = task.get("subject", "unknown")
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

        log.info(f"Executing task {task_id} of type {task_type}: {subject}")

        # Mark as running
        self._active_tasks[task_id] = task
        await self.pg_pool.execute(
            "UPDATE shared_delegations SET status = $1, assigned_agent = $2 WHERE task_id = $3",
            STATUS_RUNNING, self.node_name, task_id,
        )
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
                result_text = _safe_ascii(str(handler_result.get("result", "")))[:4000]
                # Store files in shared_files table (base64-encoded to avoid SQL_ASCII issues)
                import base64
                files = handler_result.get("files", [])
                for f in files:
                    try:
                        raw_content = f.get("content", "")
                        encoded_content = base64.b64encode(raw_content.encode("utf-8")).decode("ascii")
                        file_id = await self.pg_pool.fetchval(
                            """INSERT INTO shared_files 
                               (sender_agent, recipient_agent, filename, content_type, file_size, encoding, content, description, status)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ready')
                               RETURNING id""",
                            self.node_name,
                            task.get("from_agent", ""),
                            f.get("filename", "result.txt"),
                            f.get("content_type", "text/plain"),
                            f.get("size", len(raw_content)),
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
                result_text = _safe_ascii(str(handler_result))[:4000]

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

            # ── Fan-out: cancel sibling tasks with same subject from same sender ──
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
                        log.info(f"Fan-out: cancelled {cancelled} sibling tasks for '{subject_val}'")
            except Exception as e:
                log.debug(f"Fan-out cancel check (non-critical): {e}")

        except Exception as e:
            log.error(f"Task {task_id} failed: {e}")
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
                    STATUS_FAILED, _safe_ascii(str(e))[:4000], task_id,
                )
                await self.add_note(task_id, f"Task failed permanently after {max_retries} retries: {str(e)[:150]}")

        finally:
            self._active_tasks.pop(task_id, None)

    async def _check_results(self):
        """Check for completed tasks that we delegated out."""
        rows = await self.pg_pool.fetch(
            """SELECT * FROM shared_delegations 
               WHERE from_agent = $1 AND status IN ($2, $3) 
               AND completed_at > NOW() - INTERVAL '1 minute'
               ORDER BY completed_at DESC LIMIT 10""",
            self.node_name, STATUS_COMPLETED, STATUS_FAILED,
        )

        for row in rows:
            if self._on_result_callback:
                try:
                    if asyncio.iscoroutinefunction(self._on_result_callback):
                        await self._on_result_callback(dict(row))
                    else:
                        self._on_result_callback(dict(row))
                except Exception as e:
                    log.debug(f"Result callback error: {e}")

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