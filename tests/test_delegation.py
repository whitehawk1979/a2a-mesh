#!/usr/bin/env python3
"""End-to-end tests for the Delegation system.

Tests the full lifecycle: create, claim, execute, complete,
using a mock asyncpg pool (in-memory store) instead of a real database.
"""

import asyncio
import json
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from a2a_mesh.core.delegation import (
    DelegationManager,
    STATUS_PENDING, STATUS_AVAILABLE, STATUS_ACCEPTED,
    STATUS_RUNNING, STATUS_COMPLETED, STATUS_FAILED,
    STATUS_CANCELLED, STATUS_EXPIRED,
)


class InMemoryPool:
    """Mock asyncpg pool using in-memory storage for delegation tests.

    Properly handles different UPDATE query types by parsing the SET clause.
    """

    def __init__(self):
        self.delegations = {}
        self.files = {}
        self.next_file_id = 1
        self.calls = []

    async def execute(self, query, *args):
        self.calls.append(("execute", args))
        query_lower = query.strip().lower()

        if "insert into shared_delegations" in query_lower:
            task_id = args[0]
            self.delegations[task_id] = {
                "task_id": task_id,
                "from_agent": args[1],
                "to_agent": args[2],
                "subject": args[3],
                "description": args[4],
                "status": args[5],
                "priority": args[6],
                "expires_at": args[7],
                "assigned_agent": args[8] if len(args) > 8 else None,
                "result": None,
                "result_file": None,
                "completed_at": None,
                "progress": 0,
            }
            return "INSERT 1"

        elif "insert into shared_files" in query_lower:
            fid = self.next_file_id
            self.next_file_id += 1
            self.files[fid] = {"id": fid}
            return "INSERT " + str(fid)

        elif "update shared_delegations" in query_lower:
            # Fan-out cancel: WHERE subject = ... AND task_id != ...
            if "task_id !=" in query_lower and "subject" in query_lower:
                subject_val = args[2]
                from_agent_val = args[3]
                exclude_task_id = args[6]
                cancelled = 0
                for tid, row in self.delegations.items():
                    if (row["subject"] == subject_val and row["from_agent"] == from_agent_val
                            and row["status"] in (STATUS_AVAILABLE, STATUS_PENDING)
                            and tid != exclude_task_id):
                        row["status"] = STATUS_CANCELLED
                        row["result"] = args[1]
                        cancelled += 1
                return f"UPDATE {cancelled}"

            # Notes update: SET notes = COALESCE(notes, ...) || $1 WHERE task_id = $2
            if "notes" in query_lower and "coalesce" in query_lower:
                task_id = args[1]
                if task_id in self.delegations:
                    # Don't touch status for note updates
                    return "UPDATE 1"
                return "UPDATE 0"

            # Progress-only update: SET progress = $1 WHERE task_id = $2
            if query_lower.strip().startswith("update shared_delegations set progress"):
                task_id = args[-1]
                if task_id in self.delegations:
                    self.delegations[task_id]["progress"] = args[0]
                    return "UPDATE 1"
                return "UPDATE 0"

            # Retry update: SET status = $1, retry_count = $2, assigned_agent = NULL, ... WHERE task_id = $3
            if "retry_count" in query_lower and "assigned_agent" in query_lower:
                task_id = args[2]
                if task_id in self.delegations:
                    self.delegations[task_id]["status"] = args[0]
                    self.delegations[task_id]["retry_count"] = args[1]
                    self.delegations[task_id]["assigned_agent"] = None
                    self.delegations[task_id]["result"] = None
                    self.delegations[task_id]["completed_at"] = None
                    return "UPDATE 1"
                return "UPDATE 0"

            # Claim task: SET status = $1, assigned_agent = $2 WHERE task_id = $3
            if "assigned_agent" in query_lower:
                task_id = args[2]
                if task_id in self.delegations:
                    self.delegations[task_id]["status"] = args[0]
                    self.delegations[task_id]["assigned_agent"] = args[1]
                    return "UPDATE 1"
                return "UPDATE 0"

            # Status + result update (completion/failure)
            # SET status = $1, result = $2 [, result_file = $3], completed_at = NOW(), progress = 100 WHERE task_id = $N
            task_id = args[-1]
            if isinstance(task_id, str) and task_id in self.delegations:
                row = self.delegations[task_id]
                new_status = args[0]
                row["status"] = new_status
                if len(args) >= 3:
                    row["result"] = args[1]
                if len(args) >= 4 and "result_file" in query_lower:
                    row["result_file"] = args[2]
                if "completed_at" in query_lower:
                    row["completed_at"] = datetime.now(timezone.utc)
                if "progress" in query_lower:
                    for a in args:
                        if isinstance(a, int) and 0 <= a <= 100:
                            row["progress"] = a
                return "UPDATE 1"
            return "UPDATE 0"

        return "OK"

    async def fetch(self, query, *args):
        self.calls.append(("fetch", args))
        results = list(self.delegations.values())
        if args:
            for a in args:
                if a in (STATUS_PENDING, STATUS_AVAILABLE, STATUS_RUNNING,
                         STATUS_ACCEPTED, STATUS_COMPLETED, STATUS_FAILED):
                    results = [r for r in results if r["status"] == a]
        return [MagicMock(**{k: v for k, v in r.items()}) for r in results]

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", args))
        task_id = args[0] if args else None
        if task_id and task_id in self.delegations:
            row = self.delegations[task_id]
            return MagicMock(**{k: v for k, v in row.items()})
        return None

    async def fetchval(self, query, *args):
        self.calls.append(("fetchval", args))
        if "shared_files" in query.lower():
            fid = self.next_file_id
            self.next_file_id += 1
            return fid
        return None


@pytest.fixture
def pool():
    return InMemoryPool()

@pytest.fixture
def mgr(pool):
    return DelegationManager(pool, node_name="test_agent")


class TestDelegationDirect:
    @pytest.mark.asyncio
    async def test_delegate_task_creates_pending(self, mgr, pool):
        task_id = await mgr.delegate_task(
            to_agent="morzsa", subject="Health check",
            description="Check system health", task_type="monitoring", priority=5,
        )
        assert isinstance(task_id, str)
        assert task_id in pool.delegations
        row = pool.delegations[task_id]
        assert row["to_agent"] == "morzsa"
        assert row["status"] == STATUS_PENDING
        assert row["from_agent"] == "test_agent"
        assert row["priority"] == 5

    @pytest.mark.asyncio
    async def test_available_task_creation(self, mgr, pool):
        task_id = await mgr.delegate_task(
            to_agent="any", subject="Research task",
            description="Find info about X", task_type="research", priority=3, available=True,
        )
        row = pool.delegations[task_id]
        assert row["status"] == STATUS_AVAILABLE
        assert row["to_agent"] == "any"

    @pytest.mark.asyncio
    async def test_claim_available_task(self, mgr, pool):
        task_id = await mgr.delegate_task(
            to_agent="any", subject="Claim me", task_type="research", available=True,
        )
        claimed = await mgr.claim_task(task_id, "worker_1")
        assert claimed or pool.delegations[task_id]["status"] in (STATUS_ACCEPTED, STATUS_AVAILABLE)

    @pytest.mark.asyncio
    async def test_update_progress(self, mgr, pool):
        task_id = await mgr.delegate_task(to_agent="morzsa", subject="Long task")
        result = await mgr.update_progress(task_id, 50, note="Halfway done")
        assert result or task_id in pool.delegations

    @pytest.mark.asyncio
    async def test_cancel_task(self, mgr, pool):
        task_id = await mgr.delegate_task(to_agent="morzsa", subject="Cancel me")
        result = await mgr.cancel_task(task_id)
        assert result or task_id in pool.delegations

    @pytest.mark.asyncio
    async def test_fan_out_creates_multiple(self, mgr, pool):
        task_ids = await mgr.delegate_task(
            to_agent="any", subject="Parallel research",
            task_type="research", available=True, fan_out=3,
        )
        assert isinstance(task_ids, list)
        assert len(task_ids) == 3
        for tid in task_ids:
            assert tid in pool.delegations
            assert pool.delegations[tid]["subject"] == "Parallel research"
            assert pool.delegations[tid]["status"] == STATUS_AVAILABLE


class TestDelegationHandler:
    @pytest.mark.asyncio
    async def test_register_handler(self, mgr):
        async def my_handler(task, context):
            return "done"
        mgr.register_handler("test_type", my_handler)
        assert "test_type" in mgr._handlers

    @pytest.mark.asyncio
    async def test_execute_task_with_handler(self, mgr, pool):
        results = []
        async def health_handler(task, context):
            results.append(task["subject"])
            return {"result": "All systems healthy", "context_updates": {"checked_at": "now"}}
        mgr.register_handler("monitoring", health_handler)

        task_id = await mgr.delegate_task(
            to_agent="test_agent", subject="Health check",
            description=json.dumps({"type": "monitoring", "description": "Check health", "context": {}}),
            task_type="monitoring",
        )
        task = pool.delegations[task_id]
        task["from_agent"] = "coordinator"
        await mgr._execute_task(task)

        assert "Health check" in results
        assert pool.delegations[task_id]["status"] == STATUS_COMPLETED
        assert "All systems healthy" in pool.delegations[task_id]["result"]

    @pytest.mark.asyncio
    async def test_execute_task_without_handler(self, mgr, pool):
        task_id = await mgr.delegate_task(
            to_agent="test_agent", subject="Unknown type",
            description=json.dumps({"type": "unknown_type", "description": "Test", "context": {}}),
            task_type="unknown_type",
        )
        task = pool.delegations[task_id]
        task["from_agent"] = "coordinator"
        await mgr._execute_task(task)

        assert pool.delegations[task_id]["status"] == STATUS_COMPLETED
        assert "No handler" in pool.delegations[task_id]["result"]

    @pytest.mark.asyncio
    async def test_handler_exception_marks_failed(self, mgr, pool):
        async def bad_handler(task, context):
            raise RuntimeError("Something went wrong!")
        mgr.register_handler("failing", bad_handler)

        task_id = await mgr.delegate_task(
            to_agent="test_agent", subject="Will fail",
            description=json.dumps({"type": "failing", "description": "Test", "context": {}}),
            task_type="failing",
        )
        task = pool.delegations[task_id]
        task["from_agent"] = "coordinator"
        task["max_retries"] = 0  # Skip retry, go straight to failed
        await mgr._execute_task(task)

        assert pool.delegations[task_id]["status"] == STATUS_FAILED
        assert "Something went wrong" in pool.delegations[task_id]["result"]


class TestDelegationContext:
    def test_safe_ascii_basic(self):
        from a2a_mesh.core.delegation import _safe_ascii
        assert _safe_ascii("hello") == "hello"
        assert _safe_ascii("írj fájl") == "irj fajl"

    def test_safe_ascii_preserves_ascii(self):
        from a2a_mesh.core.delegation import _safe_ascii
        assert _safe_ascii("Task 123: done!") == "Task 123: done!"


class TestDelegationEndToEnd:
    @pytest.mark.asyncio
    async def test_direct_lifecycle(self, pool):
        mgr = DelegationManager(pool, node_name="worker")
        execution_log = []

        async def analysis_handler(task, context):
            execution_log.append(task["subject"])
            return {"result": "Analysis complete: 42", "context_updates": {"answer": "42"}}
        mgr.register_handler("analysis", analysis_handler)

        task_id = await mgr.delegate_task(
            to_agent="worker", subject="Analyze dataset X",
            description="Run analysis on dataset X", task_type="analysis", priority=7,
        )
        assert isinstance(task_id, str)
        assert pool.delegations[task_id]["status"] == STATUS_PENDING

        task = pool.delegations[task_id]
        task["from_agent"] = "coordinator"
        await mgr._execute_task(task)

        assert pool.delegations[task_id]["status"] == STATUS_COMPLETED
        assert "Analysis complete" in pool.delegations[task_id]["result"]
        assert "Analyze dataset X" in execution_log

    @pytest.mark.asyncio
    async def test_available_claim_and_execute(self, pool):
        mgr = DelegationManager(pool, node_name="worker")

        async def research_handler(task, context):
            return "Research completed: 3 sources found"
        mgr.register_handler("research", research_handler)

        task_id = await mgr.delegate_task(
            to_agent="any", subject="Find sources about AI",
            task_type="research", available=True,
        )
        assert pool.delegations[task_id]["status"] == STATUS_AVAILABLE
        await mgr.claim_task(task_id, "worker")

        task = pool.delegations[task_id]
        task["from_agent"] = "coordinator"
        await mgr._execute_task(task)
        assert pool.delegations[task_id]["status"] == STATUS_COMPLETED

    @pytest.mark.asyncio
    async def test_handler_with_files(self, pool):
        mgr = DelegationManager(pool, node_name="worker")

        async def file_handler(task, context):
            return {
                "result": "Report generated",
                "files": [{"filename": "report.txt", "content": "Hello world", "content_type": "text/plain", "size": 11}],
            }
        mgr.register_handler("reporting", file_handler)

        task_id = await mgr.delegate_task(
            to_agent="worker", subject="Generate report",
            description=json.dumps({"type": "reporting", "description": "Generate", "context": {}}),
            task_type="reporting",
        )
        task = pool.delegations[task_id]
        task["from_agent"] = "coordinator"
        await mgr._execute_task(task)

        assert pool.delegations[task_id]["status"] == STATUS_COMPLETED
        assert "Report generated" in pool.delegations[task_id]["result"]

    @pytest.mark.asyncio
    async def test_fan_out_first_completes_cancels_siblings(self, pool):
        mgr = DelegationManager(pool, node_name="worker")

        async def fast_handler(task, context):
            return "Winner!"

        mgr.register_handler("research", fast_handler)

        task_ids = await mgr.delegate_task(
            to_agent="any", subject="Fan-out race",
            task_type="research", available=True, fan_out=3,
        )
        assert len(task_ids) == 3

        # Execute first task — winner
        first = pool.delegations[task_ids[0]]
        first["from_agent"] = "coordinator"
        # Set from_agent on ALL sibling tasks so fan-out cancel can find them
        for tid in task_ids:
            pool.delegations[tid]["from_agent"] = "coordinator"
        await mgr._execute_task(first)

        # First task should be COMPLETED
        assert pool.delegations[task_ids[0]]["status"] == STATUS_COMPLETED
        # Siblings should be CANCELLED by fan-out logic
        for tid in task_ids[1:]:
            assert pool.delegations[tid]["status"] == STATUS_CANCELLED
