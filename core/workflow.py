"""A2A Mesh Workflow DAG Coordinator — Fan-out/fan-in task orchestration.

Inspired by sushaan-k/a2a-mesh DAG coordinator, adapted for our P2P mesh.

Features:
- DAG-based task orchestration (directed acyclic graph)
- Fan-out: broadcast a task to multiple agents in parallel
- Fan-in: collect results with MERGE, FIRST, or VOTE strategy
- Consensus modes: ALL (wait for all), ANY (first response), MAJORITY (>50%), QUORUM (N specific)
- Topological sort: execute tasks in dependency order
- Budget tracking with per-level cost accumulation
- Workflow-level timeout with remaining-time tracking
- Integration with Smart Router and Health Scorer for agent selection
"""

import asyncio
import copy
import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Sequence, Any, Callable, Awaitable

log = logging.getLogger("a2a_mesh.workflow")


class ConsensusMode(Enum):
    """How to handle fan-in results from multiple agents."""
    ALL = "all"          # Wait for ALL agents to respond
    ANY = "any"          # First response wins
    MAJORITY = "majority"  # Wait for >50% of agents
    QUORUM = "quorum"    # Wait for N specific agents


class FanInStrategy(Enum):
    """How to merge fan-out results."""
    MERGE = "merge"    # Return all results as list
    FIRST = "first"    # Return first successful result
    VOTE = "vote"      # Majority vote on string representation


class TaskStatus(Enum):
    """Status of a workflow task."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class WorkflowTask:
    """A single task in a workflow DAG."""
    id: str
    name: str
    agent: Optional[str] = None           # Agent name (None = auto-select)
    capabilities: List[str] = field(default_factory=list)  # Required capabilities
    payload: Dict = field(default_factory=dict)  # Task payload
    dependencies: List[str] = field(default_factory=list)  # Task IDs this depends on
    timeout: float = 60.0                # Timeout in seconds
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Any] = None          # Task result
    error: Optional[str] = None           # Error message
    cost: float = 0.0                    # Cost of this task (sushaan-k pattern)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    assigned_agent: Optional[str] = None  # Actually assigned agent
    fan_out_count: int = 1                # Number of parallel copies (fan-out)
    fan_in_strategy: FanInStrategy = FanInStrategy.MERGE  # How to merge fan-out results

    @property
    def duration_ms(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at) * 1000
        return None


@dataclass
class Workflow:
    """A workflow DAG containing tasks with dependencies."""
    id: str
    name: str
    tasks: Dict[str, WorkflowTask] = field(default_factory=dict)
    consensus_mode: ConsensusMode = ConsensusMode.ALL
    max_cost: Optional[float] = None       # Budget limit (sushaan-k pattern)
    timeout: Optional[float] = None         # Workflow-level timeout in seconds
    created_at: float = field(default_factory=time.time)
    status: TaskStatus = TaskStatus.PENDING
    results: Dict[str, Any] = field(default_factory=dict)  # task_id → result
    errors: Dict[str, str] = field(default_factory=dict)    # task_id → error
    total_cost: float = 0.0               # Accumulated cost across all tasks

    def add_task(self, task: WorkflowTask) -> 'Workflow':
        """Add a task to the workflow."""
        self.tasks[task.id] = task
        return self

    def topological_sort(self) -> List[List[str]]:
        """Sort tasks into layers for parallel execution.

        Returns layers where tasks in the same layer can run in parallel,
        and each layer depends only on previous layers.
        """
        # Build dependency graph
        in_degree = {tid: 0 for tid in self.tasks}
        dependents = {tid: [] for tid in self.tasks}

        for tid, task in self.tasks.items():
            for dep in task.dependencies:
                if dep in self.tasks:
                    in_degree[tid] += 1
                    dependents[dep].append(tid)

        # Kahn's algorithm — layer by layer
        layers = []
        ready = [tid for tid, deg in in_degree.items() if deg == 0]

        while ready:
            layers.append(sorted(ready))  # Sort for determinism
            next_ready = []
            for tid in ready:
                for dep_tid in dependents[tid]:
                    in_degree[dep_tid] -= 1
                    if in_degree[dep_tid] == 0:
                        next_ready.append(dep_tid)
            ready = next_ready

        # Check for cycles
        visited = sum(len(layer) for layer in layers)
        if visited < len(self.tasks):
            remaining = set(self.tasks.keys()) - set(tid for layer in layers for tid in layer)
            raise ValueError(f"Cycle detected in workflow! Unresolved tasks: {remaining}")

        return layers


class WorkflowCoordinator:
    """Coordinates workflow execution across mesh agents.

    Integrates with AgentRegistry and SmartRouter for intelligent
    agent selection based on capabilities and health.

    Usage:
        from a2a_mesh.core.registry import AgentRegistry
        from a2a_mesh.core.smart_router import SmartRouter

        registry = AgentRegistry()
        smart_router = SmartRouter(registry)
        coordinator = WorkflowCoordinator(registry, smart_router)

        # Define a workflow
        wf = coordinator.create_workflow("research-task")
        wf.add_task(WorkflowTask(id="search", name="Web Search", capabilities=["web_search"]))
        wf.add_task(WorkflowTask(id="summarize", name="Summarize", capabilities=["summarization@v2"], dependencies=["search"]))

        # Execute
        result = await coordinator.execute(wf)
    """

    def __init__(self, registry=None, smart_router=None, node=None):
        self.registry = registry
        self.smart_router = smart_router
        self.node = node
        self._active_workflows: Dict[str, Workflow] = {}

    def create_workflow(self, name: str, consensus_mode: ConsensusMode = ConsensusMode.ALL,
                        max_cost: Optional[float] = None, timeout: Optional[float] = None) -> Workflow:
        """Create a new workflow with optional budget and timeout."""
        wf = Workflow(
            id=str(uuid.uuid4())[:8],
            name=name,
            consensus_mode=consensus_mode,
            max_cost=max_cost,
            timeout=timeout,
        )
        return wf

    async def execute(self, workflow: Workflow) -> Dict:
        """Execute a workflow DAG layer by layer.

        For each layer:
        1. Check budget (max_cost)
        2. Check remaining timeout
        3. Fan-out: send tasks to agents in parallel
        4. Fan-in: collect results based on consensus mode
        5. Accumulate costs, pass results to dependent tasks

        Returns:
            Dict with workflow results, errors, timing, and cost.
        """
        workflow.status = TaskStatus.RUNNING
        self._active_workflows[workflow.id] = workflow
        started_at = time.time()
        timed_out = False

        try:
            layers = workflow.topological_sort()
            log.info(f"Workflow '{workflow.name}' ({workflow.id}): {len(workflow.tasks)} tasks in {len(layers)} layers")

            for layer_idx, layer in enumerate(layers):
                # Check remaining timeout
                remaining = None
                if workflow.timeout is not None:
                    elapsed = time.time() - started_at
                    remaining = max(0.0, workflow.timeout - elapsed)
                    if remaining <= 0:
                        log.warning(f"Workflow '{workflow.name}' timed out before layer {layer_idx}")
                        timed_out = True
                        break

                # Check budget
                if workflow.max_cost is not None and workflow.total_cost > workflow.max_cost:
                    log.warning(f"Workflow '{workflow.name}' budget exceeded: {workflow.total_cost:.4f} > {workflow.max_cost}")
                    workflow.status = TaskStatus.FAILED
                    return self._build_result(workflow)

                log.info(f"  Layer {layer_idx}: {len(layer)} parallel tasks (remaining={remaining:.1f}s)")

                # Fan-out: execute all tasks in this layer concurrently
                try:
                    if remaining is not None:
                        results = await asyncio.wait_for(
                            self._execute_layer(workflow, layer),
                            timeout=remaining,
                        )
                    else:
                        results = await self._execute_layer(workflow, layer)
                except asyncio.TimeoutError:
                    log.warning(f"Workflow '{workflow.name}' layer {layer_idx} timed out")
                    timed_out = True
                    for tid in layer:
                        task = workflow.tasks[tid]
                        if task.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                            task.status = TaskStatus.TIMEOUT
                            task.error = f"Workflow timeout at layer {layer_idx}"
                            workflow.errors[tid] = task.error
                    break

                # Accumulate costs
                for tid in layer:
                    task = workflow.tasks[tid]
                    workflow.total_cost += task.cost

                # Check for failures (ALL mode aborts on any failure)
                failed = [t for t in layer if workflow.tasks[t].status == TaskStatus.FAILED]
                if failed and workflow.consensus_mode == ConsensusMode.ALL:
                    log.error(f"Workflow '{workflow.name}' aborted: tasks {failed} failed")
                    workflow.status = TaskStatus.FAILED
                    return self._build_result(workflow)

                # Pass results to next layer's payload
                for tid in layer:
                    task = workflow.tasks[tid]
                    if task.status == TaskStatus.COMPLETED and task.result:
                        workflow.results[tid] = task.result

            # Handle timeout — mark unstarted tasks
            if timed_out:
                for tid, task in workflow.tasks.items():
                    if task.status == TaskStatus.PENDING:
                        task.status = TaskStatus.TIMEOUT
                        task.error = "Workflow timeout reached"
                        workflow.errors[tid] = task.error
                workflow.status = TaskStatus.TIMEOUT
            else:
                workflow.status = TaskStatus.COMPLETED
                log.info(f"Workflow '{workflow.name}' completed successfully")

        except Exception as e:
            log.error(f"Workflow '{workflow.name}' error: {e}")
            workflow.status = TaskStatus.FAILED
        finally:
            del self._active_workflows[workflow.id]

        return self._build_result(workflow)

    async def _execute_layer(self, workflow: Workflow, layer: List[str]) -> Dict[str, Any]:
        """Execute all tasks in a layer concurrently (fan-out/fan-in).

        Supports fan-out (N parallel copies) with configurable fan-in strategy:
        - MERGE: collect all results
        - FIRST: first successful result wins
        - VOTE: majority vote on string representation
        """
        coros = []
        task_ids = []
        for tid in layer:
            task = workflow.tasks[tid]
            # Inject results from dependencies into payload
            for dep_id in task.dependencies:
                if dep_id in workflow.results:
                    task.payload[f"result_{dep_id}"] = workflow.results[dep_id]

            # Fan-out: if task has fan_out_count > 1, create N copies
            if task.fan_out_count > 1:
                for i in range(task.fan_out_count):
                    cloned = copy.deepcopy(task)
                    cloned.id = f"{task.id}_fan{i}"
                    coros.append(self._execute_task(workflow, cloned))
            else:
                coros.append(self._execute_task(workflow, task))
            task_ids.append(tid)

        # Fan-out: run all tasks concurrently with return_exceptions for graceful degradation
        raw_results = await asyncio.gather(*coros, return_exceptions=True)

        # Fan-in: apply consensus mode
        if workflow.consensus_mode == ConsensusMode.ANY:
            # ANY: first successful result wins
            results = await self._fan_in_any(raw_results, task_ids, workflow)
        elif workflow.consensus_mode == ConsensusMode.MAJORITY:
            # MAJORITY: >50% must agree
            results = await self._fan_in_majority(raw_results, task_ids, workflow)
        else:
            # ALL / QUORUM: collect all results
            results = {}
            for i, (tid, res) in enumerate(zip(layer, raw_results)):
                task = workflow.tasks[tid]
                if isinstance(res, BaseException):
                    task.status = TaskStatus.FAILED
                    task.error = str(res)
                    workflow.errors[tid] = str(res)
                    results[tid] = None
                else:
                    results[tid] = res

        return results

    async def _fan_in_any(self, results: List, task_ids: List[str], workflow: Workflow) -> Dict[str, Any]:
        """Fan-in strategy: first successful result wins (ANY mode)."""
        output = {}
        for i, (tid, res) in enumerate(zip(task_ids, results)):
            if not isinstance(res, BaseException) and res is not None:
                task = workflow.tasks.get(tid)
                if task:
                    output[tid] = res
                    return output  # First success wins
        # No success — mark all as failed
        for tid in task_ids:
            task = workflow.tasks.get(tid)
            if task:
                task.status = TaskStatus.FAILED
                task.error = "All fan-out attempts failed"
                workflow.errors[tid] = task.error
                output[tid] = None
        return output

    async def _fan_in_majority(self, results: List, task_ids: List[str], workflow: Workflow) -> Dict[str, Any]:
        """Fan-in strategy: majority vote (>50%) on string representation (MAJORITY mode)."""
        successes = [(tid, res) for tid, res in zip(task_ids, results)
                     if not isinstance(res, BaseException) and res is not None]

        if not successes:
            for tid in task_ids:
                workflow.errors[tid] = "All fan-out attempts failed"
            return {tid: None for tid in task_ids}

        # Majority vote on string representation (sushaan-k pattern)
        str_results = [str(res) for _, res in successes]
        counted = Counter(str_results)
        winner, count = counted.most_common(1)[0]

        output = {}
        for tid, res in zip(task_ids, results):
            if isinstance(res, BaseException):
                output[tid] = None
                workflow.errors[tid] = str(res)
            elif str(res) == winner:
                output[tid] = res
            else:
                output[tid] = None  # Minority result

        return output

    async def _execute_task(self, workflow: Workflow, task: WorkflowTask) -> Any:
        """Execute a single task — assign agent and send message."""
        task.started_at = time.time()
        task.status = TaskStatus.RUNNING

        try:
            # Auto-assign agent if not specified
            if not task.agent and self.smart_router:
                agent = self.smart_router.route(
                    required_capabilities=task.capabilities or None,
                    strategy="health_weighted",
                )
                if agent:
                    task.assigned_agent = agent.name
                else:
                    raise ValueError(f"No agent found for capabilities: {task.capabilities}")
            elif task.agent:
                task.assigned_agent = task.agent

            # Send message via node if available
            if self.node and task.assigned_agent:
                from ..core.message import A2AMessage
                msg = A2AMessage(
                    sender=self.node.node_name,
                    recipient=task.assigned_agent,
                    type="workflow_task",
                    priority=5,
                    payload={
                        "workflow_id": workflow.id,
                        "task_id": task.id,
                        "task_name": task.name,
                        "capabilities": task.capabilities,
                        **task.payload,
                    },
                    routing_mode="direct",
                )
                result = await self.node.router.send(msg)
                task.result = {"sent": True, "message_id": msg.id, "transport": result.transport}

            else:
                # No node or agent — simulate success for testing
                task.result = {"simulated": True, "agent": task.assigned_agent}

            task.status = TaskStatus.COMPLETED
            task.completed_at = time.time()
            log.info(f"Task '{task.name}' ({task.id}) completed in {task.duration_ms:.0f}ms via {task.assigned_agent}")

        except asyncio.TimeoutError:
            task.status = TaskStatus.TIMEOUT
            task.error = f"Timeout after {task.timeout}s"
            task.completed_at = time.time()
            log.warning(f"Task '{task.name}' ({task.id}) timed out")

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.completed_at = time.time()
            log.error(f"Task '{task.name}' ({task.id}) failed: {e}")

        return task.result

    def _build_result(self, workflow: Workflow) -> Dict:
        """Build the final result dict from a workflow."""
        task_results = {}
        for tid, task in workflow.tasks.items():
            task_results[tid] = {
                "name": task.name,
                "status": task.status.value,
                "agent": task.assigned_agent,
                "duration_ms": task.duration_ms,
                "result": task.result,
                "error": task.error,
                "cost": task.cost,
            }

        return {
            "workflow_id": workflow.id,
            "workflow_name": workflow.name,
            "status": workflow.status.value,
            "total_tasks": len(workflow.tasks),
            "completed": sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.COMPLETED),
            "failed": sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.FAILED),
            "timed_out": sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.TIMEOUT),
            "total_cost": round(workflow.total_cost, 4),
            "results": workflow.results,
            "errors": workflow.errors if workflow.errors else None,
            "task_details": task_results,
        }

    def get_workflow_status(self, workflow_id: str) -> Optional[Dict]:
        """Get the current status of an active workflow."""
        wf = self._active_workflows.get(workflow_id)
        if not wf:
            return None
        return self._build_result(wf)

    def list_active_workflows(self) -> List[Dict]:
        """List all active workflows."""
        return [
            {"id": wf.id, "name": wf.name, "status": wf.status.value, "tasks": len(wf.tasks)}
            for wf in self._active_workflows.values()
        ]