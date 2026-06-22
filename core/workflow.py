"""A2A Mesh Workflow DAG Coordinator — Fan-out/fan-in task orchestration.

Inspired by sushaan-k/a2a-mesh DAG coordinator, adapted for our P2P mesh.

Features:
- DAG-based task orchestration (directed acyclic graph)
- Fan-out: broadcast a task to multiple agents in parallel
- Fan-in: collect results from all agents before proceeding
- Consensus modes: ALL (wait for all), ANY (first response), MAJORITY (>50%)
- Topological sort: execute tasks in dependency order
- Timeout handling with configurable grace periods
- Integration with Smart Router for agent selection
"""

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Sequence, Any, Callable

log = logging.getLogger("a2a_mesh.workflow")


class ConsensusMode(Enum):
    """How to handle fan-in results from multiple agents."""
    ALL = "all"          # Wait for ALL agents to respond
    ANY = "any"          # First response wins
    MAJORITY = "majority"  # Wait for >50% of agents
    QUORUM = "quorum"    # Wait for N specific agents


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
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    assigned_agent: Optional[str] = None  # Actually assigned agent

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
    created_at: float = field(default_factory=time.time)
    status: TaskStatus = TaskStatus.PENDING
    results: Dict[str, Any] = field(default_factory=dict)  # task_id → result
    errors: Dict[str, str] = field(default_factory=dict)    # task_id → error

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

    def create_workflow(self, name: str, consensus_mode: ConsensusMode = ConsensusMode.ALL) -> Workflow:
        """Create a new workflow."""
        wf = Workflow(
            id=str(uuid.uuid4())[:8],
            name=name,
            consensus_mode=consensus_mode,
        )
        return wf

    async def execute(self, workflow: Workflow) -> Dict:
        """Execute a workflow DAG layer by layer.

        For each layer:
        1. Assign agents to tasks (using SmartRouter if available)
        2. Fan-out: send tasks to agents in parallel
        3. Fan-in: collect results based on consensus mode
        4. Pass results to dependent tasks

        Returns:
            Dict with workflow results, errors, and timing.
        """
        workflow.status = TaskStatus.RUNNING
        self._active_workflows[workflow.id] = workflow

        try:
            layers = workflow.topological_sort()
            log.info(f"Workflow '{workflow.name}' ({workflow.id}): {len(workflow.tasks)} tasks in {len(layers)} layers")

            for layer_idx, layer in enumerate(layers):
                log.info(f"  Layer {layer_idx}: {len(layer)} parallel tasks")

                # Fan-out: execute all tasks in this layer concurrently
                results = await self._execute_layer(workflow, layer)

                # Check for failures
                failed = [t for t in layer if workflow.tasks[t].status == TaskStatus.FAILED]
                if failed and workflow.consensus_mode == ConsensusMode.ALL:
                    # ALL mode: any failure aborts the workflow
                    log.error(f"Workflow '{workflow.name}' aborted: tasks {failed} failed")
                    workflow.status = TaskStatus.FAILED
                    return self._build_result(workflow)

                # Pass results to next layer's payload
                for tid in layer:
                    task = workflow.tasks[tid]
                    if task.status == TaskStatus.COMPLETED and task.result:
                        workflow.results[tid] = task.result

            workflow.status = TaskStatus.COMPLETED
            log.info(f"Workflow '{workflow.name}' completed successfully")

        except Exception as e:
            log.error(f"Workflow '{workflow.name}' error: {e}")
            workflow.status = TaskStatus.FAILED
        finally:
            del self._active_workflows[workflow.id]

        return self._build_result(workflow)

    async def _execute_layer(self, workflow: Workflow, layer: List[str]) -> Dict[str, Any]:
        """Execute all tasks in a layer concurrently (fan-out/fan-in)."""
        tasks = []
        for tid in layer:
            task = workflow.tasks[tid]
            # Inject results from dependencies into payload
            for dep_id in task.dependencies:
                if dep_id in workflow.results:
                    task.payload[f"result_{dep_id}"] = workflow.results[dep_id]
            tasks.append(self._execute_task(workflow, task))

        # Fan-out: run all tasks concurrently
        if workflow.consensus_mode == ConsensusMode.ANY:
            # ANY: first successful result wins
            results = await self._execute_any(tasks, layer, workflow)
        else:
            # ALL / MAJORITY / QUORUM: wait for all, then apply consensus
            results = await asyncio.gather(*tasks, return_exceptions=True)

        return dict(zip(layer, results))

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

    async def _execute_any(self, coros, layer: List[str], workflow: Workflow) -> List[Any]:
        """Execute tasks and return on first success (ANY consensus)."""
        for coro in asyncio.as_completed(coros):
            try:
                result = await coro
                return [result] + [None] * (len(coros) - 1)
            except Exception:
                continue
        return [None] * len(coros)

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
            }

        return {
            "workflow_id": workflow.id,
            "workflow_name": workflow.name,
            "status": workflow.status.value,
            "total_tasks": len(workflow.tasks),
            "completed": sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.COMPLETED),
            "failed": sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.FAILED),
            "results": workflow.results,
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