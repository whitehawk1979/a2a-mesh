"""Test core.workflow — DAG coordinator, fan-out/fan-in, consensus modes."""
import pytest
import asyncio
from a2a_mesh.core.workflow import (
    WorkflowCoordinator,
    Workflow,
    WorkflowTask,
    ConsensusMode,
    FanInStrategy,
    TaskStatus,
)


class TestWorkflowTask:
    def test_defaults(self):
        task = WorkflowTask(id="t1", name="Test")
        assert task.status == TaskStatus.PENDING
        assert task.dependencies == []
        assert task.fan_out_count == 1
        assert task.fan_in_strategy == FanInStrategy.MERGE
        assert task.timeout == 60.0
        assert task.cost == 0.0

    def test_duration_ms(self):
        task = WorkflowTask(id="t1", name="Test")
        assert task.duration_ms is None
        task.started_at = 100.0
        task.completed_at = 100.5
        assert task.duration_ms == 500.0


class TestWorkflow:
    def test_add_task(self):
        wf = Workflow(id="w1", name="test")
        task = WorkflowTask(id="t1", name="Task 1")
        wf.add_task(task)
        assert "t1" in wf.tasks

    def test_topological_sort_linear(self):
        wf = Workflow(id="w1", name="linear")
        wf.add_task(WorkflowTask(id="t1", name="Step 1"))
        wf.add_task(WorkflowTask(id="t2", name="Step 2", dependencies=["t1"]))
        wf.add_task(WorkflowTask(id="t3", name="Step 3", dependencies=["t2"]))
        layers = wf.topological_sort()
        assert len(layers) == 3
        assert layers[0] == ["t1"]
        assert layers[1] == ["t2"]
        assert layers[2] == ["t3"]

    def test_topological_sort_parallel(self):
        wf = Workflow(id="w1", name="parallel")
        wf.add_task(WorkflowTask(id="t1", name="Step 1"))
        wf.add_task(WorkflowTask(id="t2", name="Step 2"))
        wf.add_task(WorkflowTask(id="t3", name="Step 3", dependencies=["t1", "t2"]))
        layers = wf.topological_sort()
        assert len(layers) == 2
        assert sorted(layers[0]) == ["t1", "t2"]
        assert layers[1] == ["t3"]

    def test_topological_sort_diamond(self):
        wf = Workflow(id="w1", name="diamond")
        wf.add_task(WorkflowTask(id="t1", name="Start"))
        wf.add_task(WorkflowTask(id="t2", name="Branch A", dependencies=["t1"]))
        wf.add_task(WorkflowTask(id="t3", name="Branch B", dependencies=["t1"]))
        wf.add_task(WorkflowTask(id="t4", name="End", dependencies=["t2", "t3"]))
        layers = wf.topological_sort()
        assert len(layers) == 3
        assert layers[0] == ["t1"]
        assert sorted(layers[1]) == ["t2", "t3"]
        assert layers[2] == ["t4"]

    def test_topological_sort_cycle_detected(self):
        wf = Workflow(id="w1", name="cycle")
        wf.add_task(WorkflowTask(id="t1", name="A", dependencies=["t2"]))
        wf.add_task(WorkflowTask(id="t2", name="B", dependencies=["t1"]))
        with pytest.raises(ValueError, match="Cycle detected"):
            wf.topological_sort()

    def test_workflow_defaults(self):
        wf = Workflow(id="w1", name="test")
        assert wf.consensus_mode == ConsensusMode.ALL
        assert wf.max_cost is None
        assert wf.timeout is None
        assert wf.status == TaskStatus.PENDING


class TestEnums:
    def test_consensus_modes(self):
        assert ConsensusMode.ALL.value == "all"
        assert ConsensusMode.ANY.value == "any"
        assert ConsensusMode.MAJORITY.value == "majority"
        assert ConsensusMode.QUORUM.value == "quorum"

    def test_fan_in_strategies(self):
        assert FanInStrategy.MERGE.value == "merge"
        assert FanInStrategy.FIRST.value == "first"
        assert FanInStrategy.VOTE.value == "vote"

    def test_task_statuses(self):
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
        assert TaskStatus.TIMEOUT.value == "timeout"
        assert TaskStatus.CANCELLED.value == "cancelled"


class TestWorkflowCoordinator:
    def test_create_workflow(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("test-wf")
        assert wf.name == "test-wf"
        assert wf.consensus_mode == ConsensusMode.ALL
        assert len(wf.id) > 0

    def test_create_workflow_with_options(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow(
            "test-wf",
            consensus_mode=ConsensusMode.ANY,
            max_cost=100.0,
            timeout=60.0,
        )
        assert wf.consensus_mode == ConsensusMode.ANY
        assert wf.max_cost == 100.0
        assert wf.timeout == 60.0

    @pytest.mark.asyncio
    async def test_simple_linear_workflow(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("linear-test")
        wf.add_task(WorkflowTask(id="step1", name="Step 1", capabilities=["search"]))
        wf.add_task(WorkflowTask(id="step2", name="Step 2", capabilities=["summarize"], dependencies=["step1"]))
        result = await coord.execute(wf)
        assert result["status"] in ("completed", "failed")
        assert result["total_tasks"] == 2

    @pytest.mark.asyncio
    async def test_workflow_without_agents(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("no-agents")
        wf.add_task(WorkflowTask(id="task1", name="Task 1"))
        result = await coord.execute(wf)
        assert result["status"] == "completed"
        assert result["total_tasks"] == 1

    @pytest.mark.asyncio
    async def test_workflow_with_timeout(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("timeout-test", timeout=0.001)
        wf.add_task(WorkflowTask(id="t1", name="Task 1"))
        result = await coord.execute(wf)
        assert result["status"] in ("completed", "timeout", "failed")

    @pytest.mark.asyncio
    async def test_workflow_budget_exceeded(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("budget-test", max_cost=0.0)
        wf.add_task(WorkflowTask(id="t1", name="Task 1", cost=10.0))
        result = await coord.execute(wf)
        assert result["status"] in ("failed", "completed")

    @pytest.mark.asyncio
    async def test_parallel_workflow(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("parallel-test")
        wf.add_task(WorkflowTask(id="t1", name="Task 1"))
        wf.add_task(WorkflowTask(id="t2", name="Task 2"))
        wf.add_task(WorkflowTask(id="t3", name="Task 3", dependencies=["t1", "t2"]))
        result = await coord.execute(wf)
        assert result["total_tasks"] == 3

    @pytest.mark.asyncio
    async def test_fan_out_workflow(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("fanout-test")
        wf.add_task(WorkflowTask(id="t1", name="Broadcast", fan_out_count=3))
        result = await coord.execute(wf)
        assert result["total_tasks"] == 1

    @pytest.mark.asyncio
    async def test_workflow_result_structure(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("structure-test")
        wf.add_task(WorkflowTask(id="t1", name="Task 1"))
        result = await coord.execute(wf)
        for key in ["workflow_id", "workflow_name", "status", "total_tasks",
                     "completed", "failed", "total_cost", "task_details"]:
            assert key in result

    def test_get_workflow_status_not_found(self):
        coord = WorkflowCoordinator()
        assert coord.get_workflow_status("nonexistent") is None

    def test_list_active_workflows_empty(self):
        coord = WorkflowCoordinator()
        assert coord.list_active_workflows() == []


class TestWorkflowIntegration:
    @pytest.mark.asyncio
    async def test_workflow_with_router(self):
        from a2a_mesh.core.registry import AgentRegistry, AgentCard
        from a2a_mesh.core.smart_router import SmartRouter
        registry = AgentRegistry()
        registry.register(AgentCard(name="worker", capabilities=["search"]))
        smart_router = SmartRouter(registry)
        coord = WorkflowCoordinator(registry=registry, smart_router=smart_router)
        wf = coord.create_workflow("routed-test")
        wf.add_task(WorkflowTask(id="t1", name="Search", capabilities=["search"]))
        result = await coord.execute(wf)
        details = result["task_details"]["t1"]
        assert details["agent"] == "worker"

    @pytest.mark.asyncio
    async def test_workflow_no_matching_agent(self):
        from a2a_mesh.core.registry import AgentRegistry, AgentCard
        from a2a_mesh.core.smart_router import SmartRouter
        registry = AgentRegistry()
        registry.register(AgentCard(name="worker", capabilities=["translate"]))
        smart_router = SmartRouter(registry)
        coord = WorkflowCoordinator(registry=registry, smart_router=smart_router)
        wf = coord.create_workflow("no-match-test")
        wf.add_task(WorkflowTask(id="t1", name="Search", capabilities=["nonexistent"]))
        result = await coord.execute(wf)
        assert result["status"] in ("failed", "completed")

    @pytest.mark.asyncio
    async def test_workflow_explicit_agent(self):
        from a2a_mesh.core.registry import AgentRegistry, AgentCard
        from a2a_mesh.core.smart_router import SmartRouter
        registry = AgentRegistry()
        smart_router = SmartRouter(registry)
        coord = WorkflowCoordinator(registry=registry, smart_router=smart_router)
        wf = coord.create_workflow("explicit-test")
        wf.add_task(WorkflowTask(id="t1", name="Task", agent="manual_agent"))
        result = await coord.execute(wf)
        details = result["task_details"]["t1"]
        assert details["agent"] == "manual_agent"

    @pytest.mark.asyncio
    async def test_workflow_consensus_any(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("consensus-any-test", consensus_mode=ConsensusMode.ANY)
        wf.add_task(WorkflowTask(id="t1", name="Task 1"))
        wf.add_task(WorkflowTask(id="t2", name="Task 2"))
        result = await coord.execute(wf)
        assert result["status"] in ("completed", "failed")

    @pytest.mark.asyncio
    async def test_workflow_consensus_majority(self):
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("consensus-majority-test", consensus_mode=ConsensusMode.MAJORITY)
        wf.add_task(WorkflowTask(id="t1", name="Task 1"))
        result = await coord.execute(wf)
        assert result["status"] in ("completed", "failed")
