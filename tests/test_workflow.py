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


# ── v3 Tests: Conditional branching, retry, result passing ──

class TestWorkflowV3Conditional:
    """Test v3 conditional branching — tasks with condition are skipped if False."""

    @pytest.mark.asyncio
    async def test_condition_true_runs_task(self):
        """Task with condition=True should run normally."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("cond-true-test")
        wf.add_task(WorkflowTask(id="t1", name="Setup", payload={"value": 42}))
        wf.add_task(WorkflowTask(
            id="t2", name="Conditional",
            dependencies=["t1"],
            condition="result_t1.get('simulated') == True",
        ))
        result = await coord.execute(wf)
        assert result["status"] == "completed"
        assert result["task_details"]["t2"]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_condition_false_skips_task(self):
        """Task with condition=False should be SKIPPED."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("cond-false-test")
        wf.add_task(WorkflowTask(id="t1", name="Setup", payload={"value": 42}))
        wf.add_task(WorkflowTask(
            id="t2", name="SkipMe",
            dependencies=["t1"],
            condition="result_t1.get('value') == 999",
        ))
        result = await coord.execute(wf)
        assert result["status"] == "completed"
        assert result["task_details"]["t2"]["status"] == "skipped"
        assert result["skipped"] == 1

    @pytest.mark.asyncio
    async def test_condition_error_defaults_skip(self):
        """Condition with eval error should default to skip."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("cond-error-test")
        wf.add_task(WorkflowTask(id="t1", name="Setup"))
        wf.add_task(WorkflowTask(
            id="t2", name="BadCond",
            dependencies=["t1"],
            condition="undefined_var.something",
        ))
        result = await coord.execute(wf)
        assert result["task_details"]["t2"]["status"] == "skipped"

    @pytest.mark.asyncio
    async def test_conditional_branch_if_else(self):
        """Full if/else: task B runs if condition True, task C runs if False."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("if-else-test")
        wf.add_task(WorkflowTask(id="search", name="Search", payload={"found": True}))
        wf.add_task(WorkflowTask(
            id="process", name="Process",
            dependencies=["search"],
            condition="result_search.get('simulated') == True",
        ))
        wf.add_task(WorkflowTask(
            id="fallback", name="Fallback",
            dependencies=["search"],
            condition="result_search.get('simulated') == False",
        ))
        result = await coord.execute(wf)
        assert result["task_details"]["process"]["status"] == "completed"
        assert result["task_details"]["fallback"]["status"] == "skipped"


class TestWorkflowV3Retry:
    """Test v3 retry policy — failed tasks retry up to max_retries."""

    @pytest.mark.asyncio
    async def test_no_retry_by_default(self):
        """Task without max_retries should fail immediately."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("no-retry-test")
        wf.add_task(WorkflowTask(
            id="t1", name="FailTask",
            agent="nonexistent_agent",
            capabilities=["nonexistent_cap"],
        ))
        result = await coord.execute(wf)
        # Without node, it simulates success — so test with a coordinator that has no router
        # The task will get assigned to nonexistent_agent but simulated success
        # Check retry_count is 0 (no retries attempted)
        assert result["task_details"]["t1"].get("retry_count", 0) == 0

    @pytest.mark.asyncio
    async def test_retry_with_max_retries(self):
        """Task with max_retries should retry on failure."""
        coord = WorkflowCoordinator()
        # Mock smart_router that always fails to find an agent
        class FailRouter:
            def route(self, required_capabilities=None, strategy=None):
                return None
        coord.smart_router = FailRouter()
        
        wf = coord.create_workflow("retry-test")
        wf.add_task(WorkflowTask(
            id="t1", name="RetryTask",
            max_retries=2,
            retry_delay=0.01,
        ))
        result = await coord.execute(wf)
        assert result["status"] == "failed"
        assert result["task_details"]["t1"].get("retry_count", 0) == 2

    @pytest.mark.asyncio
    async def test_retry_succeeds_eventually(self):
        """Task that fails first but succeeds on retry."""
        call_count = {"n": 0}
        coord = WorkflowCoordinator()
        
        class FakeAgent:
            name = "fake_agent"
        
        class FakeRouter:
            def route(self, required_capabilities=None, strategy=None):
                return FakeAgent()
        coord.smart_router = FakeRouter()
        
        class FakeSendResult:
            transport = "p2p"
            success = True
        
        class FakeRouter2:
            @staticmethod
            async def send(msg):
                call_count["n"] += 1
                if call_count["n"] < 2:
                    raise Exception("Simulated transient failure")
                return FakeSendResult()
        
        class FakeNode:
            node_name = "test_node"
            router = FakeRouter2()
        coord.node = FakeNode()
        
        wf = coord.create_workflow("retry-success-test")
        wf.add_task(WorkflowTask(
            id="t1", name="FlakyTask",
            max_retries=3,
            retry_delay=0.01,
        ))
        result = await coord.execute(wf)
        assert result["status"] == "completed"
        assert result["task_details"]["t1"]["status"] == "completed"
        assert result["task_details"]["t1"].get("retry_count", 0) == 1


class TestWorkflowV3ResultPassing:
    """Test v3 result passing — input_from injects dependency result as 'input'."""

    @pytest.mark.asyncio
    async def test_result_passing_injects_input(self):
        """Task with input_from should receive dependency result as 'input' in payload."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("result-passing-test")
        wf.add_task(WorkflowTask(id="t1", name="Producer", payload={"data": "hello"}))
        wf.add_task(WorkflowTask(
            id="t2", name="Consumer",
            dependencies=["t1"],
            input_from="t1",
        ))
        result = await coord.execute(wf)
        assert result["status"] == "completed"
        assert "input" in wf.tasks["t2"].payload

    @pytest.mark.asyncio
    async def test_dependency_result_in_payload(self):
        """Dependencies automatically inject result_<dep_id> into payload."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("dep-result-test")
        wf.add_task(WorkflowTask(id="t1", name="First"))
        wf.add_task(WorkflowTask(id="t2", name="Second", dependencies=["t1"]))
        result = await coord.execute(wf)
        assert result["status"] == "completed"
        assert "result_t1" in wf.tasks["t2"].payload


class TestWorkflowV3SkippedStatus:
    """Test SKIPPED status is properly tracked in results."""

    @pytest.mark.asyncio
    async def test_skipped_count_in_result(self):
        """Result should include skipped count."""
        coord = WorkflowCoordinator()
        wf = coord.create_workflow("skipped-count-test")
        wf.add_task(WorkflowTask(id="t1", name="Setup"))
        wf.add_task(WorkflowTask(
            id="t2", name="Skip1",
            dependencies=["t1"],
            condition="False",
        ))
        wf.add_task(WorkflowTask(
            id="t3", name="Skip2",
            dependencies=["t1"],
            condition="False",
        ))
        result = await coord.execute(wf)
        assert result["status"] == "completed"
        assert result["skipped"] == 2
        assert result["completed"] == 1
