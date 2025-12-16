import pytest

from dag_engine.core.manager import WorkflowManager
from dag_engine.core.constants import NodeStatus
from dag_engine.core.orchestrator import EventDrivenDagOrchestrator
from dag_engine.core.schemas import WorkflowDefinition


def make_definition():
    """
    Minimal single-node workflow.
    """
    return WorkflowDefinition.model_validate(
        {
            "name": "wf",
            "dag": {
                "nodes": [
                    {
                        "id": "A",
                        "handler": "noop",
                        "dependencies": [],
                        "config": {}
                    }
                ]
            }
        }
    )


@pytest.mark.asyncio
async def test_start_workflow_registers_and_writes_metadata(exec_store, idemp, rstore, event_bus, event_store, counter):
    """
    WorkflowManager.start_workflow:
      - creates DAG
      - registers WorkflowInfo in memory
      - writes initial metadata to ExecutionStore
      - starts orchestrator
    """
    manager = WorkflowManager(
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
        event_store=event_store,
        event_bus=event_bus,
        atomic_counter=counter,
    )

    # Patch orchestrator.start() so it doesn’t actually try to run tasks
    async def fake_start():
        pass

    workflow_id = "wf-1"
    definition = make_definition()

    # monkeypatch the orchestrator start method for this test
    original_ctor = EventDrivenDagOrchestrator.__init__

    def patched_ctor(self, *args, **kwargs):
        original_ctor(self, *args, **kwargs)
        self.start = fake_start

    EventDrivenDagOrchestrator.__init__ = patched_ctor

    await manager.start_workflow(workflow_id, definition)

    # Restore orchestrator ctor after test
    EventDrivenDagOrchestrator.__init__ = original_ctor

    # Execution store should have metadata
    meta = exec_store.meta[workflow_id]
    assert meta["workflow_id"] == workflow_id
    assert meta["status"] == "RUNNING"
    assert meta["completed_at"] is None


@pytest.mark.asyncio
async def test_get_status_falls_back_to_persisted_metadata(exec_store, idemp, rstore, event_bus, event_store, counter):
    """
    get_status should use persisted status when workflow
    is no longer present in memory.
    """
    manager = WorkflowManager(
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
        event_store=event_store,
        event_bus=event_bus,
        atomic_counter=counter,
    )

    wf_id = "wfX"

    # Persist something
    await manager.execution_store.save_metadata(
        wf_id,
        {"workflow_id": wf_id, "status": "COMPLETED", "created_at": 123, "completed_at": 456},
    )

    status = await manager.get_status(wf_id)
    assert status["state"] == "COMPLETED"
    assert status["completed_at"] == 456


@pytest.mark.asyncio
async def test_get_results_returns_persisted_results(exec_store, idemp, rstore, event_bus, event_store, counter):
    """
    get_results must always return persisted results.
    """
    manager = WorkflowManager(
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
        event_store=event_store,
        event_bus=event_bus,
        atomic_counter=counter,
    )

    wf_id = "wfY"

    await manager.execution_store.save_metadata(
        wf_id,
        {"workflow_id": wf_id, "status": "COMPLETED", "created_at": 1, "completed_at": 2},
    )
    await manager.execution_store.save_results(wf_id, {"A": {"status": "COMPLETED"}})

    results = await manager.get_results(wf_id)

    assert results["nodes"] == {"A": {"status": "COMPLETED"}}
    assert results["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_two_parallel_workflows_isolated(exec_store, idemp, rstore, event_bus, event_store, counter):
    """
    Two workflows started should not overwrite metadata
    or results of one another.
    """
    manager = WorkflowManager(
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
        event_store=event_store,
        event_bus=event_bus,
        atomic_counter=counter,
    )

    async def fake_start():
        pass

    orig = EventDrivenDagOrchestrator.__init__
    def patch(self, *a, **kw):
        orig(self, *a, **kw)
        self.start = fake_start
    EventDrivenDagOrchestrator.__init__ = patch

    d = make_definition()

    await manager.start_workflow("A", d)
    await manager.start_workflow("B", d)

    EventDrivenDagOrchestrator.__init__ = orig

    assert "A" in exec_store.meta
    assert "B" in exec_store.meta
    assert exec_store.meta["A"]["workflow_id"] == "A"
    assert exec_store.meta["B"]["workflow_id"] == "B"


@pytest.mark.asyncio
async def test_get_status_after_completion_even_if_removed(exec_store, idemp, rstore, event_bus, event_store, counter):
    """
    get_status must still work after a workflow has been removed
    from memory (after completion).
    """
    manager = WorkflowManager(
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
        event_store=event_store,
        event_bus=event_bus,
        atomic_counter=counter,
    )

    wf_id = "wfZ"

    await exec_store.save_metadata(
        wf_id,
        {
            "workflow_id": wf_id,
            "status": "COMPLETED",
            "created_at": 111,
            "completed_at": 222,
        },
    )

    # workflow is NOT in manager.workflows → fallback
    status = await manager.get_status(wf_id)

    assert status["state"] == "COMPLETED"
    assert status["completed_at"] == 222
