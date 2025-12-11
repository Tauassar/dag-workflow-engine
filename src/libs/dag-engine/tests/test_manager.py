import pytest

from dag_engine.core.manager import WorkflowManager
from dag_engine.core.constants import NodeStatus
from dag_engine.core.orchestrator import DagOrchestrator
from dag_engine.core.schemas import WorkflowDefinition


class FakeEventStore:
    async def append(self, *_args, **_kwargs):
        pass


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
async def test_start_workflow_registers_and_writes_metadata(in_mem_transport, exec_store, idemp, rstore):
    """
    WorkflowManager.start_workflow:
      - creates DAG
      - registers WorkflowInfo in memory
      - writes initial metadata to ExecutionStore
      - starts orchestrator
    """
    estore = FakeEventStore()

    manager = WorkflowManager(
        transport=in_mem_transport,
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
        event_store=estore,
    )

    # Patch orchestrator.start() so it doesn’t actually try to run tasks
    async def fake_start():
        pass

    workflow_id = "wf-1"
    definition = make_definition()

    # monkeypatch the orchestrator start method for this test
    original_ctor = DagOrchestrator.__init__

    def patched_ctor(self, *args, **kwargs):
        original_ctor(self, *args, **kwargs)
        self.start = fake_start

    DagOrchestrator.__init__ = patched_ctor

    info = await manager.start_workflow(workflow_id, definition)

    # Restore orchestrator ctor after test
    DagOrchestrator.__init__ = original_ctor

    # Workflow should be registered
    assert workflow_id in manager.workflows

    # Execution store should have metadata
    meta = exec_store.meta[workflow_id]
    assert meta["workflow_id"] == workflow_id
    assert meta["status"] == "RUNNING"
    assert meta["completed_at"] is None


@pytest.mark.asyncio
async def test_on_workflow_complete_removes_workflow_from_memory(in_mem_transport, exec_store, idemp, rstore):
    """
    _on_workflow_complete should:
      - mark workflow as COMPLETED
      - save metadata and results
      - remove workflow from manager.workflows
    """

    manager = WorkflowManager(
        transport=in_mem_transport,
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
        event_store=None,
    )

    wf_id = "wf-test"
    definition = make_definition()

    # Patch orchestrator.start so it doesn't run tasks
    async def fake_start():
        pass

    original_ctor = DagOrchestrator.__init__

    def patched_ctor(self, *a, **kw):
        original_ctor(self, *a, **kw)
        self.start = fake_start
        # force DAG to appear completed
        for n in self.dag.nodes.values():
            n.status = NodeStatus.COMPLETED

    DagOrchestrator.__init__ = patched_ctor

    info = await manager.start_workflow(wf_id, definition)

    # Trigger on_complete manually
    await manager._on_workflow_complete(wf_id)

    # restore
    DagOrchestrator.__init__ = original_ctor

    # Workflow removed from memory
    assert wf_id not in manager.workflows

    # Metadata saved
    meta = exec_store.meta[wf_id]
    assert meta["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_get_status_falls_back_to_persisted_metadata(in_mem_transport, exec_store, idemp, rstore):
    """
    get_status should use persisted status when workflow
    is no longer present in memory.
    """
    manager = WorkflowManager(
        transport=in_mem_transport,
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
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
async def test_get_results_returns_persisted_results(in_mem_transport, exec_store, idemp, rstore):
    """
    get_results must always return persisted results.
    """
    manager = WorkflowManager(
        transport=in_mem_transport,
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
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
async def test_two_parallel_workflows_isolated(in_mem_transport, exec_store, idemp, rstore):
    """
    Two workflows started should not overwrite metadata
    or results of one another.
    """
    manager = WorkflowManager(
        transport=in_mem_transport,
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
    )

    async def fake_start():
        pass

    orig = DagOrchestrator.__init__
    def patch(self, *a, **kw):
        orig(self, *a, **kw)
        self.start = fake_start
    DagOrchestrator.__init__ = patch

    d = make_definition()

    await manager.start_workflow("A", d)
    await manager.start_workflow("B", d)

    DagOrchestrator.__init__ = orig

    assert "A" in exec_store.meta
    assert "B" in exec_store.meta
    assert exec_store.meta["A"]["workflow_id"] == "A"
    assert exec_store.meta["B"]["workflow_id"] == "B"


@pytest.mark.asyncio
async def test_get_status_after_completion_even_if_removed(in_mem_transport, exec_store, idemp, rstore):
    """
    get_status must still work after a workflow has been removed
    from memory (after completion).
    """
    manager = WorkflowManager(
        transport=in_mem_transport,
        result_store=rstore,
        execution_store=exec_store,
        idempotency_store=idemp,
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
