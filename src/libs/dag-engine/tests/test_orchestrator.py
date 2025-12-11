import asyncio
import time
import typing as t
from unittest.mock import AsyncMock

import pytest

from dag_engine.transport.local import InMemoryTransport
from dag_engine.transport.messages import ResultMessage, ResultType, TaskMessage
from dag_engine.core.workflow import WorkflowDAG
from dag_engine.core.entities import NodeStatus
from dag_engine.core.orchestrator import DagOrchestrator
from dag_engine.event_sourcing import WorkflowEventType


class FakeEventStore:
    def __init__(self):
        self.append = AsyncMock()


def make_definition(nodes: list[dict]) -> dict:
    return {"name": "wf", "dag": {"nodes": nodes}}


def get_task_from_queue(transport: InMemoryTransport, timeout: float = 0.5) -> TaskMessage:
    # transport.publish_task puts TaskMessage into _task_q
    # get it synchronously from the queue
    return asyncio.get_event_loop().run_until_complete(transport._task_q.get())



@pytest.mark.asyncio
async def test_fanin_parallel_completion_same_millisecond(in_mem_transport, idemp, rstore):
    """
    Two nodes A and B complete at same millisecond.
    Orchestrator must schedule child C exactly once.
    """

    dag = WorkflowDAG.from_dict(
        make_definition([
            {"id": "A", "handler": "task", "dependencies": []},
            {"id": "B", "handler": "task", "dependencies": []},
            {"id": "C", "handler": "task", "dependencies": ["A", "B"]},
        ]),
        workflow_id="wf123",
    )
    evt = FakeEventStore()
    orch = DagOrchestrator(
        dag=dag,
        transport=in_mem_transport,
        idempotency_store=idemp,
        event_store=evt,
        result_store=rstore,
    )

    # Pretend roots A and B were already published
    dag.nodes["A"].status = NodeStatus.RUNNING
    dag.nodes["A"].attempt = 1

    dag.nodes["B"].status = NodeStatus.RUNNING
    dag.nodes["B"].attempt = 1

    ts = time.time()  # same timestamp for both completions

    # Build two completion messages
    msgA = ResultMessage(
        workflow_id="wf123",
        workflow_name="wf",
        node_id="A",
        attempt=1,
        timestamp=ts,
        type=ResultType.COMPLETED,
        payload={"a": 1},
        error=None,
    )
    msgB = ResultMessage(
        workflow_id="wf123",
        workflow_name="wf",
        node_id="B",
        attempt=1,
        timestamp=ts,
        type=ResultType.COMPLETED,
        payload={"b": 2},
        error=None,
    )

    # Process both messages concurrently — the core race condition
    await asyncio.gather(
        orch._handle_result(msgA),
        orch._handle_result(msgB),
    )

    # After both complete, dependent C should be PUBLISHED exactly once
    published = []
    try:
        while True:
            published.append(in_mem_transport._task_q.get_nowait())
    except asyncio.QueueEmpty:
        pass

    # Should only publish C exactly once
    c_tasks = [t for t in published if t.node_id == "C"]
    assert len(c_tasks) == 1, f"C scheduled {len(c_tasks)} times instead of once"

    # Ensure node C is in RUNNING state
    assert dag.nodes["C"].status == NodeStatus.RUNNING

    # Ensure events exist for A,B completions and C start
    evt_types = [call.args[0].event_type for call in evt.append.await_args_list]
    assert evt_types.count("NODE_COMPLETED") == 2
    assert "NODE_STARTED" in evt_types


@pytest.mark.asyncio
async def test_publish_root_nodes(in_mem_transport, idemp):
    """
    When orchestrator starts, root nodes (no dependencies) should be published.
    """
    definition = make_definition(
        [
            {"id": "input", "handler": "input", "dependencies": []},
            {"id": "child", "handler": "task", "dependencies": ["input"]},
        ]
    )
    dag = WorkflowDAG.from_dict(definition, workflow_id="wf1")
    evt = FakeEventStore()

    orchestrator = DagOrchestrator(
        dag=dag,
        transport=in_mem_transport,
        idempotency_store=idemp,
        event_store=evt,
        result_store=None,
        idempotency_ttl_seconds=60,
    )

    # Publish root nodes by calling _publish_task for root node
    root = dag.nodes["input"]
    # publish once
    await orchestrator._publish_task(root)

    # read published task from transport queue
    task = await in_mem_transport._task_q.get()
    assert isinstance(task, TaskMessage)
    assert task.node_id == "input"
    # Node should have status RUNNING
    assert root.status == NodeStatus.RUNNING
    assert root.attempt == 1  # attempt was incremented from default 0 -> 1


@pytest.mark.asyncio
async def test_handle_success_inline_result_and_schedule_dependents(in_mem_transport, idemp, rstore):
    """
    Simulate a worker returning inline result for node 'a'.
    Orchestrator should: persist inline result (if result_store enabled),
    mark node COMPLETED, emit NODE_COMPLETED, and publish dependent tasks when ready.
    """
    definition = make_definition(
        [
            {"id": "a", "handler": "task", "dependencies": []},
            {"id": "b", "handler": "task", "dependencies": ["a"]},
        ]
    )
    dag = WorkflowDAG.from_dict(definition, workflow_id="wf2")

    evt = FakeEventStore()

    orchestrator = DagOrchestrator(
        dag=dag,
        transport=in_mem_transport,
        idempotency_store=idemp,
        event_store=evt,
        result_store=rstore,
    )

    # Prepare node a to be RUNNING and attempt 1
    node_a = dag.nodes["a"]
    node_a.status = NodeStatus.RUNNING
    node_a.attempt = 1
    node_a.started_at = time.time()
    node_a.deadline_at = node_a.started_at + 10

    # Create result message inline
    res_msg = ResultMessage(
        workflow_id="wf2",
        workflow_name=dag.workflow_name,
        node_id="a",
        attempt=1,
        timestamp=time.time(),
        type=ResultType.COMPLETED,
        payload={"value": 123},
        error=None,
    )

    # Call internal handler directly (avoid transport loop)
    await orchestrator._handle_result(res_msg)

    # Node a should be completed and result stored in result_store
    assert node_a.status == NodeStatus.COMPLETED
    assert node_a.result == {"value": 123}

    # Event emitted for completion
    assert evt.append.await_count >= 1
    # Dependent 'b' should be published to transport
    task = await in_mem_transport._task_q.get()
    assert task.node_id == "b"


@pytest.mark.asyncio
async def test_handle_success_pointer_result_fetches_from_result_store(in_mem_transport, idemp, rstore):
    """
    If worker returns a pointer {'result_key': ...}, orchestrator should fetch
    from ResultStore and use stored result value.
    """
    definition = make_definition(
        [
            {"id": "x", "handler": "task", "dependencies": []},
            {"id": "y", "handler": "task", "dependencies": ["x"]},
        ]
    )
    dag = WorkflowDAG.from_dict(definition, workflow_id="wf3")
    evt = FakeEventStore()

    # Pre-populate result store with x
    await rstore.save_result("wf3", "x", attempt=1, result={"answer": 7})

    orchestrator = DagOrchestrator(
        dag=dag,
        transport=in_mem_transport,
        idempotency_store=idemp,
        event_store=evt,
        result_store=rstore,
    )

    # Prepare node x
    node_x = dag.nodes["x"]
    node_x.status = NodeStatus.RUNNING
    node_x.attempt = 1

    # Worker returns pointer
    res_msg = ResultMessage(
        workflow_id="wf3",
        workflow_name=dag.workflow_name,
        node_id="x",
        attempt=1,
        timestamp=time.time(),
        type=ResultType.COMPLETED,
        payload={"result_key": rstore.get_key("wf3", "x")},
        error=None,
    )

    await orchestrator._handle_result(res_msg)

    # x should be completed and have loaded result
    assert node_x.status == NodeStatus.COMPLETED
    assert node_x.result == {"answer": 7}


@pytest.mark.asyncio
async def test_handle_failure_with_retry_and_permanent_failure(in_mem_transport, idemp, rstore):
    """
    Cover both retry-allowed and permanent-failure paths.
    """
    # create nodes
    definition = make_definition(
        [
            {"id": "n1", "handler": "task", "dependencies": []},
            {"id": "n2", "handler": "task", "dependencies": ["n1"]},
        ]
    )
    dag = WorkflowDAG.from_dict(definition, workflow_id="wf4")
    evt = FakeEventStore()

    orchestrator = DagOrchestrator(
        dag=dag,
        transport=in_mem_transport,
        idempotency_store=idemp,
        event_store=evt,
        result_store=rstore,
    )

    # Attach a simple retry policy on node n1 using a minimal object matching interface
    class RP:
        def __init__(self, max_attempts, delay=0.01):
            self.max_attempts = max_attempts
            self._delay = delay

        def backoff_for_attempt(self, attempt):
            return self._delay

    node = dag.nodes["n1"]
    node.status = NodeStatus.RUNNING
    node.attempt = 1
    node.retry_policy = RP(max_attempts=2)

    # 1) First failure — should schedule retry (node -> PENDING, emit NODE_RETRY)
    res_fail = ResultMessage(
        workflow_id="wf4",
        workflow_name=dag.workflow_name,
        node_id="n1",
        attempt=1,
        timestamp=time.time(),
        type=ResultType.FAILED,
        payload=None,
        error="boom",
    )

    # call failure handler
    await orchestrator._handle_result(res_fail)

    # after first failure with retry allowed -> PENDING
    assert dag.nodes["n1"].status == NodeStatus.PENDING
    assert dag.nodes["n1"].last_error == "boom"
    # retry event emitted at least once
    assert any(call.args[0].event_type == WorkflowEventType.NODE_RETRY for call in evt.append.await_args_list)

    # 2) Exhaust retries: set attempt to max and send failed again -> permanent fail
    node.attempt = 2
    node.status = NodeStatus.RUNNING

    res_fail2 = ResultMessage(
        workflow_id="wf4",
        workflow_name=dag.workflow_name,
        node_id="n1",
        attempt=2,
        timestamp=time.time(),
        type=ResultType.FAILED,
        payload=None,
        error="boom-2",
    )

    # spy block_dependents to ensure it is called
    dag.block_dependents = AsyncMock()

    await orchestrator._handle_result(res_fail2)

    assert dag.nodes["n1"].status == NodeStatus.FAILED
    assert dag.block_dependents.await_count >= 1
    assert any(call.args[0].event_type == WorkflowEventType.NODE_FAILED for call in evt.append.await_args_list)


@pytest.mark.asyncio
async def test_ignore_stale_or_wrong_workflow_results(in_mem_transport, idemp):
    """
    Ensure orchestrator ignores results for other workflows and stale attempts.
    """
    definition = make_definition([{"id": "z", "handler": "task", "dependencies": []}])
    dag = WorkflowDAG.from_dict(definition, workflow_id="wf5")

    evt = FakeEventStore()

    orchestrator = DagOrchestrator(
        dag=dag,
        transport=in_mem_transport,
        idempotency_store=idemp,
        event_store=evt,
    )

    node = dag.nodes["z"]
    node.status = NodeStatus.RUNNING
    node.attempt = 3

    # result for different workflow -> ignored
    res_other_wf = ResultMessage(
        workflow_id="other",
        workflow_name=dag.workflow_name,
        node_id="z",
        attempt=3,
        timestamp=time.time(),
        type=ResultType.COMPLETED,
        payload={"x": 1},
        error=None,
    )
    await orchestrator._handle_result(res_other_wf)
    assert node.status == NodeStatus.RUNNING

    # stale attempt -> ignored
    res_stale = ResultMessage(
        workflow_id="wf5",
        workflow_name=dag.workflow_name,
        node_id="z",
        attempt=1,  # doesn't match current attempt 3
        timestamp=time.time(),
        type=ResultType.COMPLETED,
        payload={"x": 1},
        error=None,
    )
    await orchestrator._handle_result(res_stale)
    assert node.status == NodeStatus.RUNNING
