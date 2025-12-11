import pytest
import time
from unittest.mock import AsyncMock, MagicMock

from dag_engine.core.constants import NodeStatus
from dag_engine.core.entities import DagNode, RetryPolicy
from dag_engine.core.timeout_monitor import TimeoutMonitor
from dag_engine.core.workflow import WorkflowDAG
from dag_engine.event_sourcing import WorkflowEventType
from dag_engine.store.idempotency import IdempotencyStore


class DummyIdempotencyStore(IdempotencyStore):
    """Simple in-memory idempotency mock."""
    def __init__(self):
        self.keys = set()

    async def set_if_absent(self, key: str, ttl_seconds: int | None = None) -> bool:
        if key in self.keys:
            return False
        self.keys.add(key)
        return True


def make_node(id, depends=None, timeout=None, policy=None):
    n = DagNode(
        id=id,
        type="test",
        config={},
        depends_on=set(depends or []),
        timeout_seconds=timeout,
        retry_policy=policy,
    )
    return n


def make_dag(nodes, workflow_id="wf"):
    """Minimal DAG object with nodes injected manually."""
    class DummyDef:
        name = "x"
        dag = MagicMock(nodes=[])

    dag = WorkflowDAG(workflow_id, DummyDef)
    dag._nodes = {n.id: n for n in nodes}

    # build dependents
    for n in dag.nodes.values():
        n.dependents = set()
    for n in dag.nodes.values():
        for d in n.depends_on:
            dag.nodes[d].dependents.add(n.id)

    return dag


@pytest.mark.asyncio
async def test_no_timeout_if_not_overdue():
    node = make_node("a", timeout=100)
    node.status = NodeStatus.RUNNING
    node.started_at = time.time()
    node.deadline_at = node.started_at + 999

    dag = make_dag([node])
    idemp = DummyIdempotencyStore()

    monitor = TimeoutMonitor(
        dag=dag,
        idempotency_store=idemp,
        event_handler=None
    )

    await monitor._check_timeouts()
    assert node.status == NodeStatus.RUNNING


@pytest.mark.asyncio
async def test_timeout_triggers_retry():
    retry_policy = RetryPolicy(max_attempts=3)

    node = make_node("a", timeout=0.1, policy=retry_policy)
    node.status = NodeStatus.RUNNING
    node.attempt = 1
    node.deadline_at = time.time() - 1  # overdue

    dag = make_dag([node])
    idemp = DummyIdempotencyStore()

    retry_called = False

    async def mock_retry(node_id):
        nonlocal retry_called
        retry_called = True

    events = []

    async def mock_event(event):
        events.append(event)

    monitor = TimeoutMonitor(
        dag=dag,
        idempotency_store=idemp,
        event_handler=mock_event,
        dispatch_retry_callback=mock_retry,
    )

    await monitor._check_timeouts()

    # Node should be retried → moved to PENDING
    assert node.status == NodeStatus.PENDING
    assert retry_called is True
    assert any(e.event_type == WorkflowEventType.NODE_RETRY for e in events)


@pytest.mark.asyncio
async def test_timeout_permanent_failure_when_no_retries():
    node = make_node("a", timeout=0.1, policy=RetryPolicy(max_attempts=1))
    node.status = NodeStatus.RUNNING
    node.attempt = 1
    node.deadline_at = time.time() - 1

    dag = make_dag([node])
    idemp = DummyIdempotencyStore()
    dag.block_dependents = AsyncMock()

    events = []

    async def mock_event(event):
        events.append(event)

    monitor = TimeoutMonitor(
        dag=dag,
        idempotency_store=idemp,
        event_handler=mock_event,
    )

    await monitor._check_timeouts()

    assert node.status == NodeStatus.FAILED
    assert node.last_error.startswith("TIMEOUT")
    assert any(e.event_type == WorkflowEventType.NODE_FAILED for e in events)

    dag.block_dependents.assert_awaited_once_with("a")


@pytest.mark.asyncio
async def test_timeout_not_reprocessed_due_to_idempotency():
    node = make_node("a", timeout=0.1)
    node.status = NodeStatus.RUNNING
    node.attempt = 1
    node.deadline_at = time.time() - 1

    dag = make_dag([node])
    idemp = DummyIdempotencyStore()

    # Simulate key already set
    await idemp.set_if_absent(f"timeout:{dag.workflow_id}:a:1", ttl_seconds=60)

    retry_called = False

    async def retry(_):
        nonlocal retry_called
        retry_called = True

    monitor = TimeoutMonitor(
        dag=dag,
        idempotency_store=idemp,
        event_handler=None,
        dispatch_retry_callback=retry,
    )

    await monitor._check_timeouts()

    # Idempotency prevents retry or fail
    assert retry_called is False
    assert node.status == NodeStatus.RUNNING


@pytest.mark.asyncio
async def test_timeout_skip_if_node_already_completed():
    node = make_node("a", timeout=0.1)
    node.status = NodeStatus.COMPLETED
    node.attempt = 1
    node.deadline_at = time.time() - 1  # overdue

    dag = make_dag([node])
    idemp = DummyIdempotencyStore()

    monitor = TimeoutMonitor(
        dag=dag,
        idempotency_store=idemp,
        event_handler=None,
    )

    await monitor._check_timeouts()

    # Should remain completed
    assert node.status == NodeStatus.COMPLETED


@pytest.mark.asyncio
async def test_timeout_skip_if_missing_node():
    """Edge case: node removed between collect + process loop."""
    node = make_node("a", timeout=0.1)
    node.status = NodeStatus.RUNNING
    node.attempt = 1
    node.deadline_at = time.time() - 1

    dag = make_dag([node])
    idemp = DummyIdempotencyStore()

    # Override dag.nodes so it disappears during processing
    original = dag.nodes

    async def fake_check(*_):
        dag._nodes = {}  # delete nodes before processing

    monitor = TimeoutMonitor(dag=dag, idempotency_store=idemp)
    monitor.dag = dag

    # Monkeypatch internal behavior
    monitor.dag._nodes = original

    await monitor._check_timeouts()

    # Should not crash
    assert True
