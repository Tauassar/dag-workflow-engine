import asyncio
import pytest

from dag_engine.transport.messages import TaskMessage, ResultMessage, ResultType

from dag_engine.core.handlers import HandlerRegistry
from dag_engine.core.worker import WorkflowWorker


def make_task(node_type="echo", node_id="A", attempt=1, config=None):
    return TaskMessage(
        workflow_id="wf1",
        workflow_name="demo",
        node_id=node_id,
        node_type=node_type,
        attempt=attempt,
        config=config or {},
    )


@pytest.mark.asyncio
async def test_worker_executes_successful_handler(in_mem_transport, idemp, rstore):

    registry = HandlerRegistry()

    @registry.handler("echo")
    async def echo(task: TaskMessage):
        return {"msg": "ok"}

    worker = WorkflowWorker(
        transport=in_mem_transport,
        handler_registry=registry.handlers,
        idempotency_store=idemp,
        result_store=rstore,
        worker_id="w1",
    )

    # Feed a task to the worker
    task = make_task("echo", "A")
    await in_mem_transport.publish_task(task)

    # Run worker in background
    worker_task = asyncio.create_task(worker.run())

    # Collect result
    msg = await asyncio.wait_for(in_mem_transport._result_q.get(), timeout=1)

    assert isinstance(msg, ResultMessage)
    assert msg.type == ResultType.COMPLETED
    assert msg.node_id == "A"

    # Pointer returned?
    assert "result_key" in msg.payload

    stored = await rstore.get_result("wf1", "A")
    assert stored["result"] == {"msg": "ok"}

    worker_task.cancel()


@pytest.mark.asyncio
async def test_worker_handler_failure(in_mem_transport, idemp, rstore):
    registry = HandlerRegistry()

    @registry.handler("boom")
    async def boom(task: TaskMessage):
        raise RuntimeError("fail!")

    worker = WorkflowWorker(
        transport=in_mem_transport,
        handler_registry=registry.handlers,
        idempotency_store=idemp,
        result_store=rstore,
        worker_id="w1",
    )

    await in_mem_transport.publish_task(make_task("boom", "B"))

    worker_task = asyncio.create_task(worker.run())

    msg = await asyncio.wait_for(in_mem_transport._result_q.get(), timeout=1)

    assert msg.type == ResultType.FAILED
    assert "fail!" in msg.error
    assert msg.node_id == "B"

    worker_task.cancel()


@pytest.mark.asyncio
async def test_worker_idempotency_skips_duplicate_tasks(in_mem_transport, idemp, rstore):
    registry = HandlerRegistry()

    calls = 0

    @registry.handler("echo")
    async def echo(task: TaskMessage):
        nonlocal calls
        calls += 1
        return {"msg": "ok"}

    worker = WorkflowWorker(
        transport=in_mem_transport,
        handler_registry=registry.handlers,
        idempotency_store=idemp,
        result_store=rstore,
        worker_id="w1",
    )

    t = make_task("echo", "A", attempt=1)

    # Publish SAME task twice
    await in_mem_transport.publish_task(t)
    await in_mem_transport.publish_task(t)

    worker_task = asyncio.create_task(worker.run())

    # Wait for *first* result
    res1 = await asyncio.wait_for(in_mem_transport._result_q.get(), timeout=1)

    # Worker should not emit a second result for duplicate task
    await asyncio.sleep(0.2)
    assert in_mem_transport._result_q.empty()

    assert calls == 1  # handler executed once

    worker_task.cancel()


@pytest.mark.asyncio
async def test_worker_no_handler_failure(in_mem_transport, idemp, rstore):
    worker = WorkflowWorker(
        transport=in_mem_transport,
        handler_registry={},
        idempotency_store=idemp,
        result_store=rstore,
        worker_id="w1",
    )

    await in_mem_transport.publish_task(make_task("unknown", "X"))

    worker_task = asyncio.create_task(worker.run())
    msg = await asyncio.wait_for(in_mem_transport._result_q.get(), timeout=1)

    assert msg.type == ResultType.FAILED
    assert "No handler registered" in msg.error

    worker_task.cancel()
