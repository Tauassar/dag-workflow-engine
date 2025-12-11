import asyncio
import pytest

from dag_engine.transport.local import InMemoryTransport
from dag_engine.transport.messages import TaskMessage, ResultMessage, ResultType


def make_task(id="t1"):
    return TaskMessage(
        workflow_id="wf",
        workflow_name="name",
        node_id=id,
        node_type="task",
        attempt=1,
        config={}
    )


def make_result(id="t1", payload="ok"):
    return ResultMessage(
        workflow_id="wf",
        workflow_name="name",
        node_id=id,
        attempt=1,
        timestamp=123.0,
        type=ResultType.COMPLETED,
        payload=payload,
        error=None,
    )


@pytest.mark.asyncio
async def test_publish_and_subscribe_tasks():
    tr = InMemoryTransport()

    t1 = make_task("a")
    t2 = make_task("b")

    await tr.publish_task(t1)
    await tr.publish_task(t2)

    results = []

    async def reader():
        async for task in tr.subscribe_tasks():
            results.append(task)
            if len(results) == 2:
                await tr.close_tasks()

    await asyncio.wait_for(reader(), timeout=1.0)

    assert results == [t1, t2]


@pytest.mark.asyncio
async def test_close_tasks_stops_iteration():
    tr = InMemoryTransport()
    await tr.close_tasks()

    collected = []

    async def reader():
        async for task in tr.subscribe_tasks():
            collected.append(task)

    await asyncio.wait_for(reader(), timeout=1.0)

    assert collected == []  # no tasks yielded before termination


@pytest.mark.asyncio
async def test_publish_and_subscribe_results():
    tr = InMemoryTransport()

    r1 = make_result("n1")
    r2 = make_result("n2")

    await tr.publish_result(r1)
    await tr.publish_result(r2)

    collected = []

    async def reader():
        async for r in tr.subscribe_results():
            collected.append(r)
            if len(collected) == 2:
                await tr.close_results()

    await asyncio.wait_for(reader(), timeout=1.0)

    assert collected == [r1, r2]


@pytest.mark.asyncio
async def test_close_results_stops_iteration():
    tr = InMemoryTransport()
    await tr.close_results()

    collected = []

    async def reader():
        async for r in tr.subscribe_results():
            collected.append(r)

    await asyncio.wait_for(reader(), timeout=1.0)

    assert collected == []


@pytest.mark.asyncio
async def test_task_and_result_streams_independent():
    tr = InMemoryTransport()

    t = make_task("task1")
    r = make_result("node1")

    await tr.publish_task(t)
    await tr.publish_result(r)

    task_received = None
    result_received = None

    async def read_task():
        async for x in tr.subscribe_tasks():
            nonlocal task_received
            task_received = x
            await tr.close_tasks()

    async def read_result():
        async for x in tr.subscribe_results():
            nonlocal result_received
            result_received = x
            await tr.close_results()

    await asyncio.wait_for(asyncio.gather(read_task(), read_result()), timeout=1.0)

    assert task_received == t
    assert result_received == r


@pytest.mark.asyncio
async def test_task_order_preserved():
    tr = InMemoryTransport()

    t1 = make_task("A")
    t2 = make_task("B")
    t3 = make_task("C")

    await tr.publish_task(t1)
    await tr.publish_task(t2)
    await tr.publish_task(t3)

    received = []

    async def read():
        async for t in tr.subscribe_tasks():
            received.append(t)
            if len(received) == 3:
                await tr.close_tasks()

    await asyncio.wait_for(read(), timeout=1.0)

    assert received == [t1, t2, t3]


@pytest.mark.asyncio
async def test_result_order_preserved():
    tr = InMemoryTransport()

    r1 = make_result("1")
    r2 = make_result("2")
    r3 = make_result("3")

    await tr.publish_result(r1)
    await tr.publish_result(r2)
    await tr.publish_result(r3)

    received = []

    async def read():
        async for r in tr.subscribe_results():
            received.append(r)
            if len(received) == 3:
                await tr.close_results()

    await asyncio.wait_for(read(), timeout=1.0)

    assert received == [r1, r2, r3]
