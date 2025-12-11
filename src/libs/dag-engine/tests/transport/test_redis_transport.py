import asyncio
import pytest

from dag_engine.transport.messages import TaskMessage, ResultMessage, ResultType


def make_task(node_id="A"):
    return TaskMessage(
        workflow_id="wf1",
        workflow_name="demo",
        node_id=node_id,
        node_type="task",
        attempt=1,
        config={"k": "v"},
    )


def make_result(node_id="A"):
    return ResultMessage(
        workflow_id="wf1",
        workflow_name="demo",
        node_id=node_id,
        attempt=1,
        timestamp=123.45,
        type=ResultType.COMPLETED,
        payload={"ok": True},
        error=None,
    )


@pytest.mark.asyncio
async def test_streams_and_groups_created(redis, transport):
    # Streams must exist (use xinfo_stream which works with fakeredis)
    info_tasks = await redis.xinfo_stream("tasks")
    info_results = await redis.xinfo_stream("results")
    assert "length" in info_tasks
    assert "length" in info_results

    groups_tasks = await redis.xinfo_groups("tasks")
    # support both fakeredis (str keys) and real redis (bytes keys)
    names = [(g.get("name") or g.get(b"name")) for g in groups_tasks]
    assert "task_group" in names or b"task_group" in names

    groups_results = await redis.xinfo_groups("results")
    names2 = [(g.get("name") or g.get(b"name")) for g in groups_results]
    assert "result_group" in names2 or b"result_group" in names2


@pytest.mark.asyncio
async def test_publish_and_subscribe_tasks(redis, transport):
    t1 = make_task("A")
    await transport.publish_task(t1)

    received = []

    async def reader():
        async for msg in transport.subscribe_tasks():
            received.append(msg)
            break

    await asyncio.wait_for(reader(), timeout=1.0)

    assert len(received) == 1
    assert received[0].node_id == "A"
    assert received[0].config == {"k": "v"}

    # ✔ Verify: no message is returned again
    resp = await redis.xreadgroup(
        groupname="task_group",
        consumername="consumer-test",
        streams={"tasks": ">"},
        count=1,
        block=50,
    )
    assert resp == []  # nothing left to consume


@pytest.mark.asyncio
async def test_publish_and_subscribe_results(redis, transport):
    r1 = make_result("A")
    await transport.publish_result(r1)

    received = []

    async def reader():
        async for msg in transport.subscribe_results(wf_id="wf1"):
            received.append(msg)
            break

    await asyncio.wait_for(reader(), timeout=1.0)

    assert len(received) == 1
    assert received[0].node_id == "A"
    assert received[0].payload == {"ok": True}

    # message should be removed from Redis Streams
    resp = await redis.xreadgroup(
        groupname="result_groupwf1",
        consumername="c",
        streams={"results": ">"},
        count=1,
        block=50,
    )
    assert resp == []


@pytest.mark.asyncio
async def test_each_workflow_gets_its_own_result_group(redis, transport):
    """subscribe_results() must create a unique consumer group per workflow."""

    await transport.publish_result(make_result("X"))

    received = []

    async def reader():
        async for msg in transport.subscribe_results(wf_id="wfA"):
            received.append(msg)
            break

    await asyncio.wait_for(reader(), timeout=1.0)

    groups = await redis.xinfo_groups("results")
    names = [(g.get("name") or g.get(b"name")) for g in groups]
    assert "result_groupwfA" in names or b"result_groupwfA" in names

    assert received[0].node_id == "X"


@pytest.mark.asyncio
async def test_invalid_message_in_results_is_skipped(redis, transport):
    # Insert a fake broken JSON into results stream
    await redis.xadd("results", {"json": "not-valid-json"})

    received = []

    async def reader():
        # publish a valid one after the invalid one
        await transport.publish_result(make_result("good"))

        async for msg in transport.subscribe_results("wf2"):
            received.append(msg)
            break  # stop after valid message

    await asyncio.wait_for(reader(), timeout=1.0)

    assert len(received) == 1
    assert received[0].node_id == "good"

    # invalid entry should have been acked & removed
    resp = await redis.xreadgroup(
        groupname="task_group",
        consumername="consumer-test",
        streams={"tasks": ">"},
        count=1,
        block=50,
    )
    assert resp == []  # nothing left to consume

@pytest.mark.asyncio
async def test_invalid_message_in_tasks_is_skipped(redis, transport):
    await redis.xadd("tasks", {"json": "not-json"})

    received = []

    async def reader():
        await transport.publish_task(make_task("ok"))

        async for msg in transport.subscribe_tasks():
            received.append(msg)
            break

    await asyncio.wait_for(reader(), timeout=1.0)

    assert received[0].node_id == "ok"

    # stream should be empty after ack + del
    resp = await redis.xreadgroup(
        groupname="task_group",
        consumername="consumer-test",
        streams={"tasks": ">"},
        count=1,
        block=50,
    )
    assert resp == []  # nothing left to consume
