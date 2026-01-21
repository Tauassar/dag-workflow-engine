import asyncio
import time

import pytest

from dag_engine.event_sourcing import WorkflowEvent, WorkflowEventType
from dag_engine.core.timeout_monitor import GlobalTimeoutMonitor


@pytest.mark.asyncio
async def test_watch_task_on_node_started(event_bus, idemp, consumer):
    monitor = GlobalTimeoutMonitor(
        idempotency_store=idemp,
        event_bus=event_bus,
        check_interval=0.1,
    )
    now = time.time()
    event = WorkflowEvent(
        workflow_id="wf1",
        node_id="n1",
        event_type=WorkflowEventType.NODE_STARTED,
        attempt=1,
        timestamp=now,
        expire_at=now + 10,
    )

    await event_bus.publish(event)

    for msg in (await consumer.get_all_messages()):
        event = WorkflowEvent.model_validate(msg)
        await event_bus.handle_event(event)

    assert "n1:wf1" in monitor._started_nodes


@pytest.mark.asyncio
async def test_does_not_watch_without_expire(event_bus, idemp):
    monitor = GlobalTimeoutMonitor(idemp, event_bus)

    event = WorkflowEvent(
        workflow_id="wf1",
        node_id="n1",
        event_type=WorkflowEventType.NODE_STARTED,
        attempt=1,
        timestamp=time.time(),
        expire_at=None,
    )

    await event_bus.publish(event)

    assert monitor._started_nodes == {}


@pytest.mark.asyncio
async def test_forget_task_on_node_completed(event_bus, idemp):
    monitor = GlobalTimeoutMonitor(idempotency_store=idemp, event_bus=event_bus)

    now = time.time()
    start = WorkflowEvent(
        workflow_id="wf1",
        node_id="n1",
        event_type=WorkflowEventType.NODE_STARTED,
        attempt=1,
        timestamp=now,
        expire_at=now + 10,
    )
    await event_bus.publish(start)

    end = WorkflowEvent(
        workflow_id="wf1",
        node_id="n1",
        event_type=WorkflowEventType.NODE_COMPLETED,
        attempt=1,
        timestamp=now + 1,
    )
    await event_bus.publish(end)

    assert monitor._started_nodes == {}


@pytest.mark.asyncio
async def test_timeout_emitted(event_store, event_bus, idemp, consumer):
    monitor = GlobalTimeoutMonitor(
        idempotency_store=idemp,
        event_bus=event_bus,
        check_interval=0.05,
    )

    now = time.time()
    start = WorkflowEvent(
        workflow_id="wf1",
        node_id="n1",
        event_type=WorkflowEventType.NODE_STARTED,
        attempt=1,
        timestamp=now,
        expire_at=now + 0.01,
    )

    await event_bus.publish(start)
    await asyncio.sleep(0.02)

    for msg in (await consumer.get_all_messages()):
        await event_bus.handle_event(WorkflowEvent.model_validate(msg))

    await monitor._check_timeouts()

    for msg in (await consumer.get_all_messages()):
        await event_bus.handle_event(WorkflowEvent.model_validate(msg))

    timeout_events = [
        e for e in event_store._events["wf1"]
        if e.event_type == WorkflowEventType.NODE_TIMEOUT
    ]

    assert len(event_store._events["wf1"]) == 2
    assert len(timeout_events) == 1
    evt = timeout_events[0]
    assert evt.node_id == "n1"
    assert evt.workflow_id == "wf1"
    assert evt.error == "TIMEOUT"


@pytest.mark.asyncio
async def test_timeout_idempotency(event_store, event_bus, idemp, consumer):
    monitor = GlobalTimeoutMonitor(
        idempotency_store=idemp,
        event_bus=event_bus,
        check_interval=0.01,
    )

    now = time.time()
    start = WorkflowEvent(
        workflow_id="wf1",
        node_id="n1",
        event_type=WorkflowEventType.NODE_STARTED,
        attempt=1,
        timestamp=now,
        expire_at=now + 0.01,
    )
    await asyncio.sleep(0.02)

    await event_bus.publish(start)
    await event_bus.publish(start)

    for msg in (await consumer.get_all_messages()):
        event = WorkflowEvent.model_validate(msg)
        await event_bus.handle_event(event)

    await monitor._check_timeouts()

    for msg in (await consumer.get_all_messages()):
        event = WorkflowEvent.model_validate(msg)
        await event_bus.handle_event(event)

    timeouts = [
        e for e in event_store._events["wf1"]
        if e.event_type == WorkflowEventType.NODE_TIMEOUT
    ]

    assert len(timeouts) == 1


@pytest.mark.asyncio
async def test_forget_prevents_timeout(event_store, event_bus, idemp):
    monitor = GlobalTimeoutMonitor(
        idempotency_store=idemp,
        event_bus=event_bus,
        check_interval=0.05,
    )

    now = time.time()
    start = WorkflowEvent(
        workflow_id="wf1",
        node_id="n1",
        event_type=WorkflowEventType.NODE_STARTED,
        attempt=1,
        timestamp=now,
        expire_at=now + 0.1,
    )

    await monitor.start()
    await event_bus.publish(start)

    await event_bus.publish(
        WorkflowEvent(
            workflow_id="wf1",
            node_id="n1",
            event_type=WorkflowEventType.NODE_FAILED,
            attempt=1,
            timestamp=time.time(),
        )
    )
    await monitor.stop()

    assert not any(
        e.event_type == WorkflowEventType.NODE_TIMEOUT
        for e in event_store._events["wf1"]
    )
