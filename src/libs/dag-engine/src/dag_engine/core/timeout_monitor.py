import asyncio
import logging
import math
import time
import typing as t
from abc import ABC

from dag_engine.core import NodeStatus
from dag_engine.event_sourcing import WorkflowEvent, WorkflowEventType, EventBus, EventHandler
from dag_engine.store.idempotency import IdempotencyStore

logger = logging.getLogger(__name__)


class TimeoutMonitor:
    """
    Handles authoritative timeouts for DAG nodes.

    Responsibilities:
    - Periodically check RUNNING nodes with deadlines.
    - Apply retry or failure on timeout.
    - Use IdempotencyStore to ensure exactly-once timeout handling.
    - Emit events through orchestrator callback.
    - Safe for multiple orchestrators (if running in HA mode with shared Redis).
    """

    def __init__(
        self,
        dag,
        idempotency_store: IdempotencyStore,
        event_bus: EventBus,
        check_interval: float = 1.0,
        dispatch_retry_callback: t.Callable[[str], t.Any] | None = None,
    ):
        """
        Args:
            idempotency_store: Shared IdempotencyStore implementation.
            event_bus: event emitter.
            check_interval: Timeout polling frequency in seconds.
            dispatch_retry_callback:
                async callback(node_id) -> None
                Orchestrator must supply a function that triggers re-dispatch
                after a retry is detected.
        """
        self.dag = dag
        self.idempotency_store = idempotency_store
        self.event_bus = event_bus
        self.check_interval = check_interval
        self.dispatch_retry_callback = dispatch_retry_callback

        self._task: asyncio.Task | None = None
        self._stopped = False
        self._lock = asyncio.Lock()

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _emit_event(self, event: WorkflowEvent):
        await self.event_bus.publish(event)

    async def _run(self):
        try:
            while not self._stopped:
                await self._check_timeouts()
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(f"TimeoutMonitor error: {exc}")

    async def _check_timeouts(self):
        now = time.time()
        overdue = []

        # First collect candidate nodes
        async with self._lock:
            for node in self.dag.nodes.values():
                if node.status == NodeStatus.RUNNING and getattr(node, "deadline_at", None):
                    if now > node.deadline_at:
                        logger.warning(f"Node {node.id} is timed out.")
                        overdue.append(node.id)

        # Then process each overdue node
        for nid in overdue:
            async with self._lock:
                node = self.dag.nodes.get(nid)
                if not node:
                    continue
                # Node might have completed after collection
                if node.status != NodeStatus.RUNNING:
                    continue

                # Idempotency: ensure timeout once per attempt
                tkey = f"timeout:{self.dag.workflow_id}:{nid}:{node.attempt}"
                ok = await self.idempotency_store.set_if_absent(tkey, ttl_seconds=int(node.timeout_seconds or 60))
                if not ok:
                    continue  # already processed elsewhere

                policy = node.retry_policy

                # Retry allowed?
                if policy and node.attempt < policy.max_attempts:
                    # Mark as PENDING, increment attempt, and schedule retry
                    logger.warning(f"Retrying node {node.id}...")
                    await self._emit_event(
                        WorkflowEvent(
                            workflow_id=self.dag.workflow_id,
                            workflow_name=self.dag.workflow_name,
                            node_id=node.id,
                            event_type=WorkflowEventType.NODE_RETRY,
                            attempt=node.attempt,
                            payload={"error": "TIMEOUT"},
                            error="TIMEOUT",
                        )
                    )

                    node.status = NodeStatus.PENDING
                    node.last_error = f"TIMEOUT on attempt {node.attempt}"
                    node.finished_at = now
                    node.started_at = None
                    node.deadline_at = None

                    # Ask orchestrator to dispatch retry
                    if self.dispatch_retry_callback:
                        # dispatch_retry_callback must be async
                        await self.dispatch_retry_callback(node.id)

                else:
                    # Permanent timeout → FAIL
                    logger.warning(f"Retry not allowed for node {node.id}, failing it")
                    node.status = NodeStatus.FAILED
                    node.last_error = f"TIMEOUT on attempt {node.attempt}"
                    node.finished_at = now
                    node.started_at = None
                    node.deadline_at = None

                    await self._emit_event(
                        WorkflowEvent(
                            workflow_id=self.dag.workflow_id,
                            workflow_name=self.dag.workflow_name,
                            node_id=node.id,
                            event_type=WorkflowEventType.NODE_FAILED,
                            attempt=node.attempt,
                            payload={"detail": node.last_error},
                            error=node.last_error,
                        )
                    )
                    await self.dag.block_dependents(node.id)


class MonitorBaseHandler(EventHandler, ABC):
    _monitor: "GlobalTimeoutMonitor"
    def __init__(self, monitor: "GlobalTimeoutMonitor"):
        self._monitor = monitor


class NodeStartEventMonitorHandler(MonitorBaseHandler):
    _monitor: "GlobalTimeoutMonitor"

    async def handle(self, event: WorkflowEvent) -> None:
        await self._monitor.watch_task(event)


class NodeEndEventMonitorHandler(MonitorBaseHandler):
    _monitor: "GlobalTimeoutMonitor"

    async def handle(self, event: WorkflowEvent) -> None:
        await self._monitor.forget_task(event.node_id, event.workflow_id)


class GlobalTimeoutMonitor:
    """
    Handles authoritative timeouts for DAG nodes.

    Responsibilities:
    - Periodically check RUNNING nodes with deadlines.
    - Apply retry or failure on timeout.
    - Use IdempotencyStore to ensure exactly-once timeout handling.
    - Emit events through orchestrator callback.
    - Safe for multiple orchestrators (if running in HA mode with shared Redis).
    """

    def __init__(
        self,
        idempotency_store: IdempotencyStore,
        event_bus: EventBus,
        check_interval: float = 1.0,
    ):
        """
        Args:
            idempotency_store: Shared IdempotencyStore implementation.
            event_bus: event emitter.
            check_interval: Timeout polling frequency in seconds.
        """
        self._started_nodes: dict[str, WorkflowEvent] = {}
        self.idempotency_store = idempotency_store
        self.event_bus = event_bus
        self.event_bus.subscribe(WorkflowEventType.NODE_STARTED, NodeStartEventMonitorHandler(self))
        self.event_bus.subscribe(WorkflowEventType.NODE_COMPLETED, NodeEndEventMonitorHandler(self))
        self.event_bus.subscribe(WorkflowEventType.NODE_FAILED, NodeEndEventMonitorHandler(self))
        self.check_interval = check_interval

        self._task: asyncio.Task | None = None
        self._stopped = False
        self._lock = asyncio.Lock()

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _emit_event(self, event: WorkflowEvent):
        await self.event_bus.publish(event)

    async def _run(self):
        try:
            while not self._stopped:
                await self._check_timeouts()
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(f"TimeoutMonitor error: {exc}", exc_info=True)

    async def watch_task(self, event: WorkflowEvent):
        if event.expire_at is not None and math.ceil(event.expire_at - event.timestamp) > 0:
            logger.debug(f"Start watching task {event.node_id} {event.workflow_id}")
            self._started_nodes[f"{event.node_id}:{event.workflow_id}"] = event

    async def forget_task(self, node_id: str, workflow_id: str):
        key = f"{node_id}:{workflow_id}"
        if key in self._started_nodes:
            logger.debug(f"Stop watching task {node_id} {self._started_nodes[key].workflow_id}")
            del self._started_nodes[key]

    async def _check_timeouts(self):
        now = time.time()
        overdue: list[WorkflowEvent] = []

        # First collect candidate nodes
        async with self._lock:
            for node_id, val in self._started_nodes.items():
                if now > val.expire_at:
                    logger.warning(f"Node {node_id} is timed out.")
                    overdue.append(val)

        for node in overdue:
            # Idempotency: ensure timeout once per attempt
            tkey = f"timeout:{node.workflow_id}:{node.node_id}:{node.attempt}"
            ttl = math.ceil(node.expire_at - node.timestamp)
            ok = await self.idempotency_store.set_if_absent(tkey, ttl_seconds=ttl)
            if not ok:
                continue  # already processed elsewhere

            await self.forget_task(node.node_id, node.workflow_id)
            await self._emit_event(
                WorkflowEvent(
                    workflow_id=node.workflow_id,
                    workflow_name=node.workflow_name,
                    node_id=node.node_id,
                    event_type=WorkflowEventType.NODE_TIMEOUT,
                    attempt=node.attempt,
                    error="TIMEOUT",
                    expire_at=now + ttl
                )
            )
