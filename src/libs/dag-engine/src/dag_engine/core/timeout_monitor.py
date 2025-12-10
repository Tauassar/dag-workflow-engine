import asyncio
import logging
import time
import typing as t

from dag_engine.core import NodeStatus
from dag_engine.event_sourcing import WorkflowEventType, WorkflowEvent
from dag_engine.idempotency_store import IdempotencyStore


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
        event_handler: t.Callable[[...], t.Awaitable[None]] | None = None,
        check_interval: float = 1.0,
        dispatch_retry_callback: t.Callable[[str], t.Any] = None,
    ):
        """
        Args:
            dag: Shared WorkflowDAG instance.
            idempotency_store: Shared IdempotencyStore implementation.
            event_handler: Orchestrator event emitter.
            check_interval: Timeout polling frequency in seconds.
            dispatch_retry_callback:
                async callback(node_id) -> None
                Orchestrator must supply a function that triggers re-dispatch
                after a retry is detected.
        """
        self.dag = dag
        self.idempotency_store = idempotency_store
        self.event_handler = event_handler
        self.check_interval = check_interval
        self.dispatch_retry_callback = dispatch_retry_callback

        self._task: asyncio.Task | None = None
        self._stopped = False
        self._lock = asyncio.Lock()

    # ---------------------------------------
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

    # ---------------------------------------
    async def _emit_event(self, *args, **kwargs: t.Any):
        if not self.event_handler:
            return

        await self.event_handler(
            WorkflowEvent(*args, **kwargs)
        )

    # ---------------------------------------
    async def _run(self):
        try:
            while not self._stopped:
                await self._check_timeouts()
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            print("TimeoutMonitor error:", exc)

    # ---------------------------------------
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
                ok = await self.idempotency_store.set_if_absent(
                    tkey,
                    ttl_seconds=int(node.timeout_seconds or 60)
                )
                if not ok:
                    continue  # already processed elsewhere

                policy = node.retry_policy

                # Retry allowed?
                if policy and node.attempt < policy.max_attempts:
                    # Mark as PENDING, increment attempt, and schedule retry
                    logger.warning(f"Retrying node {node.id}...")
                    await self._emit_event(
                        workflow_id=self.dag.workflow_id,
                        workflow_name=self.dag.workflow_name,
                        node_id=node.id,
                        event_type=WorkflowEventType.NODE_RETRY,
                        attempt=node.attempt,
                        payload={"error": "TIMEOUT"},
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
                        workflow_id=self.dag.workflow_id,
                        workflow_name=self.dag.workflow_name,
                        node_id=node.id,
                        event_type=WorkflowEventType.NODE_FAILED,
                        attempt=node.attempt,
                        payload={"error": node.last_error},
                    )
                    await self.dag.block_dependents(node.id)
