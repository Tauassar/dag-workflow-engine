import asyncio
import logging
import time
import typing as t

from .entities import DagNode
from .constants import NodeStatus
from .exceptions import MissingDependencyError, TemplateResolutionError
from .templates import TemplateResolver
from .timeout_monitor import TimeoutMonitor
from .workflow import WorkflowDAG
from dag_engine.event_sourcing import WorkflowEvent, WorkflowEventType
from dag_engine.event_store import EventStore
from dag_engine.transport import Transport, TaskMessage, ResultMessage, ResultType, InMemoryTransport
from dag_engine.result_store import ResultStore
from dag_engine.idempotency_store import IdempotencyStore

logger = logging.getLogger(__name__)


class DagOrchestrator:
    """
    DagOrchestrator:
        - Owns and mutates the WorkflowDAG state
        - Listens for ResultMessage from workers via Transport
        - Publishes TaskMessage for runnable nodes
        - Handles retries, backoff, and scheduling
        - Emits events to EventStore
        - Persists node results to ResultStore
    """

    def __init__(
        self,
        dag: WorkflowDAG,
        transport: Transport,
        idempotency_store: IdempotencyStore,
        event_store: EventStore | None = None,
        result_store: ResultStore | None = None,
        idempotency_ttl_seconds: int | None = None,
        result_ttl_seconds: int | None = None,
        controller_id: str = "dag-service",
        timeout_check_interval=1.0,
        on_complete: t.Callable[[str], t.Awaitable[None]] | None = None,
    ):
        self.dag = dag
        self.transport = transport
        self.idempotency_store = idempotency_store
        self.idempotency_ttl_seconds = idempotency_ttl_seconds
        self.event_store = event_store
        self.result_store = result_store
        self.result_ttl = result_ttl_seconds
        self.controller_id = controller_id
        self.on_complete = on_complete

        self.timeout_monitor = TimeoutMonitor(
            dag=self.dag,
            idempotency_store=self.idempotency_store,
            event_handler=self._emit_event,
            check_interval=timeout_check_interval,
            dispatch_retry_callback=self._dispatch_retry,
        )

        self._lock = asyncio.Lock()
        self._result_task: asyncio.Task | None = None

        # template resolver uses an async result provider
        self.template_resolver = TemplateResolver(
            result_provider=self._async_read_node_result_value
        )

    def _is_finished(self) -> bool:
        """
        Workflow is terminal if all nodes are in SUCCESS, FAILED, or BLOCKED.
        """
        for node in self.dag.nodes.values():
            if node.status in (NodeStatus.RUNNING, NodeStatus.PENDING):
                failed_parents = [
                    p for p in node.depends_on
                    if self.dag.nodes[p].status == NodeStatus.FAILED
                ]
                if failed_parents:
                    node.status = NodeStatus.FAILED
                    node.blocked_by = failed_parents
                else:
                    return False
        return True

    async def _async_read_node_result_value(self, workflow_id: str, node_id: str) -> t.Any:
        if self.result_store:
            stored = await self.result_store.get_result(workflow_id, node_id)
            return stored["result"] if stored else None
        return self.dag.nodes[node_id].result

    async def _emit_event(self, event: WorkflowEvent) -> None:
        if self.event_store:
            await self.event_store.append(event)

    async def already_processed(self, node: DagNode) -> bool:
        dispatch_key = f"dispatch:{self.dag.workflow_id}:{node.id}:{node.attempt}"
        ok = await self.idempotency_store.set_if_absent(dispatch_key, ttl_seconds=self.idempotency_ttl_seconds)
        if not ok:
            return True
        return False

    async def _publish_task(self, node: DagNode) -> None:
        if await self.already_processed(node):
            return

        node.attempt += 1

        try:
            resolved_config = await self.template_resolver.resolve(self.dag.workflow_id, node.config)
        except (MissingDependencyError, TemplateResolutionError) as e:
            logger.error(f"Failed to resolve template, finishing execution: {e}", exc_info=True)
            node.status = NodeStatus.FAILED
            node.last_error = e
            node.finished_at = time.time()
            await self._emit_event(
                WorkflowEvent(
                    workflow_id=self.dag.workflow_id,
                    workflow_name=self.dag.workflow_name,
                    node_id=node.id,
                    event_type=WorkflowEventType.NODE_FAILED,
                    attempt=node.attempt,
                    payload={"error": e},
                )
            )
            await self.stop()
            raise

        node.status = NodeStatus.RUNNING
        node.started_at = time.time()
        node.deadline_at = (
            node.started_at + node.timeout_seconds
            if node.timeout_seconds
            else None
        )

        await self._emit_event(
            WorkflowEvent(
                workflow_id=self.dag.workflow_id,
                workflow_name=self.dag.workflow_name,
                node_id=node.id,
                event_type=WorkflowEventType.NODE_STARTED,
                attempt=node.attempt,
            )
        )
        task = TaskMessage(
            workflow_id=self.dag.workflow_id,
            workflow_name=self.dag.workflow_name,
            node_id=node.id,
            node_type=node.type,
            attempt=node.attempt,
            config=resolved_config,
        )
        await self.transport.publish_task(task)

    async def _dispatch_retry(self, node_id: str):
        """
        Called by TimeoutMonitor when a retry attempt must be dispatched.
        """
        node = self.dag.nodes[node_id]

        # Check dependencies still valid
        if not all(self.dag.nodes[d].status == NodeStatus.COMPLETED for d in node.depends_on):
            await self._check_complete()
            return  # node now blocked or invalid

        await self._publish_task(node)

    # ---------------------------------------------------------
    # Start execution (seed root nodes + start result listener)
    # ---------------------------------------------------------
    async def start(self) -> None:
        """
        Seeds all root nodes (no dependencies) by publishing TaskMessage.
        Starts background result-processing loop.
        """
        async with self._lock:
            for _, node in self.dag.nodes.items():
                if not node.depends_on and node.status == NodeStatus.PENDING:
                    await self._publish_task(node)

        # Start asynchronous loop for results
        self._result_task = asyncio.create_task(self._result_loop())
        await self.timeout_monitor.start()

    # ---------------------------------------------------------
    # Result listener loop
    # ---------------------------------------------------------
    async def _result_loop(self) -> None:
        async for result in self.transport.subscribe_results():
            await self._handle_result(result)

    async def _check_complete(self):
        # check completion
        if self._is_finished():
            if self.on_complete:
                await self.on_complete(self.dag.workflow_id)

    # ---------------------------------------------------------
    # Core: Handling ResultMessage sent by workers
    # ---------------------------------------------------------
    async def _handle_result(self, res: ResultMessage) -> None:
        if res.workflow_id != self.dag.workflow_id:
            # Ignore results belonging to a different workflow
            return

        async with self._lock:
            node = self.dag.nodes.get(res.node_id)
            if node is None or node.status != NodeStatus.RUNNING:
                logger.debug(
                    f"Failed node {res.node_id} received result {res.payload}, discarding it"
                )
                return

            # Ignore stale results: attempt mismatch (stale response)
            if res.attempt != node.attempt:
                # ignore stale/late result
                logger.debug(
                    f"Node {res.node_id}, received stale result {res.payload}"
                )
                return

            logger.debug(f"Received result for node {res.node_id}, attempt {res.attempt}, node attempt {node.attempt}")

            if res.type == ResultType.COMPLETED:
                await self._handle_success(node_id=res.node_id, res=res)
            else:
                await self._handle_failure(node_id=res.node_id, res=res)

            await self._check_complete()

    # ---------------------------------------------------------
    # SUCCESS HANDLING
    # ---------------------------------------------------------
    async def _handle_success(self, node_id: str, res: ResultMessage) -> None:
        node = self.dag.nodes[node_id]

        # If the worker returned a pointer (common with ResultStore)
        payload = res.payload
        loaded_result = None

        if isinstance(payload, dict) and "result_key" in payload:
            # Worker stored result in persistent storage, so we fetch it
            if self.result_store:
                stored = await self.result_store.get_result(res.workflow_id, node_id)
                if stored:
                    loaded_result = stored["result"]
        else:
            # Inline result (simple JSON payload)
            loaded_result = payload

            # Persist inline result if result_store is enabled
            if self.result_store:
                await self.result_store.save_result(
                    workflow_id=res.workflow_id,
                    node_id=node_id,
                    attempt=res.attempt,
                    result=loaded_result,
                    ttl_seconds=self.result_ttl,
                )

        # Update DAG
        node.status = NodeStatus.COMPLETED
        node.result = loaded_result
        node.finished_at = res.timestamp

        await self._emit_event(
            WorkflowEvent(
                workflow_id=res.workflow_id,
                workflow_name=self.dag.workflow_name,
                node_id=node_id,
                event_type=WorkflowEventType.NODE_COMPLETED,
                attempt=res.attempt,
                payload={"result": "stored" if "result_key" in (payload or {}) else loaded_result},
            )
        )

        # Schedule dependents
        for dep_id in node.dependents:
            dep = self.dag.nodes[dep_id]
            if dep.status == NodeStatus.PENDING and all(
                self.dag.nodes[d].status == NodeStatus.COMPLETED for d in dep.depends_on
            ):
                await self._publish_task(dep)


    # ---------------------------------------------------------
    # FAILURE HANDLING
    # ---------------------------------------------------------
    async def _handle_failure(self, node_id: str, res: ResultMessage) -> None:
        node = self.dag.nodes[node_id]

        # Save error into result store if configured
        if self.result_store:
            await self.result_store.save_result(
                res.workflow_id,
                node_id,
                res.attempt,
                {"error": res.error},
                ttl_seconds=self.result_ttl,
            )

        # Check retry policy
        retry_policy = node.retry_policy
        if retry_policy and node.attempt < retry_policy.max_attempts:
            await self._emit_event(
                WorkflowEvent(
                    workflow_id=res.workflow_id,
                    workflow_name=self.dag.workflow_name,
                    node_id=node_id,
                    event_type=WorkflowEventType.NODE_RETRY,
                    attempt=res.attempt,
                    payload={"error": res.error},
                )
            )

            backoff = retry_policy.backoff_for_attempt(node.attempt)

            node.status = NodeStatus.PENDING
            node.last_error = res.error

            # Schedule retry in background
            asyncio.create_task(self._retry_later(node_id, backoff))
            return

        # Permanent failure
        await self.dag.block_dependents(node.id)
        node.status = NodeStatus.FAILED
        node.last_error = res.error
        node.finished_at = res.timestamp

        await self._emit_event(
            WorkflowEvent(
                workflow_id=res.workflow_id,
                workflow_name=self.dag.workflow_name,
                node_id=node_id,
                event_type=WorkflowEventType.NODE_FAILED,
                attempt=res.attempt,
                payload={"error": res.error},
            )
        )
        await self._check_complete()

    # ---------------------------------------------------------
    # Retry logic (delayed republish)
    # ---------------------------------------------------------
    async def _retry_later(self, node_id: str, delay: float) -> None:
        await asyncio.sleep(delay)

        async with self._lock:
            node = self.dag.nodes[node_id]

            # Retry only if dependencies still satisfied and node still pending
            if node.status == NodeStatus.PENDING and all(
                self.dag.nodes[d].status == NodeStatus.COMPLETED for d in node.depends_on
            ):
                await self._publish_task(node)

    # ---------------------------------------------------------
    # Wait for DAG to finish
    # ---------------------------------------------------------
    async def wait_until_finished(self, poll_interval: float = 0.05) -> None:
        """
        Returns when all nodes are:
            COMPLETED,
            FAILED,
            or permanently BLOCKED by failed dependencies.
        """
        while True:
            async with self._lock:
                done = True
                for node in self.dag.nodes.values():
                    if node.status in (NodeStatus.COMPLETED, NodeStatus.FAILED):
                        continue
                    if node.status == NodeStatus.PENDING and any(
                        self.dag.nodes[d].status == NodeStatus.FAILED for d in node.depends_on
                    ):
                        continue
                    done = False
                    break

            if done:
                return

            await asyncio.sleep(poll_interval)

    # ---------------------------------------------------------
    # Gracefully stop result subscription
    # ---------------------------------------------------------
    async def stop(self) -> None:
        """
        Stop the DagOrchestrator's background result loop.
        For RedisTransport, this is triggered by closing result stream.
        """
        if hasattr(self.transport, "close_results"):
            await self.transport.close_results()

        if self._result_task:
            await self._result_task

        await self.timeout_monitor.stop()

    # ---------------------------------------------------------
    # Collect summary of results for API/UI consumption
    # ---------------------------------------------------------
    def collect_results(self) -> dict[str, t.Any]:
        results = {}

        for node_id, node in self.dag.nodes.items():
            if node.status == NodeStatus.COMPLETED:
                results[node_id] = {
                    "status": "COMPLETED",
                    "result": node.result,
                    "attempt": node.attempt,
                }
            elif node.status == NodeStatus.FAILED:
                results[node_id] = {
                    "status": "FAILED",
                    "error": node.last_error,
                    "attempt": node.attempt,
                }
            else:
                results[node_id] = {"status": node.status.value}

        return results