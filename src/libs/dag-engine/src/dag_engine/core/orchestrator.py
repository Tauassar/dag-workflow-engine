import asyncio
import logging
import time
import typing as t

from dag_engine.event_sourcing import WorkflowEvent, WorkflowEventType
from dag_engine.event_sourcing.bus import EventBus
from dag_engine.store.atomic_counter.protocol import DependencyCounterStore
from dag_engine.store.idempotency import IdempotencyStore
from dag_engine.store.results import ResultStore

from .constants import NodeStatus
from .entities import DagNode
from .exceptions import MissingDependencyError, TemplateResolutionError
from .templates import TemplateResolver
from .workflow import WorkflowDAG

logger = logging.getLogger(__name__)


class EventDrivenDagOrchestrator:
    """
    EventDrivenDagOrchestrator:
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
        idempotency_store: IdempotencyStore,
        event_bus: EventBus,
        atomic_counter: DependencyCounterStore,
        result_store: ResultStore | None = None,
        idempotency_ttl_seconds: int | None = None,
        result_ttl_seconds: int | None = None,
    ):
        self.dag = dag
        self.idempotency_store = idempotency_store
        self.idempotency_ttl_seconds = idempotency_ttl_seconds
        self.event_bus = event_bus
        self.atomic_counter = atomic_counter
        self.result_store = result_store
        self.result_ttl = result_ttl_seconds

        self._lock = asyncio.Lock()
        self._result_task: asyncio.Task | None = None

        # template resolver uses an async result provider
        self.template_resolver = TemplateResolver(result_provider=self._async_read_node_result_value)

    def is_finished(self) -> bool:
        """
        Workflow is terminal if all nodes are in SUCCESS, FAILED, or BLOCKED.
        """
        for node in self.dag.nodes.values():
            if node.status in (NodeStatus.RUNNING, NodeStatus.PENDING):
                failed_parents = [p for p in node.depends_on if self.dag.nodes[p].status == NodeStatus.FAILED]
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
        await self.apply_event(event)
        await self.event_bus.publish(event)

    async def already_processed(self, node: DagNode) -> bool:
        dispatch_key = f"dispatch:{self.dag.workflow_id}:{node.id}:{node.attempt}"
        ok = await self.idempotency_store.set_if_absent(dispatch_key, ttl_seconds=self.idempotency_ttl_seconds)
        if not ok:
            return True
        return False

    async def _publish_task(self, node: DagNode) -> None:
        if await self.already_processed(node):
            logger.debug(f"Duplicate publish request for {node}")
            return

        node.attempt += 1

        try:
            resolved_config = await self.template_resolver.resolve(self.dag.workflow_id, node.config)
        except (MissingDependencyError, TemplateResolutionError) as e:
            logger.error(f"Failed to resolve template, finishing execution: {e}", exc_info=True)
            await self._emit_event(
                WorkflowEvent(
                    workflow_id=self.dag.workflow_id,
                    node_id=node.id,
                    event_type=WorkflowEventType.NODE_FAILED,
                    attempt=node.attempt,
                    payload={"error": e},
                    error=e,
                )
            )
            await self.stop()
            raise

        await self.atomic_counter.init_counter(
            node_id=node.id, workflow_id=self.dag.workflow_id, initial=len(node.depends_on)
        )
        await self._emit_event(
            WorkflowEvent(
                event_type=WorkflowEventType.NODE_STARTED,
                workflow_id=self.dag.workflow_id,
                node_id=node.id,
                node_type=node.type,
                attempt=node.attempt,
                expire_at=node.deadline_at,
                payload=resolved_config,
            )
        )

    async def start(self) -> str:
        """
        Seeds all root nodes (no dependencies) by publishing TaskMessage.
        Starts background result-processing loop.
        """
        async with self._lock:
            for _, node in self.dag.nodes.items():
                if not node.depends_on and node.status == NodeStatus.PENDING:
                    await self._publish_task(node)

        # Start asynchronous loop for results
        return self.dag.workflow_id

    async def handle_event(self, event: WorkflowEvent) -> None:
        if event.workflow_id != self.dag.workflow_id:
            # Ignore results belonging to a different workflow
            logger.debug(
                f"Received result for workflow {self.dag.workflow_id}, but looking for {self.dag.workflow_id}, discarding it"
            )
            return

        node = self.dag.nodes.get(event.node_id)
        if not node:
            logger.debug(f"Node not found {node}")
            return

        logger.debug(f"Node found {node}")
        if event.attempt and event.attempt != node.attempt:
            # ignore stale/late result
            logger.debug(f"Node {event.node_id}, received stale result {event.payload}")
            return

        logger.debug(
            f"Received result for node {event.node_id}:{event.workflow_id}, attempt {event.attempt}, node attempt {node.attempt}"
        )

        if event.event_type == WorkflowEventType.NODE_COMPLETED:
            logger.info(f"Node {event.node_id}:{event.workflow_id} completed successfully")
            await self._handle_success(event=event)
        else:
            logger.info(f"Node {event.node_id}:{event.workflow_id} failed")
            await self._handle_failure(event=event)

    async def apply_event(self, event: WorkflowEvent) -> None:
        node = self.dag.nodes[event.node_id]
        node.attempt = event.attempt

        if event.event_type == WorkflowEventType.NODE_COMPLETED:
            node = self.dag.nodes[event.node_id]
            node.status = NodeStatus.COMPLETED
            payload = event.payload
            loaded_result = None

            if isinstance(payload, dict) and "result_key" in payload:
                # Worker stored result in persistent storage, so we fetch it
                if self.result_store:
                    stored = await self.result_store.get_result(event.workflow_id, event.node_id)
                    if stored:
                        loaded_result = stored["result"]
            else:
                loaded_result = payload

            node.result = loaded_result
            node.finished_at = event.timestamp
        elif event.event_type == WorkflowEventType.NODE_FAILED:
            retry_policy = node.retry_policy
            if retry_policy and node.attempt < retry_policy.max_attempts:
                node.status = NodeStatus.PENDING
                node.last_error = event.error
            else:
                await self.dag.block_dependents(node.id)
                node.status = NodeStatus.FAILED
                node.last_error = event.error
                node.finished_at = event.timestamp

        elif event.event_type == WorkflowEventType.NODE_STARTED:
            node.status = NodeStatus.RUNNING
            node.started_at = time.time()
            node.deadline_at = node.started_at + node.timeout_seconds if node.timeout_seconds else None

        elif event.event_type == WorkflowEventType.NODE_RETRY:
            node.status = NodeStatus.PENDING
            node.last_error = event.error

    async def _handle_success(self, event: WorkflowEvent) -> None:
        payload = event.payload
        loaded_result = None

        if isinstance(payload, dict) and "result_key" in payload:
            # Worker stored result in persistent storage, so we fetch it
            if self.result_store:
                stored = await self.result_store.get_result(event.workflow_id, event.node_id)
                if stored:
                    loaded_result = stored["result"]
        else:
            loaded_result = payload

        if self.result_store:
            await self.result_store.save_result(
                workflow_id=event.workflow_id,
                node_id=event.node_id,
                attempt=event.attempt,
                result=loaded_result,
                ttl_seconds=self.result_ttl,
            )

        node = self.dag.nodes[event.node_id]
        # Schedule dependents
        for dep_id in node.dependents:
            dep = self.dag.nodes[dep_id]
            remaining = await self.atomic_counter.decrement(node_id=dep_id, workflow_id=self.dag.workflow_id)

            if remaining > 0:
                logger.debug(
                    f"Node {dep.id} is not started since {remaining} dependents did not complete {dep.depends_on} {[self.dag.nodes[d].status for d in dep.depends_on]}"
                )
            else:
                logger.debug(
                    f"Node {dep.id} is started since all of dependents {dep.depends_on} {[self.dag.nodes[d].status for d in dep.depends_on]} completed successfully"
                )
                await self._publish_task(dep)

    async def _handle_failure(self, event: WorkflowEvent) -> None:
        if self.result_store:
            await self.result_store.save_result(
                workflow_id=event.workflow_id,
                node_id=event.node_id,
                attempt=event.attempt,
                result={"error": event.error},
                ttl_seconds=self.result_ttl,
            )

        node = self.dag.nodes[event.node_id]
        # Check retry policy
        retry_policy = node.retry_policy
        if retry_policy and node.attempt < retry_policy.max_attempts:
            await self._emit_event(
                WorkflowEvent(
                    workflow_id=event.workflow_id,
                    node_id=event.node_id,
                    event_type=WorkflowEventType.NODE_RETRY,
                    attempt=event.attempt,
                    payload={"error": event.error},
                )
            )

            backoff = retry_policy.backoff_for_attempt(node.attempt)
            # Schedule retry in background
            asyncio.create_task(self._retry_later(event.node_id, backoff))

    async def _retry_later(self, node_id: str, delay: float) -> None:
        await asyncio.sleep(delay)

        async with self._lock:
            node = self.dag.nodes[node_id]

            # Retry only if dependencies still satisfied and node still pending
            if node.status == NodeStatus.PENDING and all(
                self.dag.nodes[d].status == NodeStatus.COMPLETED for d in node.depends_on
            ):
                await self._publish_task(node)

    async def stop(self) -> None:
        """
        Stop the EventDrivenDagOrchestrator's background result loop.
        For RedisTransport, this is triggered by closing result stream.
        """
        ...

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
