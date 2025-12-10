import asyncio
import logging
import typing as t

from .constants import NodeStatus
from .workflow import WorkflowDAG
from dag_engine.event_sourcing import WorkflowEvent, WorkflowEventType
from dag_engine.event_store import EventStore
from dag_engine.transport import Transport, TaskMessage, ResultMessage, ResultType, InMemoryTransport
from dag_engine.result_store.protocol import ResultStore

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
        event_store: EventStore | None = None,
        result_store: ResultStore | None = None,
        result_ttl_seconds: int | None = None,
        controller_id: str = "dag-service",
    ):
        self.dag = dag
        self.transport = transport
        self.event_store = event_store
        self.result_store = result_store
        self.result_ttl = result_ttl_seconds
        self.controller_id = controller_id

        self._lock = asyncio.Lock()
        self._result_task: asyncio.Task | None = None

    # ---------------------------------------------------------
    # Helper: Emit event (safe even if event_store is None)
    # ---------------------------------------------------------
    async def _emit_event(self, event: WorkflowEvent) -> None:
        if self.event_store:
            await self.event_store.append(event)

    # ---------------------------------------------------------
    # Start execution (seed root nodes + start result listener)
    # ---------------------------------------------------------
    async def start(self) -> None:
        """
        Seeds all root nodes (no dependencies) by publishing TaskMessage.
        Starts background result-processing loop.
        """
        async with self._lock:
            for node_id, node in self.dag.nodes.items():
                if not node.depends_on and node.status == NodeStatus.PENDING:
                    node.attempt += 1

                    # Emit started event
                    await self._emit_event(
                        WorkflowEvent(
                            workflow_id=self.dag.workflow_id,
                            workflow_name=self.dag.workflow_name,
                            node_id=node_id,
                            event_type=WorkflowEventType.NODE_STARTED,
                            attempt=node.attempt,
                        )
                    )

                    task = TaskMessage(
                        workflow_id=self.dag.workflow_id,
                        workflow_name=self.dag.workflow_name,
                        node_id=node_id,
                        node_type=node.type,
                        attempt=node.attempt,
                        config=node.config,
                    )
                    await self.transport.publish_task(task)

        # Start asynchronous loop for results
        self._result_task = asyncio.create_task(self._result_loop())

    # ---------------------------------------------------------
    # Result listener loop
    # ---------------------------------------------------------
    async def _result_loop(self) -> None:
        async for result in self.transport.subscribe_results():
            await self._handle_result(result)

    # ---------------------------------------------------------
    # Core: Handling ResultMessage sent by workers
    # ---------------------------------------------------------
    async def _handle_result(self, res: ResultMessage) -> None:
        if res.workflow_id != self.dag.workflow_id:
            # Ignore results belonging to a different workflow
            return

        async with self._lock:
            node = self.dag.nodes.get(res.node_id)
            if node is None:
                return

            # Ignore stale results: attempt mismatch
            if res.attempt != node.attempt:
                return

            if res.type == ResultType.COMPLETED:
                await self._handle_success(node_id=res.node_id, res=res)
            else:
                await self._handle_failure(node_id=res.node_id, res=res)

    # ---------------------------------------------------------
    # SUCCESS HANDLING
    # ---------------------------------------------------------
    async def _handle_success(self, node_id: str, res: ResultMessage) -> None:
        node = self.dag.nodes[node_id]

        # If the worker returned a pointer (common with ResultStore)
        payload = res.payload
        loaded_result = None

        if isinstance(payload, dict) and "result_key" in payload:
            # Worker stored result in Redis, so we fetch it
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
                dep.attempt += 1

                await self._emit_event(
                    WorkflowEvent(
                        workflow_name=self.dag.workflow_name,
                        workflow_id=res.workflow_id,
                        node_id=dep_id,
                        event_type=WorkflowEventType.NODE_STARTED,
                        attempt=dep.attempt,
                    )
                )

                task = TaskMessage(
                    workflow_id=res.workflow_id,
                    workflow_name=self.dag.workflow_name,
                    node_id=dep_id,
                    node_type=dep.type,
                    attempt=dep.attempt,
                    config=dep.config,
                )
                await self.transport.publish_task(task)

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

        # Dependents remain PENDING (blocked)

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
                node.attempt += 1

                await self._emit_event(
                    WorkflowEvent(
                        workflow_id=self.dag.workflow_id,
                        workflow_name=self.dag.workflow_name,
                        node_id=node_id,
                        event_type=WorkflowEventType.NODE_STARTED,
                        attempt=node.attempt,
                    )
                )

                task = TaskMessage(
                    workflow_id=self.dag.workflow_id,
                    workflow_name=self.dag.workflow_name,
                    node_id=node_id,
                    node_type=node.type,
                    attempt=node.attempt,
                    config=node.config,
                )
                await self.transport.publish_task(task)

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
            elif node.status == NodeStatus.PENDING and any(
                self.dag.nodes[d].status == NodeStatus.FAILED for d in node.depends_on
            ):
                results[node_id] = {
                    "status": "BLOCKED",
                    "blocked_by": [d for d in node.depends_on if self.dag.nodes[d].status == NodeStatus.FAILED]
                }
            else:
                results[node_id] = {"status": node.status.value}

        return results