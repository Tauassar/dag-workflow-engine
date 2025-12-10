import asyncio
import logging
import typing as t

from .constants import NodeStatus
from .workflow import WorkflowDAG
from dag_engine.event_sourcing import WorkflowEvent, WorkflowEventType
from dag_engine.event_store import EventStore
from dag_engine.transport import Transport, TaskMessage, ResultMessage, ResultType, InMemoryTransport


logger = logging.getLogger(__name__)


class DagService:
    """
    The controller that owns the DAG and talks to workers via Transport.
    - Seeds root tasks by publishing TaskMessage to transport.
    - Subscribes to results from transport and updates DAG state.
    - Emits events to EventStore.
    """
    def __init__(self, dag: WorkflowDAG, transport: Transport, event_store: EventStore | None = None):
        self.dag = dag
        self.transport = transport
        self.event_store = event_store
        self._lock = asyncio.Lock()
        self._result_task: asyncio.Task | None = None

    async def _emit_event(self, ev: WorkflowEvent) -> None:
        if self.event_store:
            await self.event_store.append(ev)

    async def start(self) -> None:
        """Seed root tasks (those with no dependencies)."""
        async with self._lock:
            for nid, node in self.dag.nodes.items():
                if not node.depends_on and node.status == NodeStatus.PENDING:
                    # increment attempt before publishing
                    node.attempt += 1
                    task = TaskMessage(
                        workflow_name=self.dag.workflow_name,
                        workflow_id=self.dag.workflow_id,
                        node_id=nid,
                        node_type=node.type,
                        attempt=node.attempt,
                        config=node.config,
                    )
                    await self.transport.publish_task(task)

        # start listening for results
        self._result_task = asyncio.create_task(self._result_loop())

    async def _result_loop(self) -> None:
        async for res in self.transport.subscribe_results():
            # process result
            await self._handle_result(res)

    async def _handle_result(self, res: ResultMessage) -> None:
        # Only DagService touches DAG directly.
        logger.debug(f"Received result message {res}")
        async with self._lock:
            if res.workflow_id != self.dag.workflow_id:
                return
            if res.node_id not in self.dag.nodes:
                return
            node = self.dag.nodes[res.node_id]

            # if attempt mismatch (stale) ignore
            if res.attempt != node.attempt:
                # stale result (worker ran an older attempt) - ignore
                return

            if res.type == ResultType.COMPLETED:
                node.status = NodeStatus.COMPLETED
                node.result = res.payload
                node.finished_at = res.timestamp
                await self._emit_event(
                    WorkflowEvent(
                        workflow_id=self.dag.workflow_id,
                        workflow_name=self.dag.workflow_name,
                        node_id=node.id,
                        event_type=WorkflowEventType.NODE_COMPLETED,
                        attempt=node.attempt,
                        payload={"result": res.payload}
                    )
                )
                # schedule dependents that are ready
                for dep_id in node.dependents:
                    dep = self.dag.nodes[dep_id]
                    if dep.status == NodeStatus.PENDING and all(self.dag.nodes[d].status == NodeStatus.COMPLETED for d in dep.depends_on):
                        dep.attempt += 1
                        task = TaskMessage(
                            workflow_id=self.dag.workflow_id,
                            workflow_name=self.dag.workflow_name,
                            node_id=dep_id,
                            node_type=dep.type,
                            attempt=dep.attempt,
                            config=dep.config
                        )
                        await self.transport.publish_task(task)

            else:  # FAILED
                # if retry policy available and attempts remain, schedule retry
                policy = node.retry_policy
                if policy and node.attempt < policy.max_attempts:
                    # emit retry event and schedule re-publish after backoff
                    await self._emit_event(
                        WorkflowEvent(
                            workflow_id=self.dag.workflow_id,
                            workflow_name=self.dag.workflow_name,
                            node_id=node.id,
                            event_type=WorkflowEventType.NODE_RETRY,
                            attempt=node.attempt,
                            payload={"error": res.error}
                        )
                    )
                    backoff = policy.backoff_for_attempt(node.attempt)
                    # increment attempt for the planned retry
                    node.attempt += 1
                    # schedule publish after backoff without releasing lock to avoid races? we release lock and schedule task
                    async def _delayed_publish():
                        await asyncio.sleep(backoff)
                        # ensure dependencies still OK
                        async with self._lock:
                            if node.status == NodeStatus.PENDING and all(self.dag.nodes[d].status == NodeStatus.COMPLETED for d in node.depends_on):
                                task = TaskMessage(
                                    workflow_name=self.dag.workflow_name,
                                    workflow_id=self.dag.workflow_id,
                                    node_id=node.id,
                                    node_type=node.type,
                                    attempt=node.attempt,
                                    config=node.config
                                )
                                await self.transport.publish_task(task)
                    # keep node in PENDING; record last_error
                    node.status = NodeStatus.PENDING
                    node.last_error = res.error
                    asyncio.create_task(_delayed_publish())
                else:
                    node.status = NodeStatus.FAILED
                    node.last_error = res.error
                    node.finished_at = res.timestamp
                    await self._emit_event(
                        WorkflowEvent(
                            workflow_name=self.dag.workflow_name,
                            workflow_id=self.dag.workflow_id,
                            node_id=node.id,
                            event_type=WorkflowEventType.NODE_FAILED,
                            attempt=node.attempt,
                            payload={"error": res.error}
                        )
                    )
                    # dependents stay PENDING (blocked)

    async def wait_until_finished(self, poll_interval: float = 0.05) -> None:
        """Return when all nodes are COMPLETED, FAILED, or blocked by failed deps."""
        while True:
            async with self._lock:
                all_terminal = True
                for n in self.dag.nodes.values():
                    if n.status in (NodeStatus.COMPLETED, NodeStatus.FAILED):
                        continue
                    # pending but blocked by a failed dependency -> terminal
                    if n.status == NodeStatus.PENDING and any(self.dag.nodes[d].status == NodeStatus.FAILED for d in n.depends_on):
                        continue
                    all_terminal = False
                    break
            if all_terminal:
                logger.info(f"Workflow execution finished for dag {self.dag.workflow_id}.")
                return
            await asyncio.sleep(poll_interval)

    async def stop(self) -> None:
        """Stop DagService result loop by closing transport results (dev only)."""
        # close result subscription by pushing sentinel if transport supports that
        if isinstance(self.transport, InMemoryTransport):
            await self.transport.close_results()
        if self._result_task:
            await self._result_task

    def collect_results(self) -> dict[str, t.Any]:
        out: dict[str, t.Any] = {}
        for nid, n in self.dag.nodes.items():
            if n.status == NodeStatus.COMPLETED:
                out[nid] = n.result
            elif n.status == NodeStatus.FAILED:
                out[nid] = {"status": "FAILED", "error": n.last_error}
            elif n.status == NodeStatus.PENDING and any(self.dag.nodes[d].status == NodeStatus.FAILED for d in n.depends_on):
                out[nid] = {"status": "BLOCKED", "blocked_by": [d for d in n.depends_on if self.dag.nodes[d].status == NodeStatus.FAILED]}
            else:
                out[nid] = {"status": n.status.value}
        return out