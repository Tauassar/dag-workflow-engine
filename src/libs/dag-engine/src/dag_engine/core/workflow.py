from __future__ import annotations

import typing as t
import asyncio, time
import uuid

from .constants import NodeStatus
from .exceptions import DagValidationError
from .schemas import (
    WorkflowDefinition,
)
from .entities import DagNode
from dag_engine.store import InMemoryEventStore, EventStore
from dag_engine.event_sourcing.constants import WorkflowEventType
from dag_engine.event_sourcing.schemas import WorkflowEvent


class NodeCtx(t.TypedDict, total=False):
    workflow_name: str
    node_id: str
    node_def: t.Any
    dag_ref: WorkflowDAG
    attempt: int
    payload: t.Any


Handler = t.Callable[[NodeCtx], t.Awaitable[t.Any]]


class WorkflowValidatorDAG:
    def __init__(self, dag: WorkflowDAG):
        self.dag = dag

    def validate(self):
        # check cycles (Kahn)
        indeg = {nid: len(n.depends_on) for nid, n in self.dag._nodes.items()}
        q = [nid for nid, d in indeg.items() if d == 0]
        seen = 0
        while q:
            x = q.pop()
            seen += 1
            for c in self.dag._nodes[x].dependents:
                indeg[c] -= 1
                if indeg[c] == 0:
                    q.append(c)
        if seen != len(self.dag._nodes):
            raise DagValidationError("cycle detected")


class WorkflowDAG:
    def __init__(self, definition: WorkflowDefinition, event_store: EventStore | None = None, concurrency: int = 4, validator: type[WorkflowValidatorDAG] = WorkflowValidatorDAG) -> None:
        if event_store is None:
            event_store = InMemoryEventStore()

        self.workflow_id = str(uuid.uuid4())
        self.event_store = event_store
        self.workflow_name = definition.name
        self._nodes = {}

        # try to construct DAG workflow
        for nd in definition.dag.nodes:
            node = DagNode(id=nd.id, type=nd.type, config=nd.config, depends_on=set(nd.depends_on))
            self._nodes[node.id] = node

        for n in self._nodes.values():
            try:
                for d in n.depends_on:
                    self._nodes[d].dependents.add(n.id)
            except KeyError as exc:
                raise DagValidationError(f"Undefined node with id '{n.id}'") from exc

        self._handlers_by_type = {}
        self.concurrency = concurrency
        self._ready_q = asyncio.Queue()
        self._inflight = set()
        self._lock = asyncio.Lock()

        # check cycles (Kahn)
        validator = validator(self)
        validator.validate()

    @classmethod
    def from_dict(cls, input_dict: dict, *args: t.Any, **kwargs: t.Any) -> t.Self:
        definition = WorkflowDefinition.model_validate(input_dict, by_alias=True)
        return cls(definition=definition, *args, **kwargs)

    def _register_handler(self, node_type: str, handler: Handler) -> None:
        if not asyncio.iscoroutinefunction(handler):
            raise ValueError("handler must be async")
        self._handlers_by_type[node_type] = handler

    def handler(self, node_type: str):
        def decorator(func: Handler):
            self._register_handler(node_type, func)
            return func
        return decorator

    def get_handler_for_node(self, node: DagNode) -> Handler:
        return self._handlers_by_type[node.type]

    def _all_deps_ok(self, node: DagNode) -> bool:
        return all(self._nodes[d].status == NodeStatus.SUCCESS for d in node.depends_on)

    async def execute(self, initial_payload=None) -> dict[t.Any, dict[str, t.Any] | t.Any]:
        # seed
        for nid, n in self._nodes.items():
            if n.status == NodeStatus.PENDING and not n.depends_on:
                await self._ready_q.put(nid)

        sem = asyncio.Semaphore(self.concurrency)

        async def worker(worker_id: int):
            while True:
                try:
                    nid = await asyncio.wait_for(self._ready_q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    async with self._lock:
                        if not self._inflight and self._ready_q.empty():
                            break
                    continue
                async with sem:
                    await self._run_node(nid, initial_payload)

        workers = [asyncio.create_task(worker(i)) for i in range(self.concurrency)]
        try:
            await asyncio.gather(*workers)
        finally:
            for tt in workers:
                if not tt.done():
                    tt.cancel()

        return {nid: (node.result if node.status == NodeStatus.SUCCESS else {"status": node.status.value}) for nid, node in self._nodes.items()}

    async def _emit(self, event: WorkflowEvent) -> None:
        await self.event_store.append(event)

    async def _handle_failure(self, node: DagNode, exc: Exception):
        retry_policy = node.retry_policy
        if retry_policy and node.attempt < retry_policy.max_attempts:
            backoff = retry_policy.backoff_for_attempt(node.attempt)

            # emit retry
            await self._emit(WorkflowEvent(
                workflow_id=self.workflow_id,
                workflow_name=self.workflow_name,
                node_id=node.id,
                event_type=WorkflowEventType.NODE_RETRY,
                attempt=node.attempt,
                payload={"error": repr(exc)},
            ))

            async with self._lock:
                node.status = NodeStatus.PENDING
                node.last_error = exc
                self._inflight.discard(node.id)

            # requeue after backoff
            asyncio.create_task(self._schedule_retry(node.id, backoff))
        else:
            await self._mark_failed(node, exc)

    async def _schedule_retry(self, nid: str, delay: float):
        await asyncio.sleep(delay)
        async with self._lock:
            node = self._nodes[nid]
            if node.status == NodeStatus.PENDING and self._all_deps_ok(node):
                await self._ready_q.put(nid)

    async def _mark_failed(self, node: DagNode, exc: Exception):
        async with self._lock:
            node.status = NodeStatus.FAILED
            node.last_error = exc
            node.finished_at = time.time()
            self._inflight.discard(node.id)

        # emit event
        await self._emit(WorkflowEvent(
            workflow_id=self.workflow_id,
            workflow_name=self.workflow_name,
            node_id=node.id,
            event_type=WorkflowEventType.NODE_FAILED,
            attempt=node.attempt,
            payload={"error": repr(exc)},
        ))

        # mark dependents skipped
        for d in node.dependents:
            dep = self._nodes[d]
            if dep.status == NodeStatus.PENDING:
                dep.status = NodeStatus.SKIPPED
                await self._emit(WorkflowEvent(
                    workflow_id=self.workflow_id,
                    workflow_name=self.workflow_name,
                    node_id=dep.id,
                    event_type=WorkflowEventType.NODE_SKIPPED,
                    attempt=0,
                ))

    async def _mark_success(self, node: DagNode, result: t.Any):
        async with self._lock:
            node.status = NodeStatus.SUCCESS
            node.result = result
            node.finished_at = time.time()
            self._inflight.discard(node.id)

        await self._emit(WorkflowEvent(
            workflow_id=self.workflow_id,
            workflow_name=self.workflow_name,
            node_id=node.id,
            event_type=WorkflowEventType.NODE_SUCCEEDED,
            attempt=node.attempt,
            payload={"result": result},
        ))

        # schedule dependents
        for d in node.dependents:
            dep = self._nodes[d]
            if dep.status == NodeStatus.PENDING and self._all_deps_ok(dep):
                await self._ready_q.put(d)

    async def _run_node(self, nid: str, payload) -> None:
        node = self._nodes[nid]
        async with self._lock:
            if node.status != NodeStatus.PENDING:
                return
            node.status = NodeStatus.RUNNING
            node.attempt += 1
            node.started_at = time.time()
            self._inflight.add(nid)

        await self._emit(
            WorkflowEvent(
                workflow_id=self.workflow_id,
                workflow_name=self.workflow_name,
                node_id=node.id,
                event_type=WorkflowEventType.NODE_STARTED,
                attempt=node.attempt,
                payload=None,
            )
        )

        handler = self.get_handler_for_node(node)
        ctx = NodeCtx(
            workflow_name=self.workflow_name,
            node_id=nid,
            node_def=node,
            dag_ref=self,
            attempt=node.attempt,
            payload=payload,
        )
        try:
            if node.timeout_seconds:
                coro = handler(ctx)
                result = await asyncio.wait_for(coro, timeout=node.timeout_seconds)
            else:
                result = await handler(ctx)

        except Exception as exc:
            await self._handle_failure(node, exc)
            return

        await self._mark_success(node, result)
