from __future__ import annotations

import typing as t
import asyncio, time

from .constants import NodeStatus
from .exceptions import DagValidationError
from .schemas import (
    WorkflowDefinition,
)
from .entities import DagNode


class NodeCtx(t.TypedDict, total=False):
    workflow_name: str
    node_id: str
    node_def: t.Any
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
    def __init__(self, definition: WorkflowDefinition, concurrency: int = 4, validator: type[WorkflowValidatorDAG] = WorkflowValidatorDAG) -> None:
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
        self._handlers_by_id = {}
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
        if node.id in self._handlers_by_id:
            return self._handlers_by_id[node.id]
        if node.type in self._handlers_by_type:
            return self._handlers_by_type[node.type]
        raise KeyError(f"no handler for node {node.id}")

    def _all_deps_succeeded(self, node: DagNode) -> bool:
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
            for t in workers:
                if not t.done():
                    t.cancel()
        # results
        return {nid: (node.result if node.status == NodeStatus.SUCCESS else {"status": node.status.value}) for nid, node in self._nodes.items()}

    async def _run_node(self, nid: str, payload) -> None:
        node = self._nodes[nid]
        async with self._lock:
            if node.status != NodeStatus.PENDING:
                return
            node.status = NodeStatus.RUNNING
            node.attempt += 1
            node.started_at = time.time()
            self._inflight.add(nid)

        try:
            handler = self.get_handler_for_node(node)
        except Exception as e:
            async with self._lock:
                node.status = NodeStatus.FAILED
                node.last_error = e
                node.finished_at = time.time()
                self._inflight.discard(nid)
            # mark dependents skipped
            for d in node.dependents:
                dep = self._nodes[d]
                if dep.status == NodeStatus.PENDING:
                    dep.status = NodeStatus.SKIPPED
            return

        ctx = {
            "workflow_name": self.workflow_name,
            "node_id": nid,
            "node_def": node,
            "dag_ref": self,        # NEW — expose DAG state to handlers
            "attempt": node.attempt,
            "payload": payload,
        }
        try:
            result = await handler(t.cast(NodeCtx, ctx))
        except Exception as e:
            async with self._lock:
                node.status = NodeStatus.FAILED
                node.last_error = e
                node.finished_at = time.time()
                self._inflight.discard(nid)
            for d in node.dependents:
                dep = self._nodes[d]
                if dep.status == NodeStatus.PENDING:
                    dep.status = NodeStatus.SKIPPED
            return
        else:
            async with self._lock:
                node.status = NodeStatus.SUCCESS
                node.result = result
                node.finished_at = time.time()
                self._inflight.discard(nid)
            # enqueue dependents that are now ready
            for d in node.dependents:
                dep = self._nodes[d]
                if dep.status == NodeStatus.PENDING and self._all_deps_succeeded(dep):
                    await self._ready_q.put(d)
