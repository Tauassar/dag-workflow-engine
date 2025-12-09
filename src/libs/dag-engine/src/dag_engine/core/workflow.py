from __future__ import annotations

import typing as t
import asyncio
import uuid

from .exceptions import DagValidationError
from .schemas import (
    WorkflowDefinition,
)
from .entities import DagNode
from dag_engine.store import InMemoryEventStore, EventStore
from ..transport import TaskMessage

Handler = t.Callable[[TaskMessage], t.Awaitable[t.Any]]


class WorkflowDAG:
    def __init__(
        self,
        definition: WorkflowDefinition,
        event_store: EventStore | None = None,
        concurrency: int = 4,
    ) -> None:
        if event_store is None:
            event_store = InMemoryEventStore()
        self.event_store = event_store

        self.workflow_name = definition.name
        self.workflow_id = str(uuid.uuid4())
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

        # check cycles (Kahn)
        self._validate_acyclic()

        self._handlers_by_type = {}
        self.concurrency = max(1, concurrency)

        self._task_queue: asyncio.Queue = asyncio.Queue()
        self._inflight = set()
        self._lock = asyncio.Lock()

    @property
    def nodes(self):
        return self._nodes

    def _validate_acyclic(self):
        indeg = {nid: len(n.depends_on) for nid, n in self._nodes.items()}
        q = [nid for nid, d in indeg.items() if d == 0]
        seen = 0
        while q:
            x = q.pop()
            seen += 1
            for c in self._nodes[x].dependents:
                indeg[c] -= 1
                if indeg[c] == 0:
                    q.append(c)
        if seen != len(self._nodes):
            raise ValueError("Workflow contains cycle(s)")

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
