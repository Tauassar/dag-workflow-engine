from __future__ import annotations

import logging
import time
import typing as t
import asyncio
from collections import deque

from . import NodeStatus
from .exceptions import DagValidationError
from .schemas import (
    WorkflowDefinition,
)
from .entities import DagNode
from dag_engine.event_store import EventStore
from dag_engine.event_sourcing import WorkflowEvent, WorkflowEventType


logger = logging.getLogger(__name__)


class WorkflowDAG:
    def __init__(
        self,
        workflow_id: str,
        definition: WorkflowDefinition,
        event_store: EventStore | None = None,
    ) -> None:
        self.event_store = event_store

        self.workflow_name = definition.name
        self.workflow_id = workflow_id
        self._nodes = {}

        # try to construct DAG workflow
        for nd in definition.dag.nodes:
            node = DagNode(
                id=nd.id,
                type=nd.type,
                config=nd.config,
                depends_on=set(nd.depends_on),
                timeout_seconds=nd.timeout_seconds,
                retry_policy=nd.retry_policy,
            )
            self._nodes[node.id] = node

        for n in self._nodes.values():
            try:
                for d in n.depends_on:
                    self._nodes[d].dependents.add(n.id)
            except KeyError as exc:
                raise DagValidationError(f"Undefined node with id '{n.id}'") from exc

        # check cycles (Kahn)
        self._validate_acyclic()

        self._task_queue: asyncio.Queue = asyncio.Queue()
        self._inflight = set()
        self._lock = asyncio.Lock()

    async def _emit_event(self, event: WorkflowEvent) -> None:
        if self.event_store:
            await self.event_store.append(event)

    @property
    def nodes(self) -> dict[str, DagNode]:
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
    def from_definition(cls, definition: WorkflowDefinition, *args: t.Any, **kwargs: t.Any) -> t.Self:
        return cls(definition=definition, *args, **kwargs)

    @classmethod
    def from_dict(cls, input_dict: dict, *args: t.Any, **kwargs: t.Any) -> t.Self:
        return cls.from_definition(
            definition=WorkflowDefinition.model_validate(
                input_dict,
                by_alias=True,
            ),
            *args,
            **kwargs,
        )

    async def block_dependents(self, node_id: str):
        """
        Mark all downstream nodes of `node_id` as FAILED(BLOCKED).
        A node becomes FAILED(BLOCKED) if ANY of its parents is FAILED.
        Runs bfs to propagate blocking through the DAG.
        """

        failed_node = self.nodes[node_id]
        if failed_node.status != NodeStatus.FAILED:
            return

        queue = deque([node_id])
        visited = set()

        while queue:
            current_id = queue.popleft()
            if current_id in visited:
                continue
            visited.add(current_id)

            current_node = self.nodes[current_id]

            # Process all direct dependents
            for child_id in current_node.dependents:
                child = self.nodes[child_id]

                # Skip nodes that are already terminal (SUCCESS or FAILED)
                if child.status in (NodeStatus.COMPLETED, NodeStatus.FAILED):
                    continue

                # Only block if any dependency is FAILED
                failed_parents = [
                    pid for pid in child.depends_on
                    if self.nodes[pid].status == NodeStatus.FAILED
                ]

                if failed_parents:
                    # Better to use BLOCKED status
                    logger.info(f"Marking node {child.id} as blocked since parent nodes {failed_parents} are failed")
                    child.status = NodeStatus.FAILED
                    child.finished_at = time.time()
                    child.started_at = None
                    child.deadline_at = None

                    await self._emit_event(
                        WorkflowEvent(
                            workflow_id=self.workflow_id,
                            workflow_name=self.workflow_name,
                            node_id=child.id,
                            event_type=WorkflowEventType.NODE_BLOCKED,
                            attempt=child.attempt,
                            payload={"blocked_by": failed_parents},
                        )
                    )

                    # Continue propagation: child may have dependents to block as well
                    queue.append(child_id)

                else:
                    # Parent failed but child still has other running dependencies?
                    # We do *not* block yet — it may still run if all parents eventually succeed.
                    pass
