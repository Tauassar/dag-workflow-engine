from __future__ import annotations

import asyncio
import logging
import time
import typing as t

from .constants import WorkflowStatus
from .orchestrator import DagOrchestrator
from .workflow import WorkflowDAG
from dag_engine.event_sourcing import WorkflowEvent, EventHandler, WorkflowEventType
from ..event_sourcing.bus import EventBus
from dag_engine.transport import ResultMessage, ResultType


if t.TYPE_CHECKING:
    from dag_engine.store.execution import WorkflowExecutionStore
    from dag_engine.store.idempotency import IdempotencyStore
    from dag_engine.store.results import ResultStore

    from .workflow import WorkflowDefinition


logger = logging.getLogger(__name__)


class EventHandler(EventHandler):
    _manager: WorkflowManager

    async def handle(self, event: WorkflowEvent) -> None:
        await self._manager.on_node_complete(event)


class WorkflowInfo:
    """
    Tracks the runtime state of a workflow execution.
    """

    def __init__(
        self,
        workflow_id: str,
        dag: WorkflowDAG,
        service: DagOrchestrator,
    ):
        self.workflow_id = workflow_id
        self.dag = dag
        self.service = service
        self.created_at = time.time()
        self.completed_at: float | None = None
        self.status: str = WorkflowStatus.RUNNING
        self.error: str | None = None

    @property
    def is_finished(self) -> bool:
        return self.status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED)


class ManagerEventHandler(EventHandler):
    def __init__(self, manager: WorkflowManager):
        self.manager = manager

    async def handle(self, event: WorkflowEvent) -> None:
        await self.manager.on_node_complete(event)


class WorkflowManager:
    """
    A top-level component that manages *multiple* concurrent workflows.

    Responsibilities:
    - Start new workflow executions
    - Maintain a registry of active workflows
    - Wait for workflow completion
    - Cleanup (result store, DAG state, services)
    - Track lifecycle events
    - Provide workflow queries
    """
    _registry: t.Dict[str, DagOrchestrator] = {}

    def __init__(
        self,
        event_bus: EventBus,
        result_store: ResultStore,
        execution_store: WorkflowExecutionStore,
        idempotency_store: IdempotencyStore,
    ):
        self.result_store = result_store
        self.execution_store = execution_store
        self.idempotency_store = idempotency_store

        self.workflows: dict[str, WorkflowInfo] = {}
        self._lock = asyncio.Lock()

        eh = ManagerEventHandler(self)
        event_bus.subscribe(WorkflowEventType.NODE_COMPLETED, eh)
        event_bus.subscribe(WorkflowEventType.NODE_FAILED, eh)
        self.event_bus = event_bus

    async def _on_workflow_complete(self, workflow_id: str):
        """
        Called by DagOrchestrator when DAG reaches terminal state.
        """
        async with self._lock:
            info = self.workflows.get(workflow_id)
            if not info:
                return

            summary = info.service.collect_results()

            if any(v["status"] == WorkflowStatus.FAILED for v in summary.values()):
                info.status = WorkflowStatus.FAILED
            else:
                info.status = WorkflowStatus.COMPLETED

            info.completed_at = time.time()

            await self.execution_store.save_metadata(
                workflow_id,
                {
                    "workflow_id": workflow_id,
                    "status": info.status,
                    "created_at": info.created_at,
                    "completed_at": info.completed_at,
                    "error": info.error,
                },
            )
            await self.execution_store.save_results(workflow_id, summary)
            self.workflows.pop(workflow_id, None)

    async def on_node_complete(self, event: WorkflowEvent) -> None:
        orchestrator = self._registry.get(event.workflow_id)
        if orchestrator:
            if event.event_type.NODE_COMPLETED:
                msg = ResultMessage(
                    workflow_name=event.workflow_name,
                    workflow_id=event.workflow_id,
                    node_id=event.node_id,
                    attempt=event.attempt,
                    type=ResultType.COMPLETED,
                    payload=event.payload,
                    error=None,
                    timestamp=event.timestamp,
                )
            else:
                msg = ResultMessage(
                    workflow_name=event.workflow_name,
                    workflow_id=event.workflow_id,
                    node_id=event.node_id,
                    attempt=event.attempt,
                    type=ResultType.FAILED,
                    payload=event.payload,
                    error=event.error,
                    timestamp=event.timestamp,
                )

            logger.info(f"Node {event.node_id} completed event received: %s", event)
            await orchestrator.handle_result(msg)

            if orchestrator.is_finished():
                await self._on_workflow_complete(event.workflow_id)
                logger.info(f"Workflow {event.workflow_id} completed")
                await orchestrator.stop()

    async def start_workflow(self, workflow_id: str, definition: WorkflowDefinition) -> WorkflowInfo:
        """
        Starts a new workflow execution and registers it.
        """
        async with self._lock:
            if workflow_id in self.workflows:
                raise ValueError(f"Workflow {workflow_id} already exists")

            dag = WorkflowDAG.from_definition(definition, workflow_id=workflow_id, event_bus=self.event_bus)

            service = DagOrchestrator(
                dag=dag,
                result_store=self.result_store,
                idempotency_store=self.idempotency_store,
                event_bus=self.event_bus,
            )

            info = WorkflowInfo(
                workflow_id=workflow_id,
                dag=dag,
                service=service,
            )

            self.workflows[workflow_id] = info

        await self.execution_store.save_metadata(
            workflow_id,
            {
                "workflow_id": workflow_id,
                "status": info.status,
                "created_at": info.created_at,
                "completed_at": info.completed_at,
                "error": info.error,
            },
        )

        try:
            self._registry[await service.start()] = service
        except Exception:
            await self._on_workflow_complete(workflow_id)
            raise

        return info

    async def _load_persisted_status(self, workflow_id: str):
        meta = await self.execution_store.load_metadata(workflow_id)
        if not meta:
            raise ValueError("Workflow not found")

        return {
            "workflow_id": workflow_id,
            "state": meta["status"],
            "created_at": meta["created_at"],
            "completed_at": meta["completed_at"],
        }

    async def get_status(self, workflow_id: str) -> dict[str, t.Any]:
        """
        Returns current known status of workflow.
        """
        info = self.workflows.get(workflow_id)
        if not info:
            # Workflow completed earlier or this is another node
            return await self._load_persisted_status(workflow_id)

        return {
            "workflow_id": workflow_id,
            "state": info.status,
            "created_at": info.created_at,
            "completed_at": info.completed_at,
        }

    async def get_results(self, workflow_id: str) -> dict[str, t.Any]:
        info = await self._load_persisted_status(workflow_id)
        return {
            "workflow_id": workflow_id,
            "state": info["state"],
            "nodes": await self.execution_store.load_results(workflow_id),
        }
