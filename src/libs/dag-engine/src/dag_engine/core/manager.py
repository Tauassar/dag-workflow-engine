from __future__ import annotations

import asyncio
import logging
import time
import typing as t

from dag_engine.event_sourcing import EventHandler, WorkflowEvent, WorkflowEventType

from .constants import WorkflowStatus
from .exceptions import DefinitionNotFoundError
from .orchestrator import EventDrivenDagOrchestrator
from .workflow import WorkflowDAG

if t.TYPE_CHECKING:
    from dag_engine.event_sourcing.bus import EventBus
    from dag_engine.event_sourcing.store import EventStore
    from dag_engine.store.atomic_counter.protocol import DependencyCounterStore
    from dag_engine.store.execution import WorkflowExecutionStore
    from dag_engine.store.idempotency import IdempotencyStore
    from dag_engine.store.results import ResultStore

    from .workflow import WorkflowDefinition


logger = logging.getLogger(__name__)


class WorkflowInfo:
    """
    Tracks the runtime state of a workflow execution.
    """

    def __init__(
        self,
        workflow_id: str,
        dag: WorkflowDAG,
        service: EventDrivenDagOrchestrator,
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
        asyncio.create_task(self.manager.on_node_complete(event))


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

    def __init__(
        self,
        event_bus: EventBus,
        result_store: ResultStore,
        execution_store: WorkflowExecutionStore,
        idempotency_store: IdempotencyStore,
        atomic_counter: DependencyCounterStore,
        event_store: EventStore,
    ):
        self.result_store = result_store
        self.execution_store = execution_store
        self.idempotency_store = idempotency_store
        self.atomic_counter = atomic_counter

        eh = ManagerEventHandler(self)
        event_bus.subscribe(WorkflowEventType.NODE_COMPLETED, eh)
        event_bus.subscribe(WorkflowEventType.NODE_FAILED, eh)
        event_bus.subscribe(WorkflowEventType.NODE_TIMEOUT, eh)
        self.event_bus = event_bus
        self.event_store = event_store

    async def _get_orchestrator(self, workflow_id: str) -> EventDrivenDagOrchestrator:
        events = await self.event_store.list_events(workflow_id)
        meta = await self.execution_store.load_metadata(workflow_id)

        if not meta:
            raise DefinitionNotFoundError("Could not find definition in execution store")

        definition = meta["definition"]
        dag = WorkflowDAG.from_dict(definition, workflow_id=workflow_id, event_bus=self.event_bus)

        orchestrator = EventDrivenDagOrchestrator(
            dag=dag,
            result_store=self.result_store,
            idempotency_store=self.idempotency_store,
            event_bus=self.event_bus,
            atomic_counter=self.atomic_counter,
        )
        for _event in events:
            await orchestrator.apply_event(_event)

        return orchestrator

    async def _on_workflow_complete(self, workflow_id: str):
        """
        Called by EventDrivenDagOrchestrator when DAG reaches terminal state.
        """
        orchestrator = await self._get_orchestrator(workflow_id)

        summary = orchestrator.collect_results()
        info = WorkflowInfo(
            workflow_id=workflow_id,
            dag=orchestrator.dag,
            service=orchestrator,
        )

        if any(v["status"] == WorkflowStatus.FAILED for v in summary.values()):
            info.status = WorkflowStatus.FAILED
        else:
            info.status = WorkflowStatus.COMPLETED

        info.completed_at = time.time()

        meta = await self.execution_store.load_metadata(workflow_id)
        if not meta:
            return

        await self.execution_store.save_metadata(
            workflow_id,
            {
                "workflow_id": meta["workflow_id"],
                "status": info.status,
                "created_at": meta["created_at"],
                "completed_at": info.completed_at,
                "error": info.error,
                "definition": meta["definition"],
            },
        )
        await self.execution_store.save_results(workflow_id, summary)

    async def on_node_complete(self, event: WorkflowEvent) -> None:
        orchestrator = await self._get_orchestrator(event.workflow_id)
        logger.info(f"Node {event.node_id} completed event received: %s", event)
        await orchestrator.handle_event(event)

        if orchestrator.is_finished():
            await self._on_workflow_complete(event.workflow_id)
            logger.info(f"Workflow {event.workflow_id} completed")
            await orchestrator.stop()
        else:
            logger.info(f"Workflow {event.workflow_id} execution continue")

    async def start_workflow(self, workflow_id: str, definition: WorkflowDefinition) -> WorkflowInfo:
        """
        Starts a new workflow execution and registers it.
        """
        dag = WorkflowDAG.from_definition(definition, workflow_id=workflow_id, event_bus=self.event_bus)
        service = EventDrivenDagOrchestrator(
            dag=dag,
            result_store=self.result_store,
            idempotency_store=self.idempotency_store,
            event_bus=self.event_bus,
            atomic_counter=self.atomic_counter,
        )

        info = WorkflowInfo(
            workflow_id=workflow_id,
            dag=dag,
            service=service,
        )

        await self.execution_store.save_metadata(
            workflow_id,
            {
                "workflow_id": workflow_id,
                "status": info.status,
                "created_at": info.created_at,
                "completed_at": info.completed_at,
                "error": info.error,
                "definition": definition.model_dump(mode="json", by_alias=True),
            },
        )

        try:
            await service.start()
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
        return await self._load_persisted_status(workflow_id)

    async def get_results(self, workflow_id: str) -> dict[str, t.Any]:
        info = await self._load_persisted_status(workflow_id)
        return {
            "workflow_id": workflow_id,
            "state": info["state"],
            "nodes": await self.execution_store.load_results(workflow_id),
        }
