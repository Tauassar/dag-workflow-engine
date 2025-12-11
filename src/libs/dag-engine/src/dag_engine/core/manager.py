from __future__ import annotations

import asyncio
import time
import typing as t

from .orchestrator import DagOrchestrator
from .workflow import WorkflowDAG

if t.TYPE_CHECKING:
    from dag_engine.store.events import EventStore
    from dag_engine.store.execution import WorkflowExecutionStore
    from dag_engine.store.idempotency import IdempotencyStore
    from dag_engine.store.results import ResultStore
    from dag_engine.transport import Transport

    from .workflow import WorkflowDefinition


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
        self.status: str = "RUNNING"
        self.error: str | None = None

    @property
    def is_finished(self) -> bool:
        return self.status in ("COMPLETED", "FAILED")


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
        transport: Transport,
        result_store: ResultStore,
        execution_store: WorkflowExecutionStore,
        idempotency_store: IdempotencyStore,
        event_store: EventStore | None = None,
    ):
        self.transport = transport
        self.result_store = result_store
        self.execution_store = execution_store
        self.idempotency_store = idempotency_store
        self.event_store = event_store

        self.workflows: dict[str, WorkflowInfo] = {}
        self._lock = asyncio.Lock()

    async def _on_workflow_complete(self, workflow_id: str):
        """
        Called by DagOrchestrator when DAG reaches terminal state.
        """
        async with self._lock:
            info = self.workflows.get(workflow_id)
            if not info:
                return

            summary = info.service.collect_results()

            if any(v["status"] == "FAILED" for v in summary.values()):
                info.status = "FAILED"
            else:
                info.status = "COMPLETED"

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

    async def start_workflow(self, workflow_id: str, definition: WorkflowDefinition) -> WorkflowInfo:
        """
        Starts a new workflow execution and registers it.
        """
        async with self._lock:
            if workflow_id in self.workflows:
                raise ValueError(f"Workflow {workflow_id} already exists")

            dag = WorkflowDAG.from_definition(definition, workflow_id=workflow_id, event_store=self.event_store)

            service = DagOrchestrator(
                dag=dag,
                transport=self.transport,
                result_store=self.result_store,
                idempotency_store=self.idempotency_store,
                event_store=self.event_store,
                on_complete=self._on_workflow_complete,
            )

            info = WorkflowInfo(
                workflow_id=workflow_id,
                dag=dag,
                service=service,
            )

            self.workflows[workflow_id] = info

        await service.start()
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
