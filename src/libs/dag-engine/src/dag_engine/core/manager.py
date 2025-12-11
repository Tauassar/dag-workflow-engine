from __future__ import annotations

import asyncio
import time
import typing as t

from . import WorkflowDefinition
from .workflow import WorkflowDAG
from .orchestrator import DagOrchestrator
from dag_engine.transport import Transport
from dag_engine.result_store import ResultStore
from dag_engine.idempotency_store import IdempotencyStore
from ..event_store import EventStore


class WorkflowInfo:
    """
    Tracks the runtime state of a workflow execution.
    """
    def __init__(
        self,
        workflow_id: str,
        dag: WorkflowDAG,
        service: DagOrchestrator,
        created_at: float,
    ):
        self.workflow_id = workflow_id
        self.dag = dag
        self.service = service
        self.created_at = created_at
        self.completed_at: float | None = None
        self.status: str = "RUNNING"
        self.error: str | None = None

    @property
    def is_finished(self) -> bool:
        return self.status in ("COMPLETED", "FAILED", "BLOCKED")


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
        idempotency_store: IdempotencyStore,
        event_store: EventStore | None = None,
    ):
        self.transport = transport
        self.result_store = result_store
        self.idempotency_store = idempotency_store
        self.event_store = event_store
        # workflow_id → WorkflowInfo
        self.workflows: dict[str, WorkflowInfo] = {}

        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
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
                event_store=self.event_store
            )

            info = WorkflowInfo(
                workflow_id=workflow_id,
                dag=dag,
                service=service,
                created_at=time.time(),
            )

            self.workflows[workflow_id] = info

        await service.start()
        return info

    # ------------------------------------------------------------------
    async def wait_until_finished(self, workflow_id: str) -> WorkflowInfo:
        """
        Block until workflow finishes, then returns final info.
        """
        info = self.workflows.get(workflow_id)
        if not info:
            raise ValueError(f"Workflow {workflow_id} does not exist")

        try:
            await info.service.wait_until_finished()
        except Exception as exc:
            info.status = "FAILED"
            info.error = str(exc)
            info.completed_at = time.time()
            return info

        # summarize results
        summary = info.service.collect_results()

        # determine global workflow status
        if any(v["status"] == "FAILED" for v in summary.values()):
            info.status = "FAILED"
        else:
            info.status = "COMPLETED"

        info.completed_at = time.time()

        return info

    # ------------------------------------------------------------------
    def get_status(self, workflow_id: str) -> dict[str, t.Any]:
        """
        Returns current known status of workflow.
        """
        info = self.workflows.get(workflow_id)
        if not info:
            raise ValueError(f"Workflow {workflow_id} not found")

        service = info.service

        data = {
            "workflow_id": workflow_id,
            "state": info.status,
            "created_at": info.created_at,
            "completed_at": info.completed_at,
            "nodes": service.collect_results() if info.status != "RUNNING" else None,
        }

        return data
