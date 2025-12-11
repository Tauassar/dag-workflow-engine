import logging
import typing as t
import uuid

from dag_engine.core import DagValidationError, WorkflowDAG
from dag_engine.core import WorkflowDefinition as WorkflowDefinitionSchema
from dag_engine.core import WorkflowManager
from dag_service.ioc import AppContainer, get_container
from dag_service.store import WorkflowDefinitionStore
from fastapi import APIRouter, Depends, HTTPException

logger = logging.getLogger(__name__)
v1_router = APIRouter(tags=["Core API Endpoints"])


class WorkflowDefinition(WorkflowDefinitionSchema): ...


def get_definition_store(container: AppContainer = Depends(get_container)) -> WorkflowDefinitionStore:
    return container.definition_store


def get_manager(container: AppContainer = Depends(get_container)) -> WorkflowManager:
    return t.cast(WorkflowManager, container.manager)


@v1_router.post("/workflow")
async def register_workflow(
    definition: WorkflowDefinition,
    definition_store: WorkflowDefinitionStore = Depends(get_definition_store),
):
    execution_id = str(uuid.uuid4())
    try:
        WorkflowDAG.from_definition(workflow_id=str(uuid.uuid4()), definition=definition)
    except DagValidationError as e:
        raise HTTPException(400, f"Workflow didn't pass validation: {str(e).lower()}") from e

    await definition_store.save_definition(execution_id, definition)

    return {
        "execution_id": execution_id,
    }


@v1_router.post("/workflow/trigger/{execution_id}")
async def trigger_workflow(
    execution_id: str, container: AppContainer = Depends(get_container), manager: WorkflowManager = Depends(get_manager)
):
    definition = await container.definition_store.get_definition(execution_id)
    if not definition:
        raise HTTPException(status_code=404, detail="Workflow definition not found")

    # Create instance_id separate from workflow definition ID
    instance_id = str(uuid.uuid4())
    await manager.start_workflow(instance_id, WorkflowDefinition.model_validate(definition, by_alias=True))
    return {
        "instance_id": instance_id,
    }


@v1_router.get("/workflows/{instance_id}")
async def get_workflow_status(instance_id: str, manager: WorkflowManager = Depends(get_manager)):
    try:
        return await manager.get_status(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Running workflow instance not found") from exc


@v1_router.get("/workflows/{instance_id}/results")
async def get_results(instance_id: str, manager: WorkflowManager = Depends(get_manager)):
    try:
        status = await manager.get_results(instance_id)
        if status["state"] == "RUNNING":
            raise HTTPException(400, "Workflow not finished yet")

        return {
            "workflow_id": instance_id,
            "status": status["state"],
            "results": status["nodes"],
        }
    except ValueError as exc:
        raise HTTPException(404, "Workflow not found") from exc
