import datetime

import pytest
import asyncio
from http import HTTPStatus
from fastapi.testclient import TestClient

from dag_engine.core import DagValidationError
from dag_engine.core import hregistry


def make_valid_definition():
    """
    Minimal workflow with 1 node.
    """
    return {
        "name": "wf",
        "dag": {
            "nodes": [
                {
                    "id": "A",
                    "handler": "noop",
                    "dependencies": [],
                    "config": {},
                }
            ]
        }
    }


def make_invalid_definition():
    """
    DAG with missing dependency -> invalid.
    """
    return {
        "name": "wf",
        "dag": {
            "nodes": [
                {
                    "id": "A",
                    "handler": "noop",
                    "dependencies": ["B"],  # Missing dependency
                    "config": {},
                }
            ]
        }
    }


@pytest.mark.asyncio
async def test_register_workflow_success(app, container):
    client = TestClient(app)

    resp = client.post("/workflow", json=make_valid_definition())
    assert resp.status_code == 200
    execution_id = resp.json()["execution_id"]
    assert (await container.definition_store.get_definition(execution_id))


def test_register_workflow_invalid_dag(app, container, monkeypatch):
    client = TestClient(app)

    # Force WorkflowDAG.from_definition to throw validation error
    import dag_engine.core.workflow
    monkeypatch.setattr(
        dag_engine.core.workflow.WorkflowDAG,
        "from_definition",
        lambda *a, **kw: (_ for _ in ()).throw(DagValidationError("invalid dag")),
    )

    resp = client.post("/workflow", json=make_invalid_definition())
    assert resp.status_code == 400
    assert "didn't pass validation" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_trigger_workflow_success(app, container):
    client = TestClient(app)

    # Save workflow definition first
    client.post("/workflow", json=make_valid_definition())
    execution_id = list(container.definition_store._db.keys())[0]

    resp = client.post(f"/workflow/trigger/{execution_id}")
    assert resp.status_code == 200

    instance_id = resp.json()["instance_id"]
    assert len(container.manager.execution_store.meta.keys()) == 1
    assert instance_id in container.manager.execution_store.meta


def test_trigger_workflow_not_found(app):
    client = TestClient(app)
    resp = client.post("/workflow/trigger/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Workflow definition not found"


@pytest.mark.asyncio
async def test_get_status_running(app, container):
    client = TestClient(app)

    container.manager.execution_store.meta["id1"] = {
        "workflow_id": "id1",
        "status": "RUNNING",
        "created_at": 1,
        "completed_at": None,
    }

    resp = client.get("/workflows/id1")
    assert resp.status_code == 200
    assert resp.json()["state"] == "RUNNING"


@pytest.mark.asyncio
async def test_get_status_not_found(app):
    client = TestClient(app)

    resp = client.get("/workflows/unknown")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Running workflow instance not found"


@pytest.mark.asyncio
async def test_get_results_running_fails(app, container):
    client = TestClient(app)

    container.manager.execution_store.meta["id3"] = {
        "workflow_id": "id3",
        "status": "RUNNING",
        "created_at": datetime.datetime.now(),
        "completed_at": datetime.datetime.now(),
        "nodes": {},
    }

    resp = client.get("/workflows/id3/results")
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Workflow not finished yet"
