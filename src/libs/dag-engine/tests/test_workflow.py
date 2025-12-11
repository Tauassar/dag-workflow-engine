import pytest
import asyncio
from collections import deque
from unittest.mock import AsyncMock

from dag_engine.core.workflow import WorkflowDAG
from dag_engine.core.entities import DagNode, NodeStatus
from dag_engine.core.exceptions import DagValidationError
from dag_engine.core.schemas import WorkflowDefinition
from dag_engine.event_sourcing import WorkflowEventType, WorkflowEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_definition(nodes):
    """Utility: create WorkflowDefinition from simple dict list."""
    return WorkflowDefinition(
        name="test",
        dag={"nodes": nodes}
    )


# ---------------------------------------------------------------------------
# DAG CONSTRUCTION
# ---------------------------------------------------------------------------

def test_build_dag_simple():
    definition = make_definition([
        {"id": "a", "handler": "input", "dependencies": []},
        {"id": "b", "handler": "task", "dependencies": ["a"]},
    ])

    dag = WorkflowDAG.from_definition(workflow_id="wf1", definition=definition)

    assert set(dag.nodes.keys()) == {"a", "b"}
    assert dag.nodes["b"].depends_on == {"a"}
    assert dag.nodes["a"].dependents == {"b"}
    assert dag.nodes["b"].dependents == set()


def test_build_dag_missing_dependency():
    definition = make_definition([
        {"id": "a", "handler": "input", "dependencies": ["zzz"]},  # missing!
    ])

    with pytest.raises(DagValidationError):
        WorkflowDAG.from_definition(workflow_id="wf1", definition=definition)


# ---------------------------------------------------------------------------
# CYCLE DETECTION
# ---------------------------------------------------------------------------

def test_cycle_detection_direct():
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": ["b"]},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
    ])
    with pytest.raises(DagValidationError):
        WorkflowDAG.from_definition(workflow_id="wf1", definition=definition)


def test_cycle_detection_longer():
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": ["c"]},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
        {"id": "c", "handler": "t", "dependencies": ["b"]},
    ])
    with pytest.raises(DagValidationError):
        WorkflowDAG.from_definition(workflow_id="wf1", definition=definition)


def test_acyclic_valid():
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": []},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
        {"id": "c", "handler": "t", "dependencies": ["a"]},
        {"id": "d", "handler": "t", "dependencies": ["b", "c"]},
    ])
    WorkflowDAG.from_definition(workflow_id="ok", definition=definition)  # no exception


# ---------------------------------------------------------------------------
# CLASSMETHOD HELPERS
# ---------------------------------------------------------------------------

def test_from_definition():
    definition = make_definition([
        {"id": "a", "handler": "input", "dependencies": []},
    ])
    dag = WorkflowDAG.from_definition(workflow_id="X", definition=definition)
    assert "a" in dag.nodes


def test_from_dict():
    d = {
        "name": "demo",
        "dag": {
            "nodes": [
                {"id": "a", "handler": "x", "dependencies": []}
            ]
        }
    }
    dag = WorkflowDAG.from_dict(d, workflow_id="Y")
    assert set(dag.nodes.keys()) == {"a"}


# ---------------------------------------------------------------------------
# BLOCK DEPENDENTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_block_dependents_basic():
    """
    a → b → c
    If b fails, c must be blocked.
    """
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": []},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
        {"id": "c", "handler": "t", "dependencies": ["b"]},
    ])

    dag = WorkflowDAG.from_definition(workflow_id="wf", definition=definition)

    dag.nodes["b"].status = NodeStatus.FAILED
    await dag.block_dependents("b")

    assert dag.nodes["c"].status == NodeStatus.FAILED
    # "a" should remain unchanged
    assert dag.nodes["a"].status == NodeStatus.PENDING


@pytest.mark.asyncio
async def test_block_dependents_propagates_chain():
    """
    a → b → c → d → e
    If c fails, then d and e must be blocked.
    """
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": []},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
        {"id": "c", "handler": "t", "dependencies": ["b"]},
        {"id": "d", "handler": "t", "dependencies": ["c"]},
        {"id": "e", "handler": "t", "dependencies": ["d"]},
    ])

    dag = WorkflowDAG.from_definition(workflow_id="wf", definition=definition)
    dag.nodes["c"].status = NodeStatus.FAILED

    await dag.block_dependents("c")

    assert dag.nodes["d"].status == NodeStatus.FAILED
    assert dag.nodes["e"].status == NodeStatus.FAILED


@pytest.mark.asyncio
async def test_block_dependents_skips_completed():
    """
    Completed nodes should **not** be re-blocked.
    """
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": []},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
        {"id": "c", "handler": "t", "dependencies": ["b"]},
    ])

    dag = WorkflowDAG.from_definition(workflow_id="wf", definition=definition)

    dag.nodes["b"].status = NodeStatus.FAILED
    dag.nodes["c"].status = NodeStatus.COMPLETED  # already terminal

    await dag.block_dependents("b")

    assert dag.nodes["c"].status == NodeStatus.COMPLETED  # unchanged


@pytest.mark.asyncio
async def test_block_dependents_requires_failed_state():
    """
    If the parent is not FAILED, block_dependents does nothing.
    """
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": []},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
    ])

    dag = WorkflowDAG.from_definition(workflow_id="wf", definition=definition)
    dag.nodes["a"].status = NodeStatus.PENDING

    await dag.block_dependents("a")

    # No state changes
    assert dag.nodes["b"].status == NodeStatus.PENDING


# ---------------------------------------------------------------------------
# EVENT STORE INTEGRATION
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_block_dependents_emits_event():
    definition = make_definition([
        {"id": "a", "handler": "t", "dependencies": []},
        {"id": "b", "handler": "t", "dependencies": ["a"]},
    ])

    mock_event_store = AsyncMock()
    dag = WorkflowDAG.from_definition(workflow_id="wf", definition=definition, event_store=mock_event_store)

    dag.nodes["a"].status = NodeStatus.FAILED
    await dag.block_dependents("a")

    # b should be blocked
    assert dag.nodes["b"].status == NodeStatus.FAILED

    # event_store.append should have been called once
    assert mock_event_store.append.await_count == 1

    event_arg = mock_event_store.append.await_args[0][0]
    assert event_arg.event_type == WorkflowEventType.NODE_BLOCKED
    assert event_arg.node_id == "b"
    assert event_arg.workflow_id == "wf"
