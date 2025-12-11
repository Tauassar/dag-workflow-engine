import pytest
import asyncio
from dag_engine.core.handlers import HandlerRegistry, hregistry
from dag_engine.transport import TaskMessage


@pytest.mark.asyncio
async def test_register_async_handler():
    registry = HandlerRegistry()

    @registry.handler("test_type")
    async def test_handler(msg: TaskMessage):
        return "OK"

    assert "test_type" in registry.handlers
    assert asyncio.iscoroutinefunction(registry.handlers["test_type"])


@pytest.mark.asyncio
async def test_handler_executes_successfully():
    registry = HandlerRegistry()

    @registry.handler("task")
    async def test_handler(msg: TaskMessage):
        return {"value": 42}

    handler = registry.handlers["task"]
    result = await handler(TaskMessage(
        workflow_id="wf",
        workflow_name="nm",
        node_id="nodeX",
        node_type="task",
        attempt=1,
        config={}
    ))

    assert result == {"value": 42}


def test_register_non_async_handler_raises():
    registry = HandlerRegistry()

    def not_async(_):
        return None

    with pytest.raises(ValueError):
        registry._register_handler("x", not_async)


@pytest.mark.asyncio
async def test_decorator_rejects_non_async_handler():
    registry = HandlerRegistry()

    with pytest.raises(ValueError):
        @registry.handler("bad")
        def sync_handler(msg):
            return 123


@pytest.mark.asyncio
async def test_global_registry_works():
    """Ensure global hregistry behaves like an instance."""
    @hregistry.handler("global_test")
    async def handler(msg: TaskMessage):
        return "ok"

    assert "global_test" in hregistry.handlers
    assert asyncio.iscoroutinefunction(hregistry.handlers["global_test"])
