import asyncio
import logging
import time
import typing as t
import uuid
import pytest

from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

# Import your pieces (adjust import paths if needed)
from dag_service.routes.v1 import v1_router, get_container as get_container_dep, get_manager as get_manager_dep
from dag_engine.core.orchestrator import DagOrchestrator
from dag_engine.core.manager import WorkflowManager
from dag_engine.core.handlers import hregistry
from dag_engine.transport.local import InMemoryTransport
from dag_engine.transport.messages import TaskMessage, ResultMessage, ResultType

# Minimal fake stores used by the TestContainer
class FakeDefinitionStore:
    def __init__(self):
        self._db: dict[str, dict] = {}

    async def save_definition(self, wid: str, definition: dict):
        self._db[wid] = definition

    async def get_definition(self, wid: str) -> dict | None:
        return self._db.get(wid)


class FakeExecutionStore:
    def __init__(self):
        self.meta: dict[str, dict] = {}
        self.results: dict[str, dict] = {}

    async def save_metadata(self, workflow_id: str, meta: dict):
        self.meta[workflow_id] = meta

    async def load_metadata(self, workflow_id: str) -> dict | None:
        return self.meta.get(workflow_id)

    async def save_results(self, workflow_id: str, results: dict):
        self.results[workflow_id] = results

    async def load_results(self, workflow_id: str) -> dict | None:
        return self.results.get(workflow_id)


class FakeIdempotencyStore:
    def __init__(self):
        self._set = set()

    async def set_if_absent(self, key: str, ttl_seconds: int | None = None) -> bool:
        if key in self._set:
            return False
        self._set.add(key)
        return True


class FakeResultStore:
    def __init__(self):
        self._data = {}

    async def save_result(self, workflow_id: str, node_id: str, attempt: int, result: t.Any, ttl_seconds: int | None = None):
        self._data[f"{workflow_id}:{node_id}"] = {"result": result, "attempt": attempt}

    async def get_result(self, workflow_id: str, node_id: str):
        return self._data.get(f"{workflow_id}:{node_id}")

    def get_key(self, workflow_id: str, node_id: str):
        return f"res:{workflow_id}:{node_id}"


class FakeEventStore:
    def __init__(self):
        self.append_calls = []

    async def append(self, event):
        self.append_calls.append(event)


class TestContainer:
    """
    Lightweight test IoC container that uses in-memory transports and fake stores.
    """

    def __init__(self):
        self.id = uuid.uuid4().hex

        # In-memory transports shared between orchestrator and worker
        # They point at the same queues so tasks -> worker -> results flow
        self.tasks_transport = InMemoryTransport()
        self.results_transport = InMemoryTransport()

        # Stores
        self.definition_store = FakeDefinitionStore()
        self.execution_store = FakeExecutionStore()
        self.idempotency_store = FakeIdempotencyStore()
        self.result_store = FakeResultStore()
        self.event_store = FakeEventStore()

        # Manager and worker placeholders
        self.manager: WorkflowManager | None = None
        self.worker = None

    async def create_workflow_manager(self) -> WorkflowManager:
        # DagOrchestrator expects a single transport that supports both task/result streams.
        # Use a simple proxy transport that publishes tasks to tasks_transport and results to results_transport.
        class ProxyTransport:
            def __init__(self, t_tasks: InMemoryTransport, t_results: InMemoryTransport):
                self._tasks = t_tasks
                self._results = t_results

            async def publish_task(self, task: TaskMessage):
                await self._tasks.publish_task(task)

            async def publish_result(self, result: ResultMessage):
                await self._results.publish_result(result)

            async def subscribe_tasks(self):
                async for t in self._tasks.subscribe_tasks():
                    yield t

            async def subscribe_results(self):
                async for r in self._results.subscribe_results():
                    yield r

            # helper methods for orchestrator stop to call
            async def close_results(self):
                await self._results.close_results()

        transport = ProxyTransport(self.tasks_transport, self.results_transport)
        mgr = WorkflowManager(
            transport=transport,
            result_store=self.result_store,
            execution_store=self.execution_store,
            idempotency_store=self.idempotency_store,
            event_store=self.event_store,
        )
        self.manager = mgr
        return mgr

    async def create_workflow_worker(self):
        # worker consumes from tasks_transport and publishes results to results_transport
        from dag_engine.core.worker import WorkflowWorker

        # Make a proxy transport for worker (opposite direction)
        class WorkerTransport:
            def __init__(self, t_tasks: InMemoryTransport, t_results: InMemoryTransport):
                self._tasks = t_tasks
                self._results = t_results

            async def publish_task(self, task: TaskMessage):
                await self._tasks.publish_task(task)

            async def publish_result(self, result: ResultMessage):
                await self._results.publish_result(result)

            async def subscribe_tasks(self):
                async for t in self._tasks.subscribe_tasks():
                    yield t

            async def subscribe_results(self):
                async for r in self._results.subscribe_results():
                    yield r

            async def close_results(self):
                await self._results.close_results()

        transport = WorkerTransport(self.tasks_transport, self.results_transport)

        # Build worker using global handler registry (hregistry)
        w = await WorkflowWorker(
            transport=transport,
            handler_registry=hregistry.handlers,
            idempotency_store=self.idempotency_store,
            result_store=self.result_store,
            worker_id=f"test-worker-{self.id}",
        ) if False else None

        # The real WorkflowWorker is synchronous constructor; we need to call class directly
        from dag_engine.core.worker import WorkflowWorker as WW
        w = WW(
            transport=transport,
            handler_registry=hregistry.handlers,
            idempotency_store=self.idempotency_store,
            result_store=self.result_store,
            worker_id=f"test-worker-{self.id}",
        )
        self.worker = w
        return w

    async def init_for_tests(self):
        # create manager and worker objects
        await self.create_workflow_manager()
        await self.create_workflow_worker()

        # Patch DagOrchestrator._result_loop to work with our InMemoryTransport
        async def _result_loop(self):
            # subscribe_results for proxy transport does not accept wf_id
            async for result in t.cast(t.AsyncIterator[ResultMessage], self.transport.subscribe_results()):
                if getattr(self, "_stop", False):
                    break
                await self._handle_result(result)

        DagOrchestrator._result_loop = _result_loop

        # leave manager un-started; tests will call manager.start_workflow via API
        return self


@pytest.fixture(scope="session", autouse=True)
def session_setup() -> None:
    """Session setup fixture."""
    logging.basicConfig(level=logging.DEBUG)


@pytest.fixture()
async def container():
    c = TestContainer()
    await c.init_for_tests()
    return c


@pytest.fixture()
async def app(container: TestContainer):
    # Build FastAPI app and override dependencies
    app = FastAPI()
    app.include_router(v1_router)

    # override get_container and get_manager to use our test container
    async def _get_container_override():
        return container

    async def _get_manager_override():
        # manager may be None until container.init_for_tests created it
        return container.manager

    app.dependency_overrides[get_container_dep] = _get_container_override
    app.dependency_overrides[get_manager_dep] = _get_manager_override

    return app


@pytest.fixture()
async def client(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


# Helper to run a background worker for the duration of the test
@pytest.fixture()
async def run_worker(container: TestContainer):
    # start worker loop in background
    worker = container.worker
    task = asyncio.create_task(worker.run())
    yield
    # stop worker by closing the underlying transports (InMemoryTransport.close_tasks/close_results)
    try:
        await container.tasks_transport.close_tasks()
    except Exception:
        pass
    if not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
