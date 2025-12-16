import pytest
import logging
from fakeredis.aioredis import FakeRedis

from dag_engine.event_sourcing import EventBus, InMemoryEventStore
from dag_engine.store.atomic_counter import InMemCounterStore
from dag_engine.transport import InMemoryConsumer, InMemoryPublisher


class FakeResultStore:
    def __init__(self):
        self.data = {}

    def get_key(self, workflow_id: str, node_id: str):
        return f"res:{workflow_id}:{node_id}"

    async def save_result(self, workflow_id, node_id, attempt, result, ttl_seconds=None):
        key = self.get_key(workflow_id, node_id)
        self.data[key] = {
            "workflow_id": workflow_id,
            "node_id": node_id,
            "attempt": attempt,
            "result": result,
        }

    async def get_result(self, workflow_id, node_id):
        key = self.get_key(workflow_id, node_id)
        return self.data.get(key)


class FakeIdempotencyStore:
    def __init__(self):
        self._seen = set()

    async def set_if_absent(self, key: str, ttl_seconds=None):
        if key in self._seen:
            return False
        self._seen.add(key)
        return True

class FakeExecutionStore:
    def __init__(self):
        self.meta = {}
        self.results = {}

    async def save_metadata(self, workflow_id: str, meta: dict):
        self.meta[workflow_id] = meta

    async def load_metadata(self, workflow_id: str):
        return self.meta.get(workflow_id)

    async def save_results(self, workflow_id: str, results: dict):
        self.results[workflow_id] = results

    async def load_results(self, workflow_id: str):
        return self.results.get(workflow_id)


@pytest.fixture(scope="session", autouse=True)
async def session_setup() -> None:
    """Session setup fixture."""
    LOG_FORMAT = "%(asctime)s [%(levelname)s] " "%(filename)s:%(lineno)d (%(funcName)s) — %(message)s"
    logging.basicConfig(
        level=logging.DEBUG,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@pytest.fixture
async def redis():
    client = FakeRedis()
    yield client
    await client.aclose()


@pytest.fixture
async def event_store():
    return InMemoryEventStore()


@pytest.fixture
async def publisher():
    return InMemoryPublisher("test")


@pytest.fixture
async def consumer():
    return InMemoryConsumer("test")


@pytest.fixture
async def counter():
    return InMemCounterStore()


@pytest.fixture
async def event_bus(publisher, event_store):
    return EventBus(publisher=publisher, event_store=event_store)


@pytest.fixture
async def idemp(redis):
    return FakeIdempotencyStore()


@pytest.fixture
async def rstore(redis):
    return FakeResultStore()


@pytest.fixture
async def exec_store(redis):
    return FakeExecutionStore()
