import pytest
from fakeredis.aioredis import FakeRedis

from dag_engine.transport import RedisTransport, InMemoryTransport


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


@pytest.fixture
async def redis():
    client = FakeRedis()
    yield client
    await client.aclose()


@pytest.fixture
async def rtransport(redis):
    tr = RedisTransport(
        redis=redis,
        tasks_stream="tasks",
        results_stream="results",
        task_group="task_group",
        result_group="result_group",
        consumer_name="consumer",
        block_ms=50,   # small block for fast tests
    )
    await tr.init()
    return tr


@pytest.fixture
async def in_mem_transport(redis):
    return InMemoryTransport()


@pytest.fixture
async def idemp(redis):
    return FakeIdempotencyStore()


@pytest.fixture
async def rstore(redis):
    return FakeResultStore()
