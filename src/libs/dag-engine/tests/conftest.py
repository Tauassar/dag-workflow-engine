import pytest
from fakeredis.aioredis import FakeRedis

from dag_engine.transport import RedisTransport


@pytest.fixture
async def redis():
    client = FakeRedis()
    yield client
    await client.aclose()


@pytest.fixture
async def transport(redis):
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
