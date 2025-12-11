import json
import typing as t
from redis.asyncio import Redis

from .protocols import WorkflowExecutionStore


class RedisExecutionStore(WorkflowExecutionStore):
    """
    Persists final workflow execution metadata + results snapshot.
    Does NOT store in-progress state (that lives in DagOrchestrator).
    """

    def __init__(self, redis: Redis):
        self.redis = redis
        self.meta_prefix = "wf:exec:meta:"
        self.result_prefix = "wf:exec:results:"

    async def save_metadata(self, workflow_id: str, data: dict[str, t.Any]):
        key = self.meta_prefix + workflow_id
        await self.redis.set(key, json.dumps(data))

    async def save_results(self, workflow_id: str, results: dict[str, t.Any]):
        key = self.result_prefix + workflow_id
        await self.redis.set(key, json.dumps(results))

    async def load_metadata(self, workflow_id: str) -> dict[str, t.Any] | None:
        key = self.meta_prefix + workflow_id
        raw = await self.redis.get(key)
        if not raw:
            return None
        return json.loads(raw)

    async def load_results(self, workflow_id: str) -> dict[str, t.Any] | None:
        key = self.result_prefix + workflow_id
        raw = await self.redis.get(key)
        if not raw:
            return None
        return json.loads(raw)

    async def delete(self, workflow_id: str):
        await self.redis.delete(self.meta_prefix + workflow_id)
        await self.redis.delete(self.result_prefix + workflow_id)
