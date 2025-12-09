import redis.asyncio as aioredis

from dag_engine.event_sourcing.schemas import WorkflowEvent

from .protocols import EventStore


class RedisEventStore(EventStore):
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

    def _key(self, workflow_id: str) -> str:
        return f"wf:events:{workflow_id}"

    async def append(self, event: WorkflowEvent) -> None:
        await self.redis.rpush(self._key(event.workflow_id), event.json())

    async def list_events(self, workflow_id: str):
        raw = await self.redis.lrange(self._key(workflow_id), 0, -1)
        return [WorkflowEvent.model_validate_json(e) for e in raw]
