from __future__ import annotations
from redis.asyncio import Redis

from .protocol import DependencyCounterStore


class RedisDependencyCounterStore(DependencyCounterStore):
    def __init__(self, redis: Redis, prefix: str = "depcount"):
        self.redis = redis
        self.prefix = prefix

    def _key(self, workflow_id: str, node_id: str) -> str:
        return f"{self.prefix}:{workflow_id}:{node_id}"

    async def init_counter(
        self,
        workflow_id: str,
        node_id: str,
        initial: int,
        ttl_seconds: int | None = None,
    ) -> None:
        """
        SETNX ensures idempotency across orchestrators.
        """
        key = self._key(workflow_id, node_id)

        created = await self.redis.setnx(key, initial)
        if created and ttl_seconds:
            await self.redis.expire(key, ttl_seconds)

    async def decrement(
        self,
        workflow_id: str,
        node_id: str,
    ) -> int:
        """
        Atomic decrement.
        Redis guarantees INCRBY atomicity.
        """
        key = self._key(workflow_id, node_id)
        return int(await self.redis.incrby(key, -1))

    async def get(
        self,
        workflow_id: str,
        node_id: str,
    ) -> int | None:
        val = await self.redis.get(self._key(workflow_id, node_id))
        return int(val) if val is not None else None

    async def delete(
        self,
        workflow_id: str,
        node_id: str,
    ) -> None:
        await self.redis.delete(self._key(workflow_id, node_id))
