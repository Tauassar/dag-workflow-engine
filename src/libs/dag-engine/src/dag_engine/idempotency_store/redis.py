from redis.asyncio import Redis

from .protocols import IdempotencyStore


class RedisIdempotencyStore(IdempotencyStore):
    def __init__(self, redis: Redis, namespace: str = "wf:idemp"):
        self.redis = redis
        self.namespace = namespace.rstrip(":")

    def _key(self, key: str) -> str:
        # user passes "exec:wf:nid:attempt" or "dispatch:wf:nid:attempt"
        return f"{self.namespace}:{key}"

    # --------------------------------------------------------------
    async def set_if_absent(self, key: str, ttl_seconds: int | None = None) -> bool:
        """
        Returns True if the key was created.
        Returns False if already exists.
        """
        rkey = self._key(key)

        if ttl_seconds is not None:
            res = await self.redis.set(rkey, "1", nx=True, ex=ttl_seconds)
        else:
            res = await self.redis.set(rkey, "1", nx=True)

        return res is True

    async def exists(self, key: str) -> bool:
        rkey = self._key(key)
        val = await self.redis.exists(rkey)
        return val == 1

    async def delete(self, key: str) -> None:
        rkey = self._key(key)
        await self.redis.delete(rkey)
