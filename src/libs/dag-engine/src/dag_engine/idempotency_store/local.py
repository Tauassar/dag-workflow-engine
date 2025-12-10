import time

from .protocols import IdempotencyStore


class InMemoryIdempotencyStore(IdempotencyStore):
    def __init__(self):
        self._keys: dict[str, float] = {}  # key -> expiry timestamp (0 meaning no expiry)

    async def set_if_absent(self, key: str, ttl_seconds: int | None = None) -> bool:
        now = time.time()
        exp = 0 if ttl_seconds is None else now + ttl_seconds
        # cleanup expired
        to_delete = [k for k, v in self._keys.items() if v and v <= now]
        for k in to_delete:
            del self._keys[k]
        if key in self._keys:
            return False
        self._keys[key] = exp
        return True

    async def exists(self, key: str) -> bool:
        now = time.time()
        val = self._keys.get(key)
        if val is None:
            return False
        if val and val <= now:
            del self._keys[key]
            return False
        return True

    async def delete(self, key: str) -> None:
        if key in self._keys:
            del self._keys[key]
