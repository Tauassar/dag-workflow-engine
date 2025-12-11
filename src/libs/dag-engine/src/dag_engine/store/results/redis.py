import json
import time
from typing import Any

from dag_engine.serializer import JSONResultSerializer
from redis.asyncio import Redis

from .protocol import ResultStore


class RedisResultStore(ResultStore):
    """
    Redis-backed persistent result store for workflow node outputs.

    Storage format:
        Key:   wf:results:{workflow_id}:{node_id}
        Value: JSON string with fields:
            workflow_id
            node_id
            attempt
            timestamp
            value: <serialized bytes converted to str>

    NOTE: For large results or binary blobs, replace JSONResultSerializer
          with S3PointerSerializer or MsgPack serializer.
    """

    def __init__(
        self,
        redis: Redis,
        namespace: str = "wf:results",
        serializer: JSONResultSerializer | None = None,
    ):
        self.redis = redis
        self.namespace = namespace.rstrip(":")
        self.serializer = serializer or JSONResultSerializer()

    def get_key(self, workflow_id: str, node_id: str) -> str:
        return f"{self.namespace}:{workflow_id}:{node_id}"

    async def save_result(
        self,
        workflow_id: str,
        node_id: str,
        attempt: int,
        result: Any,
        ttl_seconds: int | None = None,
    ) -> None:

        encoded_value = self.serializer.dumps(result)

        payload = {
            "workflow_id": workflow_id,
            "node_id": node_id,
            "attempt": attempt,
            "timestamp": time.time(),
            # store serialized bytes as a UTF-8 string for Redis
            "value": encoded_value.decode("utf-8"),
        }

        data = json.dumps(payload)
        key = self.get_key(workflow_id, node_id)

        # NX ensures idempotency (only first writer wins)
        if ttl_seconds is None:
            await self.redis.set(key, data, nx=True)
        else:
            await self.redis.set(key, data, ex=int(ttl_seconds), nx=True)

    async def get_result(self, workflow_id: str, node_id: str) -> dict[str, Any] | None:
        key = self.get_key(workflow_id, node_id)
        raw = await self.redis.get(key)

        if raw is None:
            return None

        obj = json.loads(raw)

        try:
            value = self.serializer.loads(obj["value"].encode("utf-8"))
        except Exception:
            value = None

        return {
            "workflow_id": workflow_id,
            "node_id": node_id,
            "attempt": obj["attempt"],
            "timestamp": obj["timestamp"],
            "result": value,
        }

    async def list_results(self, workflow_id: str) -> dict[str, dict[str, Any]]:
        pattern = f"{self.namespace}:{workflow_id}:*"
        cursor = 0
        results: dict[str, dict[str, Any]] = {}

        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=200)
            for key in keys:
                raw = await self.redis.get(key)
                if not raw:
                    continue

                obj = json.loads(raw)

                try:
                    value = self.serializer.loads(obj["value"].encode("utf-8"))
                except Exception:
                    value = None

                node_id = key.split(":", 2)[-1]

                results[node_id] = {
                    "workflow_id": workflow_id,
                    "node_id": node_id,
                    "attempt": obj["attempt"],
                    "timestamp": obj["timestamp"],
                    "result": value,
                }

            if cursor == 0:
                break

        return results

    async def delete_results(self, workflow_id: str) -> None:
        pattern = f"{self.namespace}:{workflow_id}:*"
        cursor = 0
        keys_to_delete = []

        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=200)
            keys_to_delete.extend(keys)
            if cursor == 0:
                break

        if keys_to_delete:
            await self.redis.delete(*keys_to_delete)
