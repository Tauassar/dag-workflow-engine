import json
import time
from typing import Any, Dict, Optional
from redis.asyncio import Redis

from dag_engine.serializer.json import JSONResultSerializer

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
        serializer: Optional[JSONResultSerializer] = None,
    ):
        self.redis = redis
        self.namespace = namespace.rstrip(":")
        self.serializer = serializer or JSONResultSerializer()

    # --------------------------------------------------------
    # Key format helper
    # --------------------------------------------------------
    def get_key(self, workflow_id: str, node_id: str) -> str:
        return f"{self.namespace}:{workflow_id}:{node_id}"

    # --------------------------------------------------------
    # Save result for a node
    # --------------------------------------------------------
    async def save_result(
        self,
        workflow_id: str,
        node_id: str,
        attempt: int,
        result: Any,
        ttl_seconds: Optional[int] = None,
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

        if ttl_seconds is None:
            await self.redis.set(key, data)
        else:
            await self.redis.set(key, data, ex=int(ttl_seconds))

    # --------------------------------------------------------
    # Fetch result for a single node
    # --------------------------------------------------------
    async def get_result(self, workflow_id: str, node_id: str) -> Optional[Dict[str, Any]]:
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

    # --------------------------------------------------------
    # List all results for workflow
    # --------------------------------------------------------
    async def list_results(self, workflow_id: str) -> Dict[str, Dict[str, Any]]:
        pattern = f"{self.namespace}:{workflow_id}:*"
        cursor = 0
        results: Dict[str, Dict[str, Any]] = {}

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

    # --------------------------------------------------------
    # Delete all results for workflow
    # --------------------------------------------------------
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
