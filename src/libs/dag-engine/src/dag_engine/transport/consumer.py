import json
import logging
import typing as t

from redis.asyncio import Redis

from .protocols import Transport

logger = logging.getLogger(__name__)


class RedisPublisher(Transport):
    """
    Redis Streams publisher.

    Streams:
      - stream

    NOTE: subscribe_results must ACK using the same group name that it created.
    """

    def __init__(
        self,
        redis: Redis,
        stream: str,
        block_ms: int = 5000,
    ):
        self.redis = redis
        self._stream = stream
        self.block_ms = block_ms

    async def publish(self, result: str) -> None:
        logger.debug("Publishing message: %s to %s", {"json": result}, self._stream)
        await self.redis.xadd(self._stream, {"json": result}, id="*")


class RedisConsumer:
    """
    Redis Streams consumer.

    Streams:
      - stream

    NOTE: subscribe_results must ACK using the same group name that it created.
    """

    def __init__(
        self,
        redis: Redis,
        stream: str,
        groupname: str = "group",
        consumer_name: str = "consumer",
        block_ms: int = 5000,
    ):
        self.redis = redis
        self._stream = stream
        self._group = groupname
        self.consumer_name = consumer_name
        self.block_ms = block_ms

    async def _init(self):
        # ensure streams and base groups exist
        # prefer xgroup_create with mkstream=True to ensure stream exists in both redis and fakeredis
        await self._ensure_consumer_group(self._stream, self._group)

    async def _ensure_consumer_group(self, stream: str, group: str):
        try:
            await self.redis.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
        except Exception:
            # group already exists
            pass

    async def destroy_consumer_group(self, stream: str, group: str):
        try:
            await self.redis.xgroup_destroy(name=stream, groupname=group)
        except Exception:
            pass

    @staticmethod
    def _get_json_field(fields: dict) -> str | None:
        """
        Redis stream entry fields can be bytes-keys or str-keys depending on client.
        Look for 'json' key robustly.
        """
        if b"json" in fields:
            return fields[b"json"]
        if "json" in fields:
            return fields["json"]
        # fallback: attempt to find a key whose decoded name is 'json'
        for k, v in fields.items():
            try:
                if isinstance(k, bytes) and k.decode() == "json":
                    return v
            except Exception:
                pass
            if str(k) == "json":
                return v
        return None

    async def subscribe(self) -> t.AsyncIterator[dict]:
        """
        Each caller may create/read from a per-workflow consumer group.
        """
        await self._init()

        while True:
            resp = await self.redis.xreadgroup(
                groupname=self._group,
                consumername=self.consumer_name,
                streams={self._stream: ">"},
                count=1,
                block=self.block_ms,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    try:
                        raw = self._get_json_field(fields)
                        if raw is None:
                            raise ValueError("missing json field")
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        yield json.loads(raw)
                    except Exception as e:
                        logger.warning(f"result decode failure: {e}")
                    finally:
                        try:
                            await self.redis.xack(self._stream, self._group, msg_id)
                            await self.redis.xdel(self._stream, msg_id)
                        except Exception:
                            logger.debug("failed to xack/xdel result %s", msg_id)
