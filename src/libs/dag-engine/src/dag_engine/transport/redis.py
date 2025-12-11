import json
import logging
import typing as t

from redis.asyncio import Redis

from .messages import ResultMessage, TaskMessage
from .protocols import Transport

logger = logging.getLogger(__name__)


class RedisTransport(Transport):
    """
    Redis Streams transport.

    Streams:
      - tasks_stream
      - results_stream

    Each workflow may have its own consumer group for results:
      groupname = result_group + wf_id

    NOTE: subscribe_results must ACK using the same group name that it created.
    """

    def __init__(
        self,
        redis: Redis,
        tasks_stream: str = "tasks",
        results_stream: str = "results",
        task_group: str = "task_group",
        result_group: str = "result_group",
        consumer_name: str = "consumer",
        block_ms: int = 5000,
    ):
        self.redis = redis
        self.tasks_stream = tasks_stream
        self.results_stream = results_stream
        self.task_group = task_group
        self.result_group = result_group
        self.consumer_name = consumer_name
        self.block_ms = block_ms

    async def init(self):
        # ensure streams and base groups exist
        # prefer xgroup_create with mkstream=True to ensure stream exists in both redis and fakeredis
        await self._ensure_consumer_group(self.tasks_stream, self.task_group)
        await self._ensure_consumer_group(self.results_stream, self.result_group)

    async def _ensure_consumer_group(self, stream: str, group: str):
        try:
            await self.redis.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
        except Exception:
            # group already exists
            pass

    async def _destroy_consumer_group(self, stream: str, group: str):
        try:
            await self.redis.xgroup_destroy(name=stream, groupname=group)
        except Exception:
            pass

    async def publish_task(self, task: TaskMessage) -> None:
        logger.debug("Publishing task: %s", {"json": task.model_dump_json()})
        await self.redis.xadd(self.tasks_stream, {"json": task.model_dump_json()}, id="*")

    async def publish_result(self, result: ResultMessage) -> None:
        logger.debug("Publishing result: %s", {"json": result.model_dump_json()})
        await self.redis.xadd(self.results_stream, {"json": result.model_dump_json()}, id="*")

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

    async def subscribe_tasks(self) -> t.AsyncIterator[TaskMessage]:  # type: ignore[override]
        while True:
            resp = await self.redis.xreadgroup(
                groupname=self.task_group,
                consumername=self.consumer_name,
                streams={self.tasks_stream: ">"},
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
                        # raw may be bytes or str
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        data = json.loads(raw)
                        task = TaskMessage.model_validate(data)
                        yield task
                    except Exception as e:
                        logger.warning(f"task decode failure: {e}")
                    finally:
                        # ACK and delete using the static task_group
                        try:
                            await self.redis.xack(self.tasks_stream, self.task_group, msg_id)
                            await self.redis.xdel(self.tasks_stream, msg_id)
                        except Exception:
                            # best-effort: ignore ack/delete errors
                            logger.debug("failed to xack/xdel task %s", msg_id)

    async def subscribe_results(self, wf_id: str = "") -> t.AsyncIterator[ResultMessage]:  # type: ignore[override]
        """
        Each caller may create/read from a per-workflow consumer group.

        groupname = result_group + wf_id
        """
        groupname = self.result_group + wf_id
        await self._ensure_consumer_group(self.results_stream, groupname)

        consumer_name = f"{self.consumer_name}-r-{wf_id}" if wf_id else f"{self.consumer_name}-r"

        while True:
            resp = await self.redis.xreadgroup(
                groupname=groupname,
                consumername=consumer_name,
                streams={self.results_stream: ">"},
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
                        data = json.loads(raw)
                        result = ResultMessage.model_validate(data)
                        yield result
                    except Exception as e:
                        logger.warning(f"result decode failure: {e}")
                    finally:
                        try:
                            await self.redis.xack(self.results_stream, groupname, msg_id)
                            await self.redis.xdel(self.results_stream, msg_id)
                        except Exception:
                            logger.debug("failed to xack/xdel result %s", msg_id)
