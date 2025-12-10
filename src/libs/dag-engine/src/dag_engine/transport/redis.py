import json
import logging
from typing import AsyncIterator
from redis.asyncio import Redis

from .messages import TaskMessage, ResultMessage
from .protocols import Transport


logger = logging.getLogger(__name__)


class RedisTransport(Transport):
    """
    Redis Streams transport.

    All routing is done by DagOrchestrator + Worker.

    Streams are generic:
        - tasks_stream
        - results_stream

    Multiple workflows can share these streams safely.
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

    # -------------------------------------------------------
    # Initialization (idempotent)
    # -------------------------------------------------------
    async def init(self):
        await self._ensure_stream_exists(self.tasks_stream)
        await self._ensure_stream_exists(self.results_stream)
        await self._ensure_consumer_group(self.tasks_stream, self.task_group)
        await self._ensure_consumer_group(self.results_stream, self.result_group)

    async def _ensure_stream_exists(self, stream: str):
        """Ensure Redis stream exists by adding a dummy entry."""
        try:
            await self.redis.xadd(stream, {"_": "init"}, id="*")
        except Exception:
            pass

    async def _ensure_consumer_group(self, stream: str, group: str):
        try:
            await self.redis.xgroup_create(
                name=stream, groupname=group, id="0", mkstream=True
            )
        except Exception:
            # group already exists
            pass

    # -------------------------------------------------------
    # Publish
    # -------------------------------------------------------
    async def publish_task(self, task: TaskMessage) -> None:
        logger.debug("Publishing task: %s", {"json": task.model_dump_json()})
        await self.redis.xadd(
            self.tasks_stream,
            {"json": task.model_dump_json()},
            id="*"
        )

    async def publish_result(self, result: ResultMessage) -> None:
        logger.debug("Publishing result: %s", {"json": result.model_dump_json()})
        await self.redis.xadd(
            self.results_stream,
            {"json": result.model_dump_json()},
            id="*"
        )

    # -------------------------------------------------------
    # Subscribe to tasks (workers)
    # -------------------------------------------------------
    async def subscribe_tasks(self) -> AsyncIterator[TaskMessage]:
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
                        raw = fields.get(b"json") or fields.get("json")
                        data = json.loads(raw)
                        task = TaskMessage.model_validate(data)
                        yield task
                    except Exception as e:
                        print("task decode failure:", e)
                    finally:
                        await self.redis.xack(self.tasks_stream, self.task_group, msg_id)
                        await self.redis.xdel(self.tasks_stream, msg_id)

    # -------------------------------------------------------
    # Subscribe to results (DagOrchestrator)
    # -------------------------------------------------------
    async def subscribe_results(self) -> AsyncIterator[ResultMessage]:
        while True:
            resp = await self.redis.xreadgroup(
                groupname=self.result_group,
                consumername=self.consumer_name + "-r",
                streams={self.results_stream: ">"},
                count=1,
                block=self.block_ms,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    try:
                        raw = fields.get(b"json") or fields.get("json")
                        data = json.loads(raw)
                        result = ResultMessage.model_validate(data)
                        yield result
                    except Exception as e:
                        print("result decode failure:", e)
                    finally:
                        await self.redis.xack(self.results_stream, self.result_group, msg_id)
                        await self.redis.xdel(self.results_stream, msg_id)
