import json
from typing import AsyncIterator
from redis.asyncio import Redis

from .messages import TaskMessage, ResultMessage
from .protocols import Transport


class RedisTransport(Transport):
    """
    Redis Streams based transport.
    Streams:
        tasks_stream:  stream for task messages
        results_stream: stream for result messages

    Uses consumer groups to allow multiple workers to share the load.
    """

    def __init__(
        self,
        redis: Redis,
        tasks_stream: str = "wf:tasks",
        results_stream: str = "wf:results",
        task_group: str = "task_group",
        result_group: str = "result_group",
        consumer_name: str = "consumer-1",
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
    # Consumer group initialization
    # -------------------------------------------------------
    async def init(self):
        """
        Create consumer groups if they do not exist.
        Redis streams must exist before XGROUP CREATE, so we XADD a dummy entry.
        """
        # Create stream implicitly if empty
        try:
            await self.redis.xadd(self.tasks_stream, {"_": "init"}, id="*")
        except Exception:
            pass
        try:
            await self.redis.xadd(self.results_stream, {"_": "init"}, id="*")
        except Exception:
            pass

        # Create task consumer group
        try:
            await self.redis.xgroup_create(
                name=self.tasks_stream,
                groupname=self.task_group,
                id="0",
                mkstream=True,
            )
        except Exception:
            # already exists
            pass

        # Create result consumer group
        try:
            await self.redis.xgroup_create(
                name=self.results_stream,
                groupname=self.result_group,
                id="0",
                mkstream=True,
            )
        except Exception:
            pass

    # -------------------------------------------------------
    # Publish messages
    # -------------------------------------------------------
    async def publish_task(self, task: TaskMessage) -> None:
        await self.redis.xadd(
            self.tasks_stream,
            {"json": task.model_dump()},
            id="*"
        )

    async def publish_result(self, result: ResultMessage) -> None:
        await self.redis.xadd(
            self.results_stream,
            {"json": result.model_dump()},
            id="*"
        )

    # -------------------------------------------------------
    # Subscribe to tasks
    # -------------------------------------------------------
    async def subscribe_tasks(self) -> AsyncIterator[TaskMessage]:
        """
        Workers call this: uses XREADGROUP to fetch tasks.
        Uses consumer group so messages are distributed across workers.
        """
        while True:
            # Read messages for this consumer
            resp = await self.redis.xreadgroup(
                groupname=self.task_group,
                consumername=self.consumer_name,
                streams={self.tasks_stream: ">"},
                count=1,
                block=self.block_ms,
            )

            if not resp:
                continue  # timeout, check again

            for stream_name, messages in resp:
                for msg_id, fields in messages:
                    try:
                        payload = fields.get(b"json") or fields.get("json")
                        data = json.loads(payload)
                        task = TaskMessage.parse_obj(data)
                        yield task
                    except Exception as exc:
                        print("Failed to decode task:", exc)
                        continue
                    finally:
                        # Acknowledge so it's not re-delivered
                        await self.redis.xack(
                            self.tasks_stream, self.task_group, msg_id
                        )
                        await self.redis.xdel(
                            self.tasks_stream, msg_id
                        )

    # -------------------------------------------------------
    # Subscribe to results
    # -------------------------------------------------------
    async def subscribe_results(self) -> AsyncIterator[ResultMessage]:
        """
        DagService calls this: consumes results from worker.
        """
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

            for stream_name, messages in resp:
                for msg_id, fields in messages:
                    try:
                        payload = fields.get(b"json") or fields.get("json")
                        data = json.loads(payload)
                        result = ResultMessage.parse_obj(data)
                        yield result
                    except Exception as exc:
                        print("Failed to decode result:", exc)
                        continue
                    finally:
                        await self.redis.xack(
                            self.results_stream, self.result_group, msg_id
                        )
                        await self.redis.xdel(
                            self.results_stream, msg_id
                        )
