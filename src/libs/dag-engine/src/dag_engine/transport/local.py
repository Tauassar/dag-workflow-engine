import asyncio
import logging
import typing as t

from .messages import ResultMessage, TaskMessage
from .protocols import Transport

logger = logging.getLogger(__name__)


class InMemoryTransport(Transport):
    """Simple in-process transport using asyncio.Queue for dev/testing."""

    def __init__(self):
        self._task_q: asyncio.Queue[TaskMessage | None] = asyncio.Queue()
        self._result_q: asyncio.Queue[ResultMessage | None] = asyncio.Queue()

    async def publish_task(self, task: TaskMessage) -> None:
        logger.debug(f"publish_task {task}")
        await self._task_q.put(task)

    async def publish_result(self, result: ResultMessage) -> None:
        logger.debug(f"publish_result {result}")
        await self._result_q.put(result)

    async def subscribe_tasks(self) -> t.AsyncIterator[TaskMessage]:  # type: ignore[override]
        while True:
            msg = await self._task_q.get()
            # sentinel to stop: None
            if msg is None:
                return
            yield msg

    async def subscribe_results(self) -> t.AsyncIterator[ResultMessage]:  # type: ignore[override]
        while True:
            msg = await self._result_q.get()
            if msg is None:
                return
            yield msg

    # helper methods for orchestrator to close channels
    async def close_tasks(self):
        await self._task_q.put(None)

    async def close_results(self):
        await self._result_q.put(None)
