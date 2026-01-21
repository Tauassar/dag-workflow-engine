import typing as t

from .messages import ResultMessage, TaskMessage


class Publisher(t.Protocol):
    """
    publisher.
    """

    async def publish(self, result: str) -> None: ...


class Consumer(t.Protocol):
    """
    consumer.
    """

    async def subscribe(self) -> t.AsyncIterator[dict]:
        """
        Each caller may create/read from a per-workflow consumer group.
        """
        ...


class Transport(t.Protocol):
    async def init(self): ...
    async def publish_task(self, task: TaskMessage) -> None: ...
    async def publish_result(self, result: ResultMessage) -> None: ...
    async def subscribe_tasks(self) -> t.AsyncIterator[TaskMessage]: ...
    async def subscribe_results(self, wf_id: str = "") -> t.AsyncIterator[ResultMessage]: ...
