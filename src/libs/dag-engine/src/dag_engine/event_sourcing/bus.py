import abc
import asyncio
import logging

from collections import defaultdict

from .constants import WorkflowEventType
from .schemas import WorkflowEvent
from .handler import EventHandler


logger = logging.getLogger(__name__)


class EventBus(abc.ABC):
    @abc.abstractmethod
    def subscribe(
        self,
        event_type: type[WorkflowEventType],
        handler: EventHandler,
    ) -> None:
        ...

    @abc.abstractmethod
    async def publish(self, event: WorkflowEvent) -> None:
        ...

    @abc.abstractmethod
    async def publish_many(self, events: list[WorkflowEvent]) -> None:
        ...


class InMemoryEventBus(EventBus):
    def __init__(self) -> None:
        self._handlers: dict[type[WorkflowEventType], list[EventHandler]] = defaultdict(list)

    def subscribe(
        self,
        event_type: type[WorkflowEventType],
        handler: EventHandler,
    ) -> None:
        self._handlers[event_type].append(handler)

    async def publish(self, event: WorkflowEvent) -> None:
        handlers = self._handlers.get(type(event.event_type), [])

        if not handlers:
            return

        tasks = [
            asyncio.create_task(self._safe_handle(handler, event))
            for handler in handlers
        ]

        # fire-and-forget but still awaited for lifecycle control
        await asyncio.gather(*tasks)

    async def publish_many(self, events: list[WorkflowEvent]) -> None:
        for event in events:
            await self.publish(event)

    async def _safe_handle(
        self,
        handler: EventHandler,
        event: WorkflowEvent,
    ) -> None:
        try:
            await handler.handle(event)
        except Exception as exc:
            logger.warning(
                f"[EventBus] handler={handler.__class__.__name__} "
                f"event={type(event).__name__} error={exc}"
            )

