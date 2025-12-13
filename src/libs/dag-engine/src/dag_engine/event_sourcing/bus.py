import abc
import asyncio
import logging

from collections import defaultdict

from .constants import WorkflowEventType
from .schemas import WorkflowEvent
from .handler import EventHandler
from dag_engine.transport.consumer import RedisPublisher
from .store import EventStore

logger = logging.getLogger(__name__)


class AbstractEventBus(abc.ABC):
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


class EventBus(AbstractEventBus):
    def __init__(self, publisher: RedisPublisher, event_store: EventStore | None = None) -> None:
        self._handlers: dict[WorkflowEventType, list[EventHandler]] = defaultdict(list)
        self.publisher = publisher
        self.event_store = event_store

    def subscribe(
        self,
        event_type: WorkflowEventType,
        handler: EventHandler,
    ) -> None:
        self._handlers[event_type].append(handler)

    async def publish(self, event: WorkflowEvent) -> None:
        await self.publisher.publish(event.model_dump_json())
        if self.event_store:
            await self.event_store.append(event)

    async def handle_event(self, event: WorkflowEvent) -> None:
        handlers = self._handlers.get(event.event_type, [])

        if not handlers:
            return

        tasks = [
            asyncio.create_task(self._safe_handle(handler, event))
            for handler in handlers
        ]

        # fire-and-forget but still awaited for lifecycle control
        await asyncio.gather(*tasks)

    @staticmethod
    async def _safe_handle(
        handler: EventHandler,
        event: WorkflowEvent,
    ) -> None:
        try:
            await handler.handle(event)
        except Exception as exc:
            logger.warning(
                f"[EventBus] handler={handler.__class__.__name__} "
                f"event={type(event).__name__} error={exc}", exc_info=True
            )
