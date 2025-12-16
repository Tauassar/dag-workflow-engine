import abc

from .schemas import WorkflowEvent
from dag_engine.utils.registry import BaseRegistry


class EventHandler(abc.ABC):
    @abc.abstractmethod
    async def handle(self, event: WorkflowEvent) -> None:
        ...


class EventHandlerRegistry(BaseRegistry[EventHandler]):
    ...


eh_registry = EventHandlerRegistry()
