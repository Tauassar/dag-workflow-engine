import abc

from dag_engine.utils.registry import BaseRegistry

from .schemas import WorkflowEvent


class EventHandler(abc.ABC):
    @abc.abstractmethod
    async def handle(self, event: WorkflowEvent) -> None: ...


class EventHandlerRegistry(BaseRegistry[EventHandler]): ...


eh_registry = EventHandlerRegistry()
