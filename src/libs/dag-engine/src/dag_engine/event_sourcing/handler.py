import abc
import typing as t

from .schemas import WorkflowEvent

E = t.TypeVar("E", bound=WorkflowEvent)


class EventHandler(t.Generic[E], abc.ABC):
    @abc.abstractmethod
    async def handle(self, event: E) -> None:
        ...
