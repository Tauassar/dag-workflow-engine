from .schemas import WorkflowEvent
from .handler import eh_registry


class RootAggregate:
    def __init__(self):
        self._events = []
        self._version = 0

    def when(self, event: WorkflowEvent) -> None:
        handlers = eh_registry.handlers.get(event.event_type, [])
        for handler_class in handlers:
            handler = handler_class(self)
            handler.handle(event)

    def apply(self, event: WorkflowEvent) -> None:
        self.when(event)
        self._events.append(event)
        self._version += 1
