from typing import Dict, List
from dag_engine.event_sourcing import WorkflowEvent

from .protocols import EventStore


class InMemoryEventStore(EventStore):
    """
    Simple in-memory event store.
    Stores workflow events in a Python dictionary:
        events[workflow_id] = [WorkflowEvent, WorkflowEvent, ...]
    Great for local dev and unit tests.
    """

    def __init__(self) -> None:
        self._events: Dict[str, List[WorkflowEvent]] = {}

    async def append(self, event: WorkflowEvent) -> None:
        wid = event.workflow_id
        if wid not in self._events:
            self._events[wid] = []
        self._events[wid].append(event)

    async def list_events(self, workflow_id: str) -> List[WorkflowEvent]:
        return list(self._events.get(workflow_id, []))
