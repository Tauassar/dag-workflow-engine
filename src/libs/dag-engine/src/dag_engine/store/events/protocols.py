from typing import Protocol

from dag_engine.event_sourcing import WorkflowEvent


class EventStore(Protocol):
    async def append(self, event: WorkflowEvent) -> None: ...

    async def list_events(self, workflow_id: str) -> list[WorkflowEvent]: ...
