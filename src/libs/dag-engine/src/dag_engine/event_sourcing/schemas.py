import time
import typing as t

import pydantic as pd

from .constants import WorkflowEventType


class WorkflowEvent(pd.BaseModel):
    workflow_name: str
    workflow_id: str
    node_id: str
    event_type: WorkflowEventType
    timestamp: float = pd.Field(default_factory=lambda: time.time())
    attempt: int
    payload: dict[str, t.Any] | None = None  # node result or error
