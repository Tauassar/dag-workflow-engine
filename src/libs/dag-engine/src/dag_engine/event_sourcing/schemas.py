import time
import typing as t

import pydantic as pd

from .constants import WorkflowEventType


class WorkflowEvent(pd.BaseModel):
    workflow_name: str
    workflow_id: str
    node_id: str
    node_type: str | None = None
    event_type: WorkflowEventType
    timestamp: float = pd.Field(default_factory=time.time)
    expire_at: float | None = None
    attempt: int
    payload: dict[str, t.Any] = pd.Field(default_factory=dict)
    error: str | None = None
