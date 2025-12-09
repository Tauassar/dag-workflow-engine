import enum
import time
import typing as t
import pydantic as pd


class TaskMessage(pd.BaseModel):
    workflow_name: str
    workflow_id: str
    node_id: str
    node_type: str
    attempt: int
    config: dict[str, t.Any] = {}
    timestamp: float = pd.Field(default_factory=lambda: time.time())


class ResultType(enum.StrEnum):
    COMPLETED = "COMPLETED"
    FAILED  = "FAILED"


class ResultMessage(pd.BaseModel):
    workflow_name: str
    workflow_id: str
    node_id: str
    attempt: int
    type: ResultType
    payload: t.Any = None
    error: str | None = None
    timestamp: float = pd.Field(default_factory=lambda: time.time())
