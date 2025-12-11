import typing as t
from dataclasses import dataclass, field

from .constants import NodeStatus
from .schemas import (
    RetryPolicy,
)


@dataclass
class DagNode:
    id: str
    type: str
    config: dict[str, t.Any]
    depends_on: set[str] = field(default_factory=set)
    dependents: set[str] = field(default_factory=set)
    retry_policy: RetryPolicy | None = None
    timeout_seconds: float | None = None
    status: NodeStatus = NodeStatus.PENDING
    last_error: Exception | str | None = None
    attempt: int = 0
    result: t.Any = None
    started_at: float | None = None
    finished_at: float | None = None
    deadline_at: float | None = None
    blocked_by: list[str] | None = None
