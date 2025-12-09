import typing as t

from .entities import NodeExecutionSnapshot
from ..transport import TaskMessage

Handler = t.Callable[[TaskMessage], t.Awaitable[t.Any]]


class WorkflowExecutionContext(t.Protocol):
    workflow_name: str
    async def mark_running(self, node_id: str) -> NodeExecutionSnapshot | None: ...
    async def mark_success(self, node_id: str, result: t.Any) -> None: ...
    async def mark_failed(self, node_id: str, exc: Exception) -> None: ...
    async def schedule_retry(self, node_id: str, delay: float) -> None: ...
    def get_handler(self, node_type: str) -> Handler: ...
    def get_node_result(self, node_id: str) -> t.Any: ...


class NodeCtx(t.TypedDict, total=False):
    workflow_name: str
    node_id: str
    node_def: t.Any
    attempt: int
    payload: t.Any
    execution: WorkflowExecutionContext  # handlers can call get_node_result()
