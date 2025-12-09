import typing as t
from dag_engine.transport import ResultType, ResultMessage, Transport, TaskMessage


Handler = t.Callable[[TaskMessage], t.Awaitable[t.Any]]


class WorkflowWorker:
    """
    Worker waits for work from DAG.task_queue.
    Lifecycle is controlled EXTERNALLY (do not start inside DAG).
    """

    def __init__(self, transport: Transport, handler_registry: dict[str, Handler], worker_id: str = "w"):
        self.transport = transport
        self.handlers = handler_registry
        self.worker_id = worker_id
        self._stop = False

    async def run(self) -> None:
        async for task in self.transport.subscribe_tasks():
            if task is None:
                return
            await self._handle_task(task)

    async def _handle_task(self, task: TaskMessage) -> None:
        # Find handler
        handler = self.handlers.get(task.node_type)
        if handler is None:
            # publish failed result
            await self.transport.publish_result(ResultMessage(
                workflow_id=task.workflow_id,
                workflow_name=task.workflow_name,
                node_id=task.node_id,
                attempt=task.attempt,
                type=ResultType.FAILED,
                error=f"No handler for type {task.node_type}",
            ))
            return

        try:
            # call handler with TaskMessage; handler returns arbitrary payload
            result_payload = await handler(task)
            await self.transport.publish_result(ResultMessage(
                workflow_id=task.workflow_id,
                workflow_name=task.workflow_name,
                node_id=task.node_id,
                attempt=task.attempt,
                type=ResultType.COMPLETED,
                payload=result_payload,
            ))
        except Exception as exc:
            await self.transport.publish_result(ResultMessage(
                workflow_id=task.workflow_id,
                workflow_name=task.workflow_name,
                node_id=task.node_id,
                attempt=task.attempt,
                type=ResultType.FAILED,
                error=str(exc),
            ))
