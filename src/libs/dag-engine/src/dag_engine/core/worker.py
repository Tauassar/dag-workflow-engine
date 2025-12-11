import traceback
import typing as t

from dag_engine.store.idempotency import IdempotencyStore
from dag_engine.store.results import ResultStore
from dag_engine.transport import ResultMessage, ResultType, TaskMessage, Transport

from .handlers import Handler


class WorkflowWorker:
    """
    Distributed worker that:
      - consumes TaskMessage from Transport
      - executes user-defined handler
      - stores result in ResultStore (if provided)
      - publishes ResultMessage to Transport

    Workers are stateless and safe to run across many processes.
    """

    def __init__(
        self,
        transport: Transport,
        handler_registry: dict[str, Handler],
        idempotency_store: IdempotencyStore,
        result_store: ResultStore | None = None,
        worker_id: str = "worker",
        result_ttl_seconds: int | None = None,
    ):
        self.transport = transport
        self.handlers = handler_registry
        self.result_store = result_store
        self.idempotency_store = idempotency_store
        self.worker_id = worker_id
        self._stop = False
        self.result_ttl = result_ttl_seconds

    async def run(self) -> None:
        """
        Main loop. Continuously listens for tasks from transport.
        Stop by setting self._stop or transport closing the stream.
        """
        async for task in t.cast(t.AsyncIterator[TaskMessage], self.transport.subscribe_tasks()):
            if task is None:
                return
            await self._handle_task(task)

    async def already_processed(self, task: TaskMessage) -> bool:
        exec_key = f"exec:{task.workflow_id}:{task.node_id}:{task.attempt}"
        ok = await self.idempotency_store.set_if_absent(exec_key, ttl_seconds=3600)
        if not ok:
            return True
        return False

    async def _handle_task(self, task: TaskMessage) -> None:
        """
        Execute the handler for the task.
        Persist result first (if ResultStore is enabled).
        Publish a ResultMessage (SUCCESS or FAILED).
        """
        if await self.already_processed(task):
            return

        handler = self.handlers.get(task.node_type)

        if handler is None:
            await self._publish_failure(
                task, f"Worker {self.worker_id}: No handler registered for node type '{task.node_type}'"
            )
            return

        try:
            # Run handler
            result_value = await handler(task)

            # Persist result first if using ResultStore
            pointer = None
            if self.result_store:
                await self.result_store.save_result(
                    workflow_id=task.workflow_id,
                    node_id=task.node_id,
                    attempt=task.attempt,
                    result=result_value,
                    ttl_seconds=self.result_ttl,
                )
                pointer = {"result_key": self.result_store.get_key(task.workflow_id, task.node_id)}

            # Publish success (payload is either pointer or actual result)
            await self.transport.publish_result(
                ResultMessage(
                    workflow_id=task.workflow_id,
                    workflow_name=task.workflow_name,
                    node_id=task.node_id,
                    attempt=task.attempt,
                    type=ResultType.COMPLETED,
                    payload=pointer if pointer else result_value,
                )
            )

        except Exception as exc:
            await self._publish_failure(task, str(exc), exc)

    async def _publish_failure(self, task: TaskMessage, error: str, exc: Exception | None = None):
        """
        Publish a FAILED ResultMessage.
        """
        err_text = error
        if exc:
            # attach traceback info for debugging purposes
            tb = traceback.format_exc()
            err_text = f"{error}\n{tb}"

        await self.transport.publish_result(
            ResultMessage(
                workflow_id=task.workflow_id,
                workflow_name=task.workflow_name,
                node_id=task.node_id,
                attempt=task.attempt,
                type=ResultType.FAILED,
                error=err_text,
            )
        )
