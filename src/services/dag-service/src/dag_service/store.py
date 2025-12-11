import json
from redis.asyncio import Redis

from dag_engine.core import WorkflowDefinition


class WorkflowDefinitionStore:
    """
    Persist workflow DAG definitions so that /workflow only registers
    the workflow and does NOT start execution.
    """

    def __init__(self, redis: Redis):
        self.redis = redis
        self.prefix = "wf:definition:"

    async def save_definition(self, workflow_id: str, definition: WorkflowDefinition) -> None:
        key = self.prefix + workflow_id
        await self.redis.set(key, definition.model_dump_json(by_alias=True), nx=True)

    async def get_definition(self, workflow_id: str) -> dict | None:
        key = self.prefix + workflow_id
        raw = await self.redis.get(key)
        if not raw:
            return None
        return json.loads(raw)

    async def delete_definition(self, workflow_id: str) -> None:
        await self.redis.delete(self.prefix + workflow_id)
