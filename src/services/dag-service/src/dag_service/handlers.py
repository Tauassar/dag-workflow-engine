import asyncio
import time
import typing as t

from dag_engine.core import hregistry
from dag_engine.transport import TaskMessage


@hregistry.handler("input")
async def input_handler(_: TaskMessage) -> dict[str, t.Any]:
    # produce initial payload
    await asyncio.sleep(0.01)
    return {"input_payload": {"user_id": "u-123"}}


@hregistry.handler("call_external_service")
async def call_external_service(task: TaskMessage) -> dict[str, t.Any]:
    # Simulate HTTP call
    await asyncio.sleep(2)
    # return data including config echo
    return {
        "node": task.node_id,
        "url": task.config.get("url"),
        "fetched_content": "some sample content",
        "fetched_at": time.time(),
    }


@hregistry.handler("call_llm_service")
async def call_llm_service(task: TaskMessage) -> str:
    await asyncio.sleep(0.1)
    return f"Here is sample response for original {t.cast(str, task.config.get("prompt")).format(**task.config)} prompt"


@hregistry.handler("output")
async def output_handler(task: TaskMessage) -> dict[str, t.Any]:
    await asyncio.sleep(0.01)
    return {"node": task.node_id, "aggregated": True, "note": "aggregation done by DagOrchestrator", "ctx": task}
