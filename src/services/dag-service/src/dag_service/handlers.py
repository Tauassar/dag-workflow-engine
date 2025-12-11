import asyncio
import time

from dag_engine.core import hregistry
from dag_engine.transport import TaskMessage


@hregistry.handler("input")
async def input_handler(_: TaskMessage):
    # produce initial payload
    await asyncio.sleep(0.01)
    return {"input_payload": {"user_id": "u-123"}}


@hregistry.handler("call_external_service")
async def call_external_service(task: TaskMessage):
    # Simulate HTTP call
    await asyncio.sleep(0.05)
    # return data including config echo
    return {
        "node": task.node_id,
        "url": task.config.get("url"),
        "fetched_at": time.time(),
        "user_id": task.config.get("user_id"),
    }


@hregistry.handler("output")
async def output_handler(task: TaskMessage):
    await asyncio.sleep(0.01)
    return {"node": task.node_id, "aggregated": True, "note": "aggregation done by DagOrchestrator", "ctx": task}
