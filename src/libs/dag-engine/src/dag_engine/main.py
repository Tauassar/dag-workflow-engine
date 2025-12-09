from __future__ import annotations
import json

import asyncio, time
import logging

from dag_engine.core.services import DagService
from dag_engine.core.worker import WorkflowWorker
from dag_engine.store import InMemoryEventStore
from dag_engine.transport import TaskMessage, InMemoryTransport
from .core.workflow import WorkflowDAG, Handler

logging.basicConfig(level=logging.DEBUG)

USER_JSON = """{
  "name": "Parallel API Fetcher",
  "dag": {
    "nodes": [
      {
        "id": "input",
        "handler": "input",
        "dependencies": []
      },
      {
        "id": "get_user",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {
          "url": "http://localhost:8911/document/policy/list"
        }
      },
      {
        "id": "get_posts",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {
          "url": "http://localhost:8911/document/policy/list"
        }
      },
      {
        "id": "get_comments",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {
          "url": "http://localhost:8911/document/policy/list"
        }
      },
      {
        "id": "output",
        "handler": "output",
        "dependencies": [
          "get_user",
          "get_posts",
          "get_comments"
        ]
      }
    ]
  }
}"""

dag = WorkflowDAG.from_dict(json.loads(USER_JSON), concurrency=3)

@dag.handler("input")
async def input_handler(task: TaskMessage):
    # produce initial payload
    await asyncio.sleep(0.01)
    return {"input_payload": {"user_id": "u-123"}}


@dag.handler("call_external_service")
async def call_external_service(task: TaskMessage):
    # Simulate HTTP call
    await asyncio.sleep(0.05)
    # return data including config echo
    return {"node": task.node_id, "url": task.config.get("url"), "fetched_at": time.time()}


@dag.handler("output")
async def output_handler(task: TaskMessage):
    await asyncio.sleep(0.01)
    return {"node": task.node_id, "aggregated": True, "note": "aggregation done by DAGService", "ctx": task}



# --- Run the workflow ---
async def main():
    transport = InMemoryTransport()
    store = InMemoryEventStore()
    dag_service = DagService(dag, transport, event_store=store)

    # start DagService (seed roots and start result subscription)
    await dag_service.start()

    # start external workers (they read tasks via transport)
    handler_registry: dict[str, Handler] = {
        "input": input_handler,
        "call_external_service": call_external_service,
        "output": output_handler,
    }
    worker1 = WorkflowWorker(transport, handler_registry, worker_id="w1")
    worker2 = WorkflowWorker(transport, handler_registry, worker_id="w2")

    # run workers in background
    wtask1 = asyncio.create_task(worker1.run())
    wtask2 = asyncio.create_task(worker2.run())

    # wait for workflow to finish (DagService watches results and updates DAG)
    await dag_service.wait_until_finished()

    # stop workers & close transport to end their subscribe_tasks loops
    # (InMemoryTransport supports a close_tasks helper)
    if isinstance(transport, InMemoryTransport):
        await transport.close_tasks()
        await transport.close_results()  # close result subscription as well

    # await asyncio.gather(wtask1, wtask2, return_exceptions=True)

    # collect outputs and events
    results = dag_service.collect_results()
    events = await store.list_events(dag.workflow_id)

    print("=== RESULTS ===")
    print(results)
    print("\n=== EVENTS ===")
    for e in events:
        print(e.model_dump())

if __name__ == "__main__":
    asyncio.run(main())
