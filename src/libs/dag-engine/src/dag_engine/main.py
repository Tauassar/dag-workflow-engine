from __future__ import annotations
import json

import asyncio, time
import logging

from dag_engine.core import DagOrchestrator,  WorkflowWorker
from dag_engine.event_store import RedisEventStore
from dag_engine.idempotency_store import RedisIdempotencyStore
from dag_engine.result_store import RedisResultStore
from dag_engine.transport import TaskMessage, InMemoryTransport, RedisTransport
from .core.workflow import WorkflowDAG
from redis.asyncio import Redis

redis = Redis(host="localhost", port=6379, decode_responses=True)

LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] "
    "%(filename)s:%(lineno)d (%(funcName)s) — %(message)s"
)

logging.basicConfig(
    level=logging.DEBUG,
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)

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
          "url": "http://localhost:8911/document/policy/list/{{input.input_payload.user_id}}",
          "user_id": "{{input.input_payload.user_id}}"
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
        "handler": "call_external_service1",
        "dependencies": ["input"],
        "timeout_seconds": 1,
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
    return {"node": task.node_id, "url": task.config.get("url"), "fetched_at": time.time(), "user_id": task.config.get("user_id")}


@dag.handler("call_external_service1")
async def call_external_service(task: TaskMessage):
    print(f"Received task {task.model_dump()}")
    # Simulate HTTP call
    await asyncio.sleep(50.01)
    # return data including config echo
    return {"node": task.node_id, "url": task.config.get("url"), "fetched_at": time.time(), "user_id": task.config.get("user_id")}


@dag.handler("output")
async def output_handler(task: TaskMessage):
    await asyncio.sleep(0.01)
    return {"node": task.node_id, "aggregated": True, "note": "aggregation done by DagOrchestrator", "ctx": task}



# --- Run the workflow ---
async def main():
    result_store = RedisResultStore(redis)
    idemp_store = RedisIdempotencyStore(redis)
    transport = RedisTransport(
        redis=redis,
        tasks_stream="engine:tasks",
        results_stream="engine:results",
        task_group="engine-task-group",
        result_group="engine-result-group",
        consumer_name="controller",   # for DagOrchestrator
    )
    worker_transport = RedisTransport(
        redis=redis,
        tasks_stream="engine:tasks",
        results_stream="engine:results",
        task_group="engine-task-group",
        result_group="engine-result-group",
        consumer_name="worker-1",
    )
    worker2_transport = RedisTransport(
        redis=redis,
        tasks_stream="engine:tasks",
        results_stream="engine:results",
        task_group="engine-task-group",
        result_group="engine-result-group",
        consumer_name="worker-2",
    )
    await transport.init()
    await worker_transport.init()
    await worker2_transport.init()
    event_store = RedisEventStore(redis)
    dag.event_store = event_store
    dag_service = DagOrchestrator(dag, transport, idemp_store, result_store=result_store, event_store=event_store)

    # start DagOrchestrator (seed roots and start result subscription)
    await dag_service.start()

    # start external workers (they read tasks via transport)
    worker1 = WorkflowWorker(worker_transport, dag.handlers, idemp_store, result_store=result_store, worker_id="w1")
    worker2 = WorkflowWorker(worker2_transport, dag.handlers, idemp_store, result_store=result_store, worker_id="w2")

    # run workers in background
    wtask1 = asyncio.create_task(worker1.run())
    wtask2 = asyncio.create_task(worker2.run())

    # wait for workflow to finish (DagOrchestrator watches results and updates DAG)
    await dag_service.wait_until_finished()

    # stop workers & close transport to end their subscribe_tasks loops
    # (InMemoryTransport supports a close_tasks helper)
    if isinstance(transport, InMemoryTransport):
        await transport.close_tasks()
        await transport.close_results()  # close result subscription as well

    # await asyncio.gather(wtask1, wtask2, return_exceptions=True)

    # collect outputs and events
    results = dag_service.collect_results()
    events = await event_store.list_events(dag.workflow_id)

    print("=== RESULTS ===")
    print(json.dumps(results, indent=2))
    print("\n=== EVENTS ===")
    for e in events:
        print(e.model_dump())

if __name__ == "__main__":
    asyncio.run(main())
