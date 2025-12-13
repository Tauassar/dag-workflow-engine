from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid

from dag_engine.core import WorkflowWorker, WorkflowManager, WorkflowDefinition
from dag_engine.core import hregistry
from dag_engine.event_sourcing import WorkflowEvent
from dag_engine.event_sourcing.bus import EventBus
from dag_engine.event_sourcing.store import RedisEventStore
from dag_engine.store.execution import RedisExecutionStore
from dag_engine.store.idempotency import RedisIdempotencyStore
from dag_engine.store.results import RedisResultStore
from dag_engine.transport import TaskMessage
from redis.asyncio import Redis

from dag_engine.transport.consumer import RedisConsumer, RedisPublisher
from .core.workflow import WorkflowDAG

_EVENTS_STREAM = "events"
logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s [%(levelname)s] " "%(filename)s:%(lineno)d (%(funcName)s) — %(message)s"

logging.basicConfig(
    level=logging.DEBUG,
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class RedisOrchestratorConsumer:
    def __init__(self, redis_consumer: RedisConsumer, event_bus: EventBus):
        self.event_bus = event_bus
        self.consumer = redis_consumer

    async def consume(self):
        async for message in self.consumer.subscribe():
            logger.debug("Coordinator received message: %s", message)
            event = WorkflowEvent.model_validate(message)
            await self.event_bus.handle_event(event)


class RedisWorkerConsumer:
    def __init__(self, redis_consumer: RedisConsumer, event_bus: EventBus):
        self.event_bus = event_bus
        self.consumer = redis_consumer

    async def consume(self):
        async for message in self.consumer.subscribe():
            logger.debug("Worker received message: %s", message)
            event = WorkflowEvent.model_validate(message)
            await self.event_bus.handle_event(event)


_redis = Redis(host="localhost", port=6379, decode_responses=True)
_redis_orchestrator_consumer = RedisOrchestratorConsumer(RedisConsumer(_redis, _EVENTS_STREAM), EventBus(RedisPublisher(_redis, _EVENTS_STREAM), RedisEventStore(_redis),))
_redis_worker_consumer1 = RedisWorkerConsumer(
    RedisConsumer(
        _redis,
        _EVENTS_STREAM,
        groupname="worker_group",
        consumer_name=f"worker_consumer_{uuid.uuid4()}",
    ),
    EventBus(
        RedisPublisher(
            _redis,
            _EVENTS_STREAM,
        ),
        RedisEventStore(_redis),
    )
)
_redis_worker_consumer2 = RedisWorkerConsumer(
    RedisConsumer(
        _redis,
        _EVENTS_STREAM,
        groupname="worker_group",
        consumer_name=f"worker_consumer_{uuid.uuid4()}",
    ),
    EventBus(
        RedisPublisher(
            _redis,
            _EVENTS_STREAM,
        ),
        RedisEventStore(_redis),
    )
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
        "handler": "call_external_service",
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

dag = WorkflowDAG.from_dict(json.loads(USER_JSON), workflow_id=str(uuid.uuid4()))


@hregistry.handler("input")
async def input_handler(task: TaskMessage):
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


async def main():
    result_store = RedisResultStore(_redis)
    idemp_store = RedisIdempotencyStore(_redis)
    manager = WorkflowManager(
        event_bus=_redis_orchestrator_consumer.event_bus,
        result_store=RedisResultStore(_redis),
        execution_store=RedisExecutionStore(_redis),
        idempotency_store=RedisIdempotencyStore(_redis),
    )
    async def runner():
        await asyncio.sleep(1)
        await manager.start_workflow(str(uuid.uuid4()), WorkflowDefinition.model_validate(json.loads(USER_JSON), by_alias=True))

    # start external workers (they read tasks via transport)
    worker1 = WorkflowWorker(
        _redis_worker_consumer1.event_bus, hregistry.handlers, idemp_store, result_store=result_store, worker_id="w1"
    )
    worker2 = WorkflowWorker(
        _redis_worker_consumer2.event_bus, hregistry.handlers, idemp_store, result_store=result_store, worker_id="w2"
    )

    # run in background
    await asyncio.gather(
        _redis_orchestrator_consumer.consume(),
        runner(),
        _redis_worker_consumer1.consume(),
        _redis_worker_consumer2.consume(),
    )

    # events = await event_store.list_events(dag.workflow_id)

    logger.info("=== RESULTS ===")
    # logger.info(json.dumps(results, indent=2))
    logger.info("\n=== EVENTS ===")
    # for e in events:
    #     logger.info(e.model_dump())


if __name__ == "__main__":
    asyncio.run(main())
