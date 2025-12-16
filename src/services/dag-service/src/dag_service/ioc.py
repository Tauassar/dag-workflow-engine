import logging
import uuid

from dag_engine.core import WorkflowWorker
from dag_engine.core import hregistry
from dag_engine.core.manager import WorkflowManager
from dag_engine.core.timeout_monitor import GlobalTimeoutMonitor
from dag_engine.event_sourcing import RedisEventStore, WorkflowEvent, EventBus
from dag_engine.store.atomic_counter import RedisDependencyCounterStore
from dag_engine.store.execution import RedisExecutionStore
from dag_engine.store.idempotency import RedisIdempotencyStore
from dag_engine.store.results import RedisResultStore
from dag_engine.transport import RedisConsumer, RedisPublisher
from redis.asyncio import Redis

from .config import Settings
from .store import WorkflowDefinitionStore

logger = logging.getLogger(__name__)


class EventsRedisConsumer:
    def __init__(self, redis_consumer: RedisConsumer, event_bus: EventBus):
        self.event_bus = event_bus
        self.consumer = redis_consumer

    async def consume(self):
        async for message in self.consumer.subscribe():
            logger.debug("Event received message: %s", message)
            event = WorkflowEvent.model_validate(message)
            await self.event_bus.handle_event(event)


class AppContainer:
    """
    Fully testable IoC container.

    In production → real Redis.
    In tests → fakeredis injected.
    """

    def __init__(self, config: Settings, redis_client: Redis | None = None):
        self._id = uuid.uuid4().hex

        # Allow injection of fakeredis in tests
        self.redis: Redis = redis_client or Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True,
        )

        self._EVENTS_STREAM = config.EVENTS_STREAM

        # Stores
        self.definition_store = WorkflowDefinitionStore(self.redis)
        self.result_store = RedisResultStore(self.redis)
        self.idempotency_store = RedisIdempotencyStore(self.redis)
        self.event_store = RedisEventStore(self.redis)
        self.execution_store = RedisExecutionStore(self.redis)

        # transport
        self.atomic_counter = RedisDependencyCounterStore(self.redis)
        self.publisher = RedisPublisher(self.redis, self._EVENTS_STREAM)
        self.consumer = RedisConsumer(self.redis, self._EVENTS_STREAM)
        self.event_bus = EventBus(
            self.publisher,
            self.event_store,
        )
        self.monitor: GlobalTimeoutMonitor = GlobalTimeoutMonitor(
            idempotency_store=self.idempotency_store,
            event_bus=self.event_bus,
        )

        # Lazy initialized objects
        self.manager: WorkflowManager | None = None
        self.worker: WorkflowWorker | None = None
        self.consumer: RedisConsumer | None = None
        self.events_consumer: EventsRedisConsumer | None = None

    async def init_orchestrator(self):
        """Init streams + manager"""
        self.consumer = RedisConsumer(
            self.redis,
            self._EVENTS_STREAM,
            groupname="orchestrator_group",
            consumer_name=f"orchestrator_consumer_{uuid.uuid4()}",
        )
        self.events_consumer = EventsRedisConsumer(self.consumer, self.event_bus)
        self.manager = await self.create_workflow_manager()
        logger.info("Orchestrator initialized")

    async def init_worker(self):
        """Init worker-side streams"""
        self.consumer = RedisConsumer(
            self.redis,
            self._EVENTS_STREAM,
            groupname="worker_group",
            consumer_name=f"worker_consumer_{uuid.uuid4()}",
        )
        self.events_consumer = EventsRedisConsumer(self.consumer, self.event_bus)
        self.worker = await self.create_workflow_worker()
        logger.info("Worker initialized")

    async def create_workflow_manager(self) -> WorkflowManager:
        return WorkflowManager(
            event_bus=self.event_bus,
            result_store=self.result_store,
            execution_store=self.execution_store,
            idempotency_store=self.idempotency_store,
            event_store=self.event_store,
            atomic_counter=self.atomic_counter,
        )

    async def create_workflow_worker(self) -> WorkflowWorker:
        return WorkflowWorker(
            self.event_bus,
            hregistry.handlers,
            self.idempotency_store,
            result_store=self.result_store,
            worker_id=f"w{self._id}",
        )

    async def shutdown(self):
        try:
            await self.redis.close()  # works on fakeredis + redis-py
        except Exception:
            pass
        logger.info("Redis client closed")


def scoper_container():
    from .config import settings

    container = AppContainer(settings)

    def _get_container() -> AppContainer:
        """
        In production this returns the real container,
        but tests override it with a factory.
        """
        return container

    return _get_container


get_container = scoper_container()
