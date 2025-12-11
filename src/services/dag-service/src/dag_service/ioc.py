import logging
import uuid

from dag_engine.core import WorkflowWorker
from dag_engine.core.handlers import hregistry
from dag_engine.core.manager import WorkflowManager
from dag_engine.store.events import RedisEventStore
from dag_engine.store.execution import RedisExecutionStore
from dag_engine.store.idempotency import RedisIdempotencyStore
from dag_engine.store.results import RedisResultStore
from dag_engine.transport import RedisTransport
from redis.asyncio import Redis

from .config import Settings
from .store import WorkflowDefinitionStore

logger = logging.getLogger(__name__)


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

        # Stores
        self.definition_store = WorkflowDefinitionStore(self.redis)
        self.result_store = RedisResultStore(self.redis)
        self.idempotency_store = RedisIdempotencyStore(self.redis)
        self.event_store = RedisEventStore(self.redis)
        self.execution_store = RedisExecutionStore(self.redis)
        self.orchestrator_transport = RedisTransport(
            self.redis,
            tasks_stream="engine:tasks",
            results_stream="engine:results",
            task_group="engine-task-group",
            result_group="engine-result-group",
            consumer_name="controller",
        )
        self.worker_transport = RedisTransport(
            self.redis,
            tasks_stream="engine:tasks",
            results_stream="engine:results",
            task_group="engine-task-group",
            result_group="engine-result-group",
            consumer_name=f"worker-{self._id}",
        )

        # Lazy initialized objects
        self.manager: WorkflowManager | None = None
        self.worker: WorkflowWorker | None = None

    async def init_orchestrator(self):
        """Init streams + manager"""
        await self.orchestrator_transport.init()
        self.manager = await self.create_workflow_manager()
        logger.info("Orchestrator initialized")

    async def init_worker(self):
        """Init worker-side streams"""
        await self.worker_transport.init()
        self.worker = await self.create_workflow_worker()
        logger.info("Worker initialized")

    async def create_workflow_manager(self) -> WorkflowManager:
        return WorkflowManager(
            transport=self.orchestrator_transport,
            result_store=self.result_store,
            execution_store=self.execution_store,
            idempotency_store=self.idempotency_store,
            event_store=self.event_store,
        )

    async def create_workflow_worker(self) -> WorkflowWorker:
        return WorkflowWorker(
            transport=self.worker_transport,
            handler_registry=hregistry.handlers,
            idempotency_store=self.idempotency_store,
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
