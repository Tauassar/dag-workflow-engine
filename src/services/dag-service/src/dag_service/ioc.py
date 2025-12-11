import logging
import uuid

from redis.asyncio import Redis

from dag_engine.core import WorkflowWorker
from dag_engine.core.handlers import hregistry
from dag_engine.core.manager import WorkflowManager
from dag_engine.event_store import RedisEventStore
from dag_engine.idempotency_store import RedisIdempotencyStore
from dag_engine.result_store import RedisResultStore
from dag_engine.transport import RedisTransport
from .config import Settings, settings
from .store import WorkflowDefinitionStore


logger = logging.getLogger(__name__)


class AppContainer:
    manager: WorkflowManager
    worker: WorkflowWorker

    def __init__(self, config: Settings):
        self.redis = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)
        self._id = uuid.uuid4().hex

        self.definition_store = WorkflowDefinitionStore(self.redis)
        self.result_store = RedisResultStore(self.redis)
        self.idempotency_store = RedisIdempotencyStore(self.redis)
        self.event_store = RedisEventStore(self.redis)
        self.orchestrator_transport = RedisTransport(
            self.redis,
            tasks_stream="engine:tasks",
            results_stream="engine:results",
            task_group="engine-task-group",
            result_group="engine-result-group",
            consumer_name="controller",   # for DagOrchestrator
        )
        self.worker_transport = RedisTransport(
            self.redis,
            tasks_stream="engine:tasks",
            results_stream="engine:results",
            task_group="engine-task-group",
            result_group="engine-result-group",
            consumer_name=f"worker-{self._id}",
        )

    async def create_workflow_manager(self) -> WorkflowManager:
        return WorkflowManager(
            transport=self.orchestrator_transport,
            result_store=self.result_store,
            idempotency_store=self.idempotency_store,
        )

    async def create_workflow_worker(self) -> WorkflowWorker:
        return WorkflowWorker(
            self.worker_transport,
            hregistry.handlers,
            self.idempotency_store,
            result_store=self.result_store,
            worker_id=f"w{self._id}"
        )

    async def init_orchestrator(self):
        self.manager = await self.create_workflow_manager()
        await self.orchestrator_transport.init()
        logger.info("Initialized workflow manager")
        logger.info("Initialized AppContainer")

    async def init_worker(self) -> None:
        self.worker = await self.create_workflow_worker()
        await self.worker_transport.init()
        logger.info("Initialized workflow manager")
        logger.info("Initialized AppContainer")

    async def shutdown(self):
        await self.redis.shutdown(save=True)
        logger.info("Shut down Redis client")
        logger.info("Shut down AppContainer")


container = AppContainer(settings)


async def get_container() -> AppContainer:
    return container
