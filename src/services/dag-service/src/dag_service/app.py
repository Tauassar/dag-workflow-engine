from contextlib import asynccontextmanager

from fastapi import FastAPI

from .config import settings
from .ioc import get_container
from .routes.v1 import v1_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    await get_container().init_orchestrator()
    yield
    await get_container().shutdown()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION,
    openapi_url=(f"{settings.API_STR}/openapi.json" if settings.API_ENABLE_DOCS else None),
    docs_url=(f"{settings.API_STR}/docs" if settings.API_ENABLE_DOCS else None),
    lifespan=lifespan,
)
app.include_router(v1_router, prefix=settings.API_STR)
