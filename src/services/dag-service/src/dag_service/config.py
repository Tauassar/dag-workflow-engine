import logging
import pathlib
import typing as t

import pydantic as pd
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BASE_DIR: pd.DirectoryPath = pathlib.Path(__file__).resolve().parent.parent

    PROJECT_NAME: str = "Event-Driven Workflow Orchestration Engine"
    PROJECT_VERSION: str = "0.0.1"

    API_STR: str = "/api"
    API_HOST: str = "0.0.0.0"  # nosec B104
    API_PORT: int = 8000
    API_ENABLE_DOCS: bool = True
    BACKEND_CORS_ORIGINS: list[pd.AnyHttpUrl] | str = []

    LOG_LEVEL: int = logging.DEBUG

    @pd.field_validator("BACKEND_CORS_ORIGINS", mode="plain")
    def assemble_cors_origins(cls, v: t.Any) -> list[pd.AnyHttpUrl]:
        if isinstance(v, str):
            return [pd.AnyHttpUrl(i.strip()) for i in v.removeprefix("[").removesuffix("]").split(" ")]
        elif isinstance(v, list):
            return v
        raise ValueError(v)

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int | str = 0


settings = Settings()
