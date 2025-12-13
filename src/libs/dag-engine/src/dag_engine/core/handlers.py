import asyncio
import typing as t

from .registry import BaseRegistry
from dag_engine.transport import TaskMessage

Handler = t.Callable[[TaskMessage], t.Awaitable[t.Any]]


class HandlerRegistry(BaseRegistry[Handler]):
    def _register_handler(self, node_type: str, handler: Handler) -> None:
        if not asyncio.iscoroutinefunction(handler):
            raise ValueError("handler must be async")
        super()._register_handler(node_type, handler)


hregistry = HandlerRegistry()
