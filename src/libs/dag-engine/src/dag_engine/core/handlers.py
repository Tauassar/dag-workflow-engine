import asyncio
import typing as t

from dag_engine.transport import TaskMessage

Handler = t.Callable[[TaskMessage], t.Awaitable[t.Any]]


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers_by_type: dict[str, Handler] = {}

    @property
    def handlers(self):
        return self._handlers_by_type

    def _register_handler(self, node_type: str, handler: Handler) -> None:
        if not asyncio.iscoroutinefunction(handler):
            raise ValueError("handler must be async")
        self._handlers_by_type[node_type] = handler

    def handler(self, node_type: str):
        def decorator(func: Handler):
            self._register_handler(node_type, func)
            return func

        return decorator


hregistry = HandlerRegistry()
