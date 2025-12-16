import asyncio
from typing import TypeVar

T = TypeVar("T")


class BaseRegistry[T]:
    def __init__(self) -> None:
        self._handlers_by_type: dict[str, T] = {}

    @property
    def handlers(self):
        return self._handlers_by_type

    def _register_handler(self, node_type: str, handler: T) -> None:
        if not asyncio.iscoroutinefunction(handler):
            raise ValueError("handler must be async")
        self._handlers_by_type[node_type] = handler  # type: ignore[assignment]

    def handler(self, node_type: str):
        def decorator(func: T):
            self._register_handler(node_type, func)
            return func

        return decorator
