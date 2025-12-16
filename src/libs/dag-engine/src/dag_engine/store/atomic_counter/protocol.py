from __future__ import annotations

from abc import ABC, abstractmethod


class DependencyCounterStore(ABC):
    """
    Atomic dependency counter for fan-in synchronization.

    One counter per (workflow_id, node_id).
    """

    @abstractmethod
    async def init_counter(
        self,
        workflow_id: str,
        node_id: str,
        initial: int,
        ttl_seconds: int | None = None,
    ) -> None:
        """
        Initialize counter if not exists.
        Must be idempotent.
        """
        ...

    @abstractmethod
    async def decrement(
        self,
        workflow_id: str,
        node_id: str,
    ) -> int:
        """
        Atomically decrement counter by 1.
        Returns the new value AFTER decrement.
        """
        ...

    @abstractmethod
    async def get(
        self,
        workflow_id: str,
        node_id: str,
    ) -> int | None:
        """
        Read current value (debug / observability).
        """
        ...

    @abstractmethod
    async def delete(
        self,
        workflow_id: str,
        node_id: str,
    ) -> None:
        """
        Cleanup after node becomes terminal.
        """
        ...
