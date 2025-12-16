from __future__ import annotations

import asyncio
import time

from .protocol import DependencyCounterStore


class InMemCounterStore(DependencyCounterStore):
    """
    In-memory DependencyCounterStore.

    Characteristics:
    - SETNX semantics for init_counter
    - Atomic decrement via asyncio.Lock
    - Optional TTL support (best-effort)
    - Single-process only (for tests / dev)
    """

    def __init__(self, prefix: str = "depcount"):
        self.prefix = prefix
        self._values: dict[str, int] = {}
        self._expires_at: dict[str, float] = {}
        self._lock = asyncio.Lock()

    # -----------------------------------------------------
    def _key(self, workflow_id: str, node_id: str) -> str:
        return f"{self.prefix}:{workflow_id}:{node_id}"

    def _is_expired(self, key: str) -> bool:
        exp = self._expires_at.get(key)
        return exp is not None and time.time() >= exp

    # -----------------------------------------------------
    async def init_counter(
        self,
        workflow_id: str,
        node_id: str,
        initial: int,
        ttl_seconds: int | None = None,
    ) -> None:
        """
        SETNX semantics:
        - If key exists (and not expired), do nothing
        - Else create with initial value
        """
        key = self._key(workflow_id, node_id)

        async with self._lock:
            if key in self._values:
                if not self._is_expired(key):
                    return
                # expired → clean up
                self._values.pop(key, None)
                self._expires_at.pop(key, None)

            self._values[key] = int(initial)

            if ttl_seconds is not None:
                self._expires_at[key] = time.time() + ttl_seconds

    # -----------------------------------------------------
    async def decrement(
        self,
        workflow_id: str,
        node_id: str,
    ) -> int:
        """
        Atomic decrement.
        Raises KeyError if counter missing (same as Redis behavior if key deleted).
        """
        key = self._key(workflow_id, node_id)

        async with self._lock:
            if key not in self._values or self._is_expired(key):
                raise KeyError(f"Dependency counter not initialized: {key}")

            self._values[key] -= 1
            return self._values[key]

    # -----------------------------------------------------
    async def get(
        self,
        workflow_id: str,
        node_id: str,
    ) -> int | None:
        key = self._key(workflow_id, node_id)

        async with self._lock:
            if key not in self._values or self._is_expired(key):
                self._values.pop(key, None)
                self._expires_at.pop(key, None)
                return None

            return self._values[key]

    # -----------------------------------------------------
    async def delete(
        self,
        workflow_id: str,
        node_id: str,
    ) -> None:
        key = self._key(workflow_id, node_id)

        async with self._lock:
            self._values.pop(key, None)
            self._expires_at.pop(key, None)
