import asyncio
import json
import logging
import typing as t
import itertools
from collections import defaultdict
from dataclasses import dataclass
from .protocols import Publisher, Consumer

logger = logging.getLogger(__name__)

# =====================
# Internal stream model
# =====================

MessageId = int


@dataclass
class _Message:
    id: MessageId
    payload: dict


class _InMemoryStream:
    """
    Single stream with Redis-Streams-like consumer groups.
    """
    def __init__(self):
        self._messages: list[_Message] = []
        self._id_seq = itertools.count(1)

        self._group_offsets: dict[str, int] = defaultdict(int)


    async def publish(self, payload: dict) -> None:
        msg = _Message(
            id=next(self._id_seq),
            payload=payload,
        )
        self._messages.append(msg)

    async def read(
        self,
        *,
        group: str,
    ) -> _Message | None:
        offset = self._group_offsets[group]
        if offset < len(self._messages):
            msg = self._messages[offset]
            self._group_offsets[group] += 1
            return msg

    async def ack_and_delete(self, msg_id: MessageId) -> None:
        self._messages = [m for m in self._messages if m.id != msg_id]


# =====================
# Stream registry
# =====================

_STREAMS: dict[str, _InMemoryStream] = defaultdict(_InMemoryStream)


def _get_stream(name: str) -> _InMemoryStream:
    return _STREAMS[name]


class InMemoryPublisher(Publisher):
    """
    In-memory analog of RedisPublisher.
    """
    def __init__(self, stream: str):
        self._stream = stream
        self._backend = _get_stream(stream)

    async def publish(self, result: str) -> None:
        logger.debug("Publishing message to %s", self._stream)
        await self._backend.publish({"json": json.loads(result)})


class InMemoryConsumer(Consumer):
    """
    In-memory analog of RedisConsumer.
    """
    def __init__(
        self,
        stream: str,
        groupname: str = "group",
        consumer_name: str = "consumer",
    ):
        self._stream = stream
        self._group = groupname
        self.consumer_name = consumer_name
        self._backend = _get_stream(stream)

    async def subscribe(self) -> t.AsyncIterator[dict]:
        while True:
            msg = await self._backend.read(
                group=self._group,
            )

            if msg is None:
                continue

            try:
                raw = msg.payload.get("json")
                yield raw
            except Exception as e:
                logger.warning("decode failure: %s", e)
            finally:
                await self._backend.ack_and_delete(msg.id)

    async def get_all_messages(self) -> list[dict]:
        messages = []
        msg = await self._backend.read(
            group=self._group,
        )

        while msg is not None:
            try:
                raw = msg.payload.get("json")
                messages.append(raw)
            except Exception as e:
                logger.warning("decode failure: %s", e)
            finally:
                await self._backend.ack_and_delete(msg.id)

            msg = await self._backend.read(
                group=self._group,
            )

        return messages
