from abc import ABC, abstractmethod
from asyncio import Future
from dataclasses import dataclass, field
from ..cache import MtCache
from ..masstransit import MassTransit


def _get_static_mt():
    from .. import static
    return static


@dataclass
class MessageProvider:
    mt_type: str
    mt: MassTransit = field(default_factory=_get_static_mt)

    async def execute(self, message: dict):
        await self.mt.publisher.publish(message, self.mt_type)