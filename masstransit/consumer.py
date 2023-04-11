import asyncio
import datetime
import json
import logging
import platform
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Any, Awaitable

import aio_pika

logger = logging.getLogger(__name__)


HandlerType = Callable[[str, Any, Any], Awaitable[None]]


@dataclass()
class ConsumerABC(ABC):
    contracts_namespace: str = ""
    handlers: dict[str, list[HandlerType]] = field(default_factory=lambda: defaultdict(list))

    async def on_message(self, mt_type: str, message, context: Any):
        logger.debug("%s %s", mt_type, message, )

        handlers = self.handlers.get(mt_type)
        if not handlers:
            logger.warning("No handler for %s", mt_type)
            return

        for handler in handlers:
            await handler(mt_type, message, context)

    def handle(self, mt_type: str, handler: HandlerType):
        self.handlers[mt_type].append(handler)

    def unsubscribe(self, mt_type: str, handler: HandlerType):
        self.handlers[mt_type].remove(handler)


@dataclass()
class RabbitMQConsumer(ConsumerABC):
    queue: aio_pika.abc.AbstractRobustQueue = None

    def __post_init__(self):
        async def post_init():
            await self.queue.consume(self.on_rabbit_message)

        asyncio.ensure_future(post_init())

    async def on_rabbit_message(self, message: aio_pika.abc.AbstractIncomingMessage):
        async with message.process():
            body_dict = json.loads(message.body)
            message_types: list[str] = body_dict["messageType"]

            for t in message_types:
                mt_type = self._message_type_to_mt_type(t)
                await self.on_message(mt_type, body_dict["message"], body_dict)

    @staticmethod
    def _message_type_to_mt_type(t: str):
        return t.split(":")[-1]

