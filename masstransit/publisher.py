import datetime
import json
import platform
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import aio_pika

from .utils import to_camel_case


@dataclass()
class PublisherABC(ABC):
    contracts_namespace: str = ""

    def wrap_message(self, content, mt_type: str):
        return {
            "messageId": str(uuid.uuid4()),
            "conversationId": str(uuid.uuid4()),
            "correlationId": str(uuid.uuid4()),
            "messageType": [
                f"urn:message:{self.contracts_namespace}:{to_camel_case(mt_type)}"
            ],
            "message": content,
            "sentTime": datetime.datetime.utcnow().isoformat(),
            "headers": {},
            "host": {
                "machineName": platform.node(),
                "processName": "python_masstransit",
            }
        }

    @abstractmethod
    async def publish(self, message, mt_type: str) -> dict[str, Any]:
        pass


@dataclass()
class RabbitMQPublisher(PublisherABC):
    channel: aio_pika.abc.AbstractRobustChannel = None

    async def publish(self, message, mt_type: str):
        mt_message = self.wrap_message(message, mt_type)

        msg = aio_pika.Message(
            body=json.dumps(mt_message, indent=4).encode('utf-8'),
        )
        await self.channel.default_exchange.publish(msg, routing_key=mt_type)
        return mt_message