import datetime
import json
import platform
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable

import aio_pika


@dataclass()
class PublisherABC(ABC):
    contracts_namespace: str = ""

    def wrap_message(self, content, mt_type: str, case_converter=None):
        converted_mt_type = mt_type
        if case_converter is not None:
            converted_mt_type = case_converter(mt_type)

        return {
            "messageId": str(uuid.uuid4()),
            "conversationId": str(uuid.uuid4()),
            "correlationId": str(uuid.uuid4()),
            "messageType": [
                f"urn:message:{self.contracts_namespace}:{converted_mt_type}"
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
    async def publish(self, message, mt_type: str, exchange_name: str, case_converter: Callable[[str], str] | None = None) -> dict[str, Any]:
        pass


@dataclass()
class RabbitMQPublisher(PublisherABC):
    channel: aio_pika.abc.AbstractRobustChannel = None

    async def publish(self, message, mt_type: str, exchange_name: str, case_converter: Callable[[str], str] | None = None):
        mt_message = self.wrap_message(message, mt_type, case_converter)

        msg = aio_pika.Message(
            body=json.dumps(mt_message, indent=4).encode('utf-8'),
        )
        exchange = await self.channel.get_exchange(exchange_name, ensure=True)
        await exchange.publish(msg, routing_key=mt_type)
        return mt_message
