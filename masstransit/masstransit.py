from abc import ABC
from dataclasses import dataclass

import aio_pika
from aio_pika import ExchangeType

from .publisher import PublisherABC, RabbitMQPublisher
from .consumer import ConsumerABC, RabbitMQConsumer
from . import decorators


@dataclass
class MassTransit(ABC):
    publisher: PublisherABC
    consumer: ConsumerABC

    def start_base(self):
        for mt_type, handler in decorators.HANDLERS:
            self.consumer.handle(mt_type, handler)


@dataclass
class RabbitMQMassTransit(MassTransit):
    connection: aio_pika.abc.AbstractRobustConnection = None
    channel: aio_pika.abc.AbstractRobustChannel = None

    @classmethod
    async def connect(cls,
                      connection_string: str,
                      contracts_namespace: str,
                      consume_queue_exchange: str,
                      consume_queue: str,
                      prefetch_count=5):
        connection = await aio_pika.connect_robust(
            connection_string
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=prefetch_count)
        queue = await channel.declare_queue(consume_queue, durable=True)
        exchange = await channel.declare_exchange(consume_queue_exchange, type=ExchangeType.FANOUT, durable=True)
        await queue.bind(exchange)

        consumer = RabbitMQConsumer(contracts_namespace, queue=queue)
        publisher = RabbitMQPublisher(contracts_namespace, channel=channel)

        mt = cls(
            publisher=publisher,
            consumer=consumer,
            connection=connection,
            channel=channel)
        mt.start_base()
        return mt
