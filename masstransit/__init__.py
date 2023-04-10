from .masstransit import MassTransit, RabbitMQMassTransit
from .cache import MtCache
from .providers import RpcProvider, rpc_call, MessageProvider
from .publisher import PublisherABC, RabbitMQPublisher
from .consumer import ConsumerABC, RabbitMQConsumer
from .decorators import consume

static: MassTransit = None


async def connect_rabbitmq(
        connection_string: str,
        contracts_namespace: str,
        consume_queue_exchange: str,
        consume_queue: str) -> RabbitMQMassTransit:
    global static
    static = await RabbitMQMassTransit.connect(
        connection_string,
        contracts_namespace,
        consume_queue_exchange,
        consume_queue)
    return static

