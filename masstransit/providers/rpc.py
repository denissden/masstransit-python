from abc import ABC, abstractmethod
from asyncio import Future
from dataclasses import dataclass, field
from ..cache import MtCache
from ..masstransit import MassTransit


def _get_static_mt():
    from .. import static
    return static


@dataclass
class RpcProvider:
    mt_request_type: str
    mt_response_type: str
    timeout_seconds: float = 5
    check_timeout_period: float = 0.5
    mt: MassTransit = field(default_factory=_get_static_mt)
    _futures: MtCache = None

    def __post_init__(self):
        self._futures = MtCache(
            ttl_seconds=self.timeout_seconds,
            ttl_check_period=self.check_timeout_period,
            on_delete=self._on_future_delete
        )

        self.mt.consumer.handle(self.mt_response_type, self._resp_handler)

    async def _resp_handler(self, _, message, context):
        c_id = context["conversationId"]
        future = self._futures.get(c_id)
        if future:
            future.set_result(message)

    @staticmethod
    def _on_future_delete(key, future: Future):
        future.set_exception(TimeoutError())

    async def execute(self, request: dict) -> dict:
        ctx = await self.mt.publisher.publish(
            request,
            self.mt_request_type)

        future = Future()
        c_id = ctx["conversationId"]
        self._futures.set(c_id, future)
        await future
        self._futures.delete(c_id, silent=True)
        return future.result()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        print("exit")
        self.mt.consumer.unsubscribe(self.mt_response_type, self._resp_handler)


async def rpc_call(
        request: dict,
        mt_request_type: str,
        mt_response_type: str):
    with RpcProvider(mt_request_type, mt_response_type) as p:
        return await p.execute(request)
