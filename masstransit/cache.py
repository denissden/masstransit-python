import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Callable


@dataclass
class MtCache:
    ttl_seconds: float = 60.
    ttl_check_period: float = 5.
    on_delete: Callable[[Any, Any], None] = None
    _cache: dict[Any, tuple[float, Any]] = field(default_factory=dict)
    _running: bool = True

    def __post_init__(self):
        asyncio.create_task(self._expire_loop())

    def set(self, key: Any, value: Any):
        self._cache[key] = (time.time(), value)

    def get(self, key: Any) -> Optional[Any]:
        value = self._cache.get(key)
        return value and value[1] or None

    def delete(self, key: Any, silent=False):
        value = self._cache.pop(key, None)
        if self.on_delete is not None and not silent:
            self.on_delete(key, value and value[1] or None)

    async def _expire_loop(self):
        while self._running:
            self._expire_job()
            await asyncio.sleep(self.ttl_check_period)

    def _expire_job(self):
        start_time = time.time()
        to_delete = set()
        for key, (t, value) in self._cache.items():
            if t + self.ttl_seconds < start_time:
                to_delete.add(key)

        for key in to_delete:
            self.delete(key)