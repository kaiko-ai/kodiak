from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from typing import Any, Deque, Mapping, Optional

import structlog

logger = structlog.get_logger()


class Throttler:
    """
    Redis-backed sliding window rate limiter.

    Uses Redis INCR with a per-hour-bucket key to enforce rate limits across
    multiple pods. Falls back to the local in-memory implementation if Redis
    is unavailable.

    via https://github.com/hallazzang/asyncio-throttle (original in-memory version)

    The MIT License (MIT)

    Copyright (c) 2017-2019 Hanjun Kim

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
    """

    def __init__(
        self,
        rate_limit: float,
        period: float = 1.0,
        retry_interval: float = 0.01,
        installation_id: str = "",
        redis_client: Any = None,
    ) -> None:
        self.rate_limit = rate_limit
        self.period = period
        self.retry_interval = retry_interval
        self.installation_id = installation_id
        self._redis: Any = redis_client

        # Fallback local state
        self._task_logs: Deque[float] = deque()

    def _hour_bucket(self) -> int:
        return int(time.time()) // 3600

    def _redis_key(self) -> str:
        return f"throttle:{self.installation_id}:{self._hour_bucket()}"

    async def _try_redis_acquire(self) -> Optional[bool]:
        """
        Try to acquire using Redis. Returns True if acquired, False if over
        limit, None if Redis is unavailable.
        """
        if self._redis is None:
            return None
        try:
            key = self._redis_key()
            # hourly rate limit
            hourly_limit = int(self.rate_limit * 3600 / self.period) if self.period != 3600 else int(self.rate_limit)
            count = await self._redis.incr(key)
            if count == 1:
                # First request in this bucket — set TTL
                await self._redis.expire(key, 3700)
            return bool(count <= hourly_limit)
        except (OSError, ConnectionError, TimeoutError):
            logger.debug("throttle_redis_fallback", exc_info=True)
            return None

    def flush(self) -> None:
        now = time.time()
        while self._task_logs:
            if now - self._task_logs[0] > self.period:
                self._task_logs.popleft()
            else:
                break

    async def _local_acquire(self) -> None:
        while True:
            self.flush()
            if len(self._task_logs) < self.rate_limit:
                break
            await asyncio.sleep(self.retry_interval)
        self._task_logs.append(time.time())

    async def acquire(self) -> None:
        result = await self._try_redis_acquire()
        if result is True:
            return
        if result is False:
            # Over limit — wait and retry
            await asyncio.sleep(1.0)
            while True:
                result = await self._try_redis_acquire()
                if result is True:
                    return
                if result is None:
                    # Redis down, fall back
                    break
                await asyncio.sleep(1.0)

        # Fallback to local rate limiting
        await self._local_acquire()

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        pass


def _get_redis_client() -> Any:
    """Lazily import redis_bot to avoid circular imports."""
    try:
        from kodiak.redis_client import redis_bot

        return redis_bot
    except ImportError:
        return None


# installation_id => Throttler
THROTTLER_CACHE: Mapping[str, Throttler] = defaultdict(
    # TODO(chdsbd): Store rate limits in redis and update via http rate limit response headers
    lambda: Throttler(rate_limit=5000 / 60 / 60)
)


def get_thottler_for_installation(*, installation_id: str) -> Throttler:
    throttler = THROTTLER_CACHE[installation_id]
    # Ensure Redis client is set (lazy init to avoid circular imports)
    if throttler._redis is None:
        throttler._redis = _get_redis_client()
        throttler.installation_id = installation_id
    return throttler
