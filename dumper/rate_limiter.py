"""Async token bucket — the ONE global throttle for all RapidAPI calls.

Every worker takes a token before each HTTP attempt; tokens refill at
`DUMP_RATE_PER_SEC` (clamped to the plan's hard limit). This replaces the old
per-call fixed sleep and lets N concurrent workers share the budget exactly.
"""

import asyncio
import time
from typing import Optional

from config import settings


class TokenBucket:
    def __init__(self, rate: float, burst: Optional[float] = None):
        self.rate = max(0.1, float(rate))
        # Burst = 1s worth of tokens; enough to absorb gather() pairs without spiking past the plan limit.
        self.capacity = float(burst) if burst is not None else max(1.0, self.rate)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available, then consume it."""
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(wait)


_bucket: Optional[TokenBucket] = None


def configure_bucket(rate: Optional[float] = None) -> TokenBucket:
    """(Re)create the global bucket — called once at run start (CLI --rate overrides config)."""
    global _bucket
    effective = min(float(rate), float(settings.RAPIDAPI_RATE_LIMIT_PER_SEC)) if rate else settings.effective_rate_per_sec
    _bucket = TokenBucket(effective)
    return _bucket


def get_bucket() -> TokenBucket:
    global _bucket
    if _bucket is None:
        _bucket = TokenBucket(settings.effective_rate_per_sec)
    return _bucket
