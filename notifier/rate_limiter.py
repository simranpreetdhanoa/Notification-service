"""Token-bucket rate limiter (one bucket per downstream provider)."""
from __future__ import annotations

import threading
import time


class TokenBucket:
    """Allows ``rate`` operations/second with bursts up to ``capacity``.

    ``try_acquire`` never blocks: it returns 0.0 when a token was taken, or the
    number of seconds until one will be available. Callers use that hint to
    defer work instead of parking a worker thread on a sleep.
    """

    def __init__(self, rate: float, capacity: float | None = None, clock=time.monotonic) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        if self.capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._tokens = self.capacity
        self._clock = clock
        self._last = clock()
        self._lock = threading.Lock()

    def try_acquire(self, tokens: float = 1.0) -> float:
        with self._lock:
            now = self._clock()
            self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
            self._last = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return 0.0
            return (tokens - self._tokens) / self.rate

    def acquire(self, tokens: float = 1.0, timeout: float | None = None) -> bool:
        """Blocking variant; returns False if ``timeout`` elapses first."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            wait = self.try_acquire(tokens)
            if wait == 0.0:
                return True
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait = min(wait, remaining)
            time.sleep(wait)
