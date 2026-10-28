"""Mock downstream providers (stand-ins for Twilio / SES / SNS-Mobile-Push).

Each mock enforces its *own* hard rate limit and answers 429 when exceeded,
so tests can prove the dispatcher's client-side limiter keeps us under it.
"""
from __future__ import annotations

import random
import threading
import time
import uuid
from collections import defaultdict, deque

from .models import Channel, Notification
from .rate_limiter import TokenBucket


class ProviderError(Exception):
    retryable = True


class TransientProviderError(ProviderError):
    """5xx, timeouts, connection resets: worth retrying."""


class ThrottledError(TransientProviderError):
    """HTTP 429. ``retry_after`` mirrors the Retry-After header."""

    def __init__(self, message: str, retry_after: float = 0.0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PermanentProviderError(ProviderError):
    """4xx such as an invalid phone number or unsubscribed device: never retry."""

    retryable = False


class MockProvider:
    def __init__(
        self,
        channel: Channel,
        max_per_second: float,
        burst: float | None = None,
        failure_rate: float = 0.0,
        latency: tuple[float, float] = (0.005, 0.02),
        seed: int | None = None,
    ) -> None:
        self.channel = channel
        self._limit = TokenBucket(max_per_second, burst)
        self._failure_rate = failure_rate
        self._latency = latency
        self._rng = random.Random(seed)
        self._lock = threading.Lock()
        self._delivered: dict[str, str] = {}     # idempotency token -> message id
        self._scripted: dict[str, deque] = defaultdict(deque)
        self.deliveries: list[tuple[float, Notification]] = []  # actually delivered
        self.calls = 0
        self.throttled = 0
        self.duplicates_suppressed = 0

    def script(self, recipient: str, *errors: Exception) -> None:
        """Make the next len(errors) sends to ``recipient`` raise these, in order."""
        with self._lock:
            self._scripted[recipient].extend(errors)

    def send(self, n: Notification, idempotency_token: str) -> str:
        with self._lock:
            self.calls += 1
            latency = self._rng.uniform(*self._latency)
        time.sleep(latency)

        wait = self._limit.try_acquire()
        if wait > 0:
            with self._lock:
                self.throttled += 1
            raise ThrottledError(f"{self.channel.value}: 429 Too Many Requests", wait)

        with self._lock:
            if idempotency_token in self._delivered:
                self.duplicates_suppressed += 1
                return self._delivered[idempotency_token]
            if self._scripted[n.recipient]:
                raise self._scripted[n.recipient].popleft()
            if n.recipient.startswith("invalid"):
                raise PermanentProviderError(f"{self.channel.value}: 400 invalid recipient")
            if self._rng.random() < self._failure_rate:
                raise TransientProviderError(f"{self.channel.value}: 503 Service Unavailable")
            message_id = f"{self.channel.value}-{uuid.uuid4().hex[:12]}"
            self._delivered[idempotency_token] = message_id
            self.deliveries.append((time.monotonic(), n))
            return message_id
