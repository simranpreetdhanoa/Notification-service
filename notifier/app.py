"""Wires store, providers, limiters and dispatcher together with defaults."""
from __future__ import annotations

from .dispatcher import Dispatcher
from .models import Channel
from .providers import MockProvider
from .rate_limiter import TokenBucket
from .retry import RetryPolicy
from .store import NotificationStore

# Provider-side hard limits (requests/second) for the mocks...
PROVIDER_LIMITS = {Channel.SMS: 10.0, Channel.EMAIL: 50.0, Channel.PUSH: 100.0}
# ...and the client-side budget we hold ourselves to: 80% headroom, small bursts.
CLIENT_HEADROOM = 0.8


def build_dispatcher(
    db_path: str = "notifications.db",
    workers: int = 8,
    failure_rate: float = 0.05,
    retry_policy: RetryPolicy = RetryPolicy(),
    provider_limits: dict[Channel, float] | None = None,
) -> Dispatcher:
    limits = provider_limits or PROVIDER_LIMITS
    store = NotificationStore(db_path)
    providers = {ch: MockProvider(ch, max_per_second=rate, failure_rate=failure_rate)
                 for ch, rate in limits.items()}
    limiters = {ch: TokenBucket(rate * CLIENT_HEADROOM, capacity=max(1.0, rate * CLIENT_HEADROOM / 2))
                for ch, rate in limits.items()}
    return Dispatcher(store, providers, limiters, retry_policy=retry_policy, workers=workers)
