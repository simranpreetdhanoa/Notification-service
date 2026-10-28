from notifier import Channel, Dispatcher, NotificationStore, RetryPolicy, TokenBucket
from notifier.providers import MockProvider

FAST_RETRY = RetryPolicy(max_attempts=4, base_delay=0.01, max_delay=0.05, jitter=False)


def make_dispatcher(workers=4, provider_rate=1000.0, client_rate=None, retry=FAST_RETRY,
                    db=":memory:", failure_rate=0.0, providers=None):
    store = NotificationStore(db)
    providers = providers or {
        ch: MockProvider(ch, max_per_second=provider_rate, failure_rate=failure_rate,
                         latency=(0.0, 0.002), seed=7)
        for ch in Channel
    }
    limiters = {}
    if client_rate is not None:
        limiters = {ch: TokenBucket(client_rate, capacity=1) for ch in Channel}
    return Dispatcher(store, providers, limiters, retry_policy=retry, workers=workers)
