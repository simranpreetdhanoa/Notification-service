"""Worker pool that drains the priority queue into the providers.

The queue hands a worker the highest-priority item whose channel has a
rate-limit token (see priority_queue). The worker then:
  1. claims the row in SQLite (PENDING -> SENDING); losing the race means
     someone else owns it, so it drops the item;
  2. calls the provider, then records DISPATCHED, schedules a retry with
     exponential backoff (back to PENDING), or records FAILED.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from collections import Counter
from typing import Mapping, Protocol

from .models import Channel, Notification
from .priority_queue import DispatchQueue, QueueItem
from .providers import PermanentProviderError, ThrottledError
from .rate_limiter import TokenBucket
from .retry import RetryPolicy
from .store import NotificationStore

log = logging.getLogger(__name__)


class Provider(Protocol):
    def send(self, n: Notification, idempotency_token: str) -> str: ...


class Dispatcher:
    def __init__(
        self,
        store: NotificationStore,
        providers: Mapping[Channel, Provider],
        rate_limiters: Mapping[Channel, TokenBucket],
        retry_policy: RetryPolicy = RetryPolicy(),
        workers: int = 4,
        lease_seconds: float = 30.0,
        rng: random.Random | None = None,
    ) -> None:
        missing = set(Channel) - set(providers)
        if missing:
            raise ValueError(f"no provider for {sorted(c.value for c in missing)}")
        self.store = store
        self.providers = dict(providers)
        self.rate_limiters = dict(rate_limiters)
        self.retry_policy = retry_policy
        self.lease_seconds = lease_seconds
        self.queue = DispatchQueue(self.rate_limiters)
        self.metrics: Counter[str] = Counter()
        self._metrics_lock = threading.Lock()
        self._rng = rng or random.Random()
        self._n_workers = workers
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()

    # -- lifecycle ------------------------------------------------------------

    def start(self) -> None:
        """Recover crashed claims, reload PENDING rows from disk, start workers."""
        recovered = self.store.recover_expired_leases()
        if recovered:
            log.warning("requeued %d notifications with expired leases", len(recovered))
        now = time.time()
        for n in self.store.pending():
            self.queue.put(n.id, n.priority, n.channel, delay=max(0.0, n.next_attempt_at - now))
        for i in range(self._n_workers):
            t = threading.Thread(target=self._run, name=f"dispatch-worker-{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self.queue.close()
        for t in self._threads:
            t.join(timeout)
        self._threads.clear()

    def wait_until_idle(self, timeout: float = 10.0, poll: float = 0.02) -> bool:
        """Block until nothing is PENDING or SENDING (for tests and demos)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.store.open_count() == 0:
                return True
            time.sleep(poll)
        return False

    # -- intake ---------------------------------------------------------------

    def submit(self, n: Notification) -> tuple[Notification, bool]:
        stored, created = self.store.insert(n)
        if created:
            self.queue.put(stored.id, stored.priority, stored.channel)
            self._count("accepted")
        else:
            self._count("deduplicated")
        return stored, created

    # -- workers --------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop.is_set():
            item = self.queue.get(timeout=0.25)
            if item is None:
                continue
            try:
                self._process(item)
            except Exception:  # never let one bad item kill a worker
                log.exception("unexpected error processing %s", item.notification_id)
                self._count("worker_errors")

    def _process(self, item: QueueItem) -> None:
        n = self.store.claim(item.notification_id, self.lease_seconds)
        if n is None:
            self._count("claim_lost")  # duplicate queue entry or already terminal
            return

        try:
            # The row id doubles as the provider idempotency token, so a resend
            # after a crash between "provider accepted" and "DB updated" is a no-op.
            message_id = self.providers[n.channel].send(n, idempotency_token=n.id)
        except PermanentProviderError as e:
            self.store.mark_failed(n.id, f"permanent: {e}")
            self._count("failed")
        except Exception as e:  # transient, throttled, or unknown -> retry
            self._handle_retryable(n, item, e)
        else:
            self.store.mark_dispatched(n.id, message_id)
            self._count("dispatched")

    def _handle_retryable(self, n: Notification, item: QueueItem, e: Exception) -> None:
        if isinstance(e, ThrottledError):
            self._count("provider_throttled")
        if self.retry_policy.exhausted(n.attempts):
            self.store.mark_failed(n.id, f"gave up after {n.attempts} attempts: {e}")
            self._count("failed")
            return
        delay = self.retry_policy.backoff(n.attempts, self._rng)
        if isinstance(e, ThrottledError):
            delay = max(delay, e.retry_after)
        self.store.mark_retry(n.id, time.time() + delay, str(e))
        self.queue.put(n.id, n.priority, n.channel, delay=delay, seq=item.seq)
        self._count("retries")

    def _count(self, key: str) -> None:
        with self._metrics_lock:
            self.metrics[key] += 1

    def snapshot_metrics(self) -> dict[str, int]:
        with self._metrics_lock:
            return dict(self.metrics)
