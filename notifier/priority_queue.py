"""Thread-safe, rate-aware priority queue built on heapq.

Structure:
  * one ready heap per channel, ordered by (priority, seq)
  * one delayed heap, ordered by (ready_at, priority, seq) -- retry backoffs

``get`` promotes due delayed items, then considers each channel's best item in
priority order and returns the first whose channel can take a rate-limit token.
So:
  * a Level-1 item submitted *after* a pile of Level-3 items still pops first;
  * an exhausted SMS budget never blocks email or push work behind it;
  * a worker never pops an item it can't send yet, so there is no busy
    re-queue loop while a bucket refills.

``seq`` is a monotonically increasing tie-breaker giving FIFO order within a
priority level; re-queuing an item with its original seq keeps its place.
The queue also refuses to hold the same notification id twice.
"""
from __future__ import annotations

import heapq
import itertools
import threading
import time
from typing import Mapping, NamedTuple

from .models import Channel, Priority
from .rate_limiter import TokenBucket


class QueueItem(NamedTuple):
    priority: int
    seq: int
    notification_id: str
    channel: Channel


class DispatchQueue:
    def __init__(self, rate_limiters: Mapping[Channel, TokenBucket] | None = None,
                 clock=time.monotonic) -> None:
        self._limiters = dict(rate_limiters or {})
        self._ready: dict[Channel, list[QueueItem]] = {}
        self._delayed: list[tuple[float, QueueItem]] = []
        self._members: set[str] = set()
        self._cv = threading.Condition()
        self._seq = itertools.count()
        self._closed = False
        self._clock = clock

    def put(
        self,
        notification_id: str,
        priority: Priority | int,
        channel: Channel,
        delay: float = 0.0,
        seq: int | None = None,
    ) -> QueueItem | None:
        """Enqueue; returns None (no-op) if this id is already queued."""
        with self._cv:
            if notification_id in self._members:
                return None
            if seq is None:
                seq = next(self._seq)
            item = QueueItem(int(priority), seq, notification_id, channel)
            self._members.add(notification_id)
            if delay > 0:
                heapq.heappush(self._delayed, (self._clock() + delay, item))
            else:
                heapq.heappush(self._ready.setdefault(channel, []), item)
            self._cv.notify()
            return item

    def get(self, timeout: float | None = None) -> QueueItem | None:
        """Pop the highest-priority sendable item (its rate token is already
        taken); None on timeout or close."""
        deadline = None if timeout is None else self._clock() + timeout
        with self._cv:
            while True:
                now = self._clock()
                self._promote_due(now)
                item, token_wait = self._pop_sendable()
                if item is not None:
                    self._members.discard(item.notification_id)
                    return item
                if self._closed:
                    return None
                waits = [] if token_wait is None else [token_wait]
                if self._delayed:
                    waits.append(self._delayed[0][0] - now)
                if deadline is not None:
                    remaining = deadline - now
                    if remaining <= 0:
                        return None
                    waits.append(remaining)
                self._cv.wait(max(0.0, min(waits)) if waits else None)

    def close(self) -> None:
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    def __len__(self) -> int:
        with self._cv:
            return len(self._members)

    def __contains__(self, notification_id: str) -> bool:
        with self._cv:
            return notification_id in self._members

    def _pop_sendable(self) -> tuple[QueueItem | None, float | None]:
        """Returns (item, None) or (None, seconds until some channel has a token)."""
        heads = sorted(heap[0] for heap in self._ready.values() if heap)
        soonest: float | None = None
        for head in heads:
            limiter = self._limiters.get(head.channel)
            wait = limiter.try_acquire() if limiter else 0.0
            if wait == 0.0:
                return heapq.heappop(self._ready[head.channel]), None
            soonest = wait if soonest is None else min(soonest, wait)
        return None, soonest

    def _promote_due(self, now: float) -> None:
        while self._delayed and self._delayed[0][0] <= now:
            _, item = heapq.heappop(self._delayed)
            heapq.heappush(self._ready.setdefault(item.channel, []), item)
