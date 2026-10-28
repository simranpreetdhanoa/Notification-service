# Distributed Notification & Alerting Service

Accepts SMS / Email / Push notifications over HTTP, dispatches **critical alerts
before marketing traffic** (even when submitted later), stays **under each
provider's rate limit**, retries transient failures with **exponential
backoff**, and records every state change in **SQLite** so that **no
notification is sent twice**. Pure Python 3.10+ standard library, no dependencies.

## Quick start (VS Code)

1. `File → Open Folder…` → this folder. Install the recommended extensions when prompted.
2. **Run and Debug** panel (Ctrl+Shift+D) → choose a configuration:
   * **Demo: priority routing + retries**: offline demo that prints delivery order
   * **Serve API on :8080**: then open `requests.http` and click *Send Request*
   * **Run all tests**
3. Or use the **Testing** panel (beaker icon); tests are auto-discovered.

From a terminal:

```bash
python -m notifier demo
python -m notifier serve --port 8080
python -m unittest discover -s tests -t . -v
```

## Architecture

```
 POST /v1/notifications ─┐                        ┌─► MockProvider(SMS)   10/s hard limit
 POST /v1/.../batch ─────┼─► Dispatcher.submit    │
                         │     │ INSERT PENDING   ├─► MockProvider(EMAIL) 50/s
                         │     ▼ (idempotency key)│
                         │  SQLite (WAL) ◄──┐     └─► MockProvider(PUSH) 100/s
                         │     │            │ claim / mark_* (atomic CAS)
                         │     ▼            │
                         │  DispatchQueue ──► N worker threads
                         │  heapq per channel + token bucket gate
 GET /v1/notifications/id┘  + delayed heap for retry backoff
```

| Requirement | Where | How |
|---|---|---|
| Priority routing | `notifier/priority_queue.py` | `heapq` keyed by `(priority, seq)`. Level 1 always pops before Level 3; `seq` keeps FIFO order within a level. Retries wait in a separate *delayed* heap, so a backing-off item never blocks ready work. |
| Rate limiting | `rate_limiter.py`, `priority_queue.py` | Token bucket per channel at 80% of the provider's limit. The queue only hands out an item whose channel has a token, so an exhausted SMS budget doesn't stall email, and workers never sleep holding work. |
| Exponential backoff | `retry.py`, `dispatcher.py` | `min(cap, base·2ⁿ)` with **full jitter** to avoid synchronized retry storms; honours the provider's `Retry-After` on HTTP 429. Permanent 4xx-style errors fail immediately. |
| Atomic status log | `store.py` | `BEGIN IMMEDIATE` transactions; every transition writes the `dispatch_log` audit row in the same transaction. |
| No double sends | `store.py: claim()` | `UPDATE … SET status='SENDING' WHERE id=? AND status='PENDING'`, a compare-and-set where exactly one worker sees `rowcount == 1`. Only the claim holder can move the row to a terminal state. |
| Crash safety | `store.recover_expired_leases`, `Dispatcher.start` | Claims carry a lease. On startup, expired `SENDING` rows go back to `PENDING` and all `PENDING` rows are reloaded into the heap. The row id is sent as the provider idempotency token, so a resend after a crash mid-send is dropped by the provider. |
| Client retries | `store.insert` | `Idempotency-Key` header → `UNIQUE` column → `ON CONFLICT DO NOTHING`; repeat requests return the original id. |

### States

```
PENDING ──claim──► SENDING ──ok──────────► DISPATCHED
   ▲                  │ ──4xx / retries out──► FAILED
   └──transient (backoff)─┘
   └──lease expired (crash recovery)
```

`SENDING` is a fourth, internal state beyond the PENDING/DISPATCHED/FAILED
asked for. It is the lock that makes "send at most once" enforceable across threads.

## API

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/notifications` | `{channel, recipient, body, subject?, priority?}` → 202. `priority` is `1`–`3` or `critical` / `transactional` / `marketing`. Optional `Idempotency-Key` header. |
| POST | `/v1/notifications/batch` | `{"notifications": [...]}`, up to 500, validated all-or-nothing |
| GET | `/v1/notifications/{id}` | current state + full audit history |
| GET | `/v1/stats` | counts by status/priority, queue depth, dispatcher metrics |
| GET | `/healthz` | liveness |

## Tests (31)

* **Queue**: critical-submitted-later pops first, FIFO per level, delayed items don't block, exhausted channel doesn't block others.
* **Store**: 16 threads race to claim one row → exactly one winner; terminal states are final; audit trail order; lease recovery.
* **Dispatcher**: retry→success, permanent failure not retried, give up after max attempts, **200 notifications × duplicate queue entries × 8 workers × 20% failure rate → zero double sends**, **zero provider 429s** under load, pending work survives a restart.
* **API**: submit/track, idempotency header, batch, validation.

## Scaling beyond one box (interview talking points)

* Swap the in-process heap for **SQS** (one queue per priority, or a FIFO queue with message groups) and SQLite for **DynamoDB**, using a conditional write (`ConditionExpression: status = PENDING`) as the same compare-and-set claim.
* Fan out from **SNS** topics by channel; use **DLQs** (dead-letter queues) for the FAILED path.
* Share rate limits across hosts with a central bucket (Redis / ElastiCache with a Lua script) or by giving each host a fixed share of the budget.
* Reserve part of each provider's budget for Level 1, so a marketing blast can never use up the tokens a password reset needs.
* Delivery is "exactly once" only when the provider honours an idempotency token. Otherwise the realistic guarantee is at-most-once (fail, don't requeue, on lease expiry) or at-least-once. Choose per channel.
