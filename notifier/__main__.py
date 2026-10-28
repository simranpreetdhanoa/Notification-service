"""CLI:  python -m notifier serve | demo"""
from __future__ import annotations

import argparse
import logging
import os
import tempfile
import time

from .app import build_dispatcher
from .models import Channel, Notification, Priority
from .api import make_server
from .retry import RetryPolicy


def serve(args) -> None:
    dispatcher = build_dispatcher(args.db, workers=args.workers, failure_rate=args.failure_rate)
    dispatcher.start()
    server = make_server(dispatcher, args.host, args.port)
    print(f"notifier listening on http://{args.host}:{args.port}  (db={args.db})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        dispatcher.stop()


def demo(args) -> None:
    """Queue 30 marketing emails, *then* 3 password resets, and show the resets
    jump the line; with a 5% failure rate you'll also see retries in action."""
    db = os.path.join(tempfile.mkdtemp(), "demo.db")
    dispatcher = build_dispatcher(db, workers=2, failure_rate=0.1,
                                  retry_policy=RetryPolicy(base_delay=0.05, max_delay=1.0),
                                  provider_limits={Channel.SMS: 10, Channel.EMAIL: 10,
                                                   Channel.PUSH: 10})
    for i in range(30):
        dispatcher.submit(Notification(Channel.EMAIL, f"user{i}@example.com", "50% off!",
                                       Priority.MARKETING, subject="Sale"))
    for i in range(3):
        dispatcher.submit(Notification(Channel.EMAIL, f"victim{i}@example.com",
                                       "Your reset link: ...", Priority.CRITICAL,
                                       subject="Password reset"))
    dispatcher.submit(Notification(Channel.SMS, "invalid-number", "2FA code 123456",
                                   Priority.CRITICAL))

    start = time.monotonic()
    dispatcher.start()
    dispatcher.wait_until_idle(timeout=30)
    dispatcher.stop()

    email = dispatcher.providers[Channel.EMAIL]
    print(f"\nDelivery order (email provider, rate limited to 8/s of its 10/s cap):")
    for rank, (t, n) in enumerate(sorted(email.deliveries, key=lambda d: d[0]), 1):
        if rank <= 6 or n.priority is Priority.CRITICAL:
            attempts = dispatcher.store.get(n.id).attempts
            note = f"  (attempt {attempts}: earlier try hit a mock 503)" if attempts > 1 else ""
            print(f"  #{rank:>2}  +{t - start:5.2f}s  {n.priority.name:<11} {n.recipient}{note}")
    print("  ...")
    print(f"\nStatus counts: {dispatcher.store.stats()['by_status']}")
    print(f"Dispatcher metrics: {dispatcher.snapshot_metrics()}")
    print(f"Provider 429s: {email.throttled}   duplicate sends suppressed: "
          f"{email.duplicates_suppressed}")
    print(f"SQLite file: {db}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="notifier")
    sub = parser.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve", help="run the HTTP API + dispatcher")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080)
    s.add_argument("--db", default="notifications.db")
    s.add_argument("--workers", type=int, default=8)
    s.add_argument("--failure-rate", type=float, default=0.05,
                   help="probability a mock provider returns a transient 503")
    sub.add_parser("demo", help="offline demo of priority routing + retries")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(threadName)s %(levelname)s %(message)s")
    {"serve": serve, "demo": demo}[args.cmd](args)


if __name__ == "__main__":
    main()
