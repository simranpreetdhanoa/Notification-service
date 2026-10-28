import os
import tempfile
import unittest

from notifier import Channel, Notification, Priority, Status
from notifier.providers import MockProvider, PermanentProviderError, ThrottledError, TransientProviderError

from tests.helpers import FAST_RETRY, make_dispatcher


def email(recipient, priority=Priority.MARKETING):
    return Notification(Channel.EMAIL, recipient, "body", priority, subject="s")


class DispatcherTest(unittest.TestCase):
    def tearDown(self):
        self.d.stop()

    def test_priority_order_with_single_worker(self):
        self.d = make_dispatcher(workers=1)
        for i in range(10):
            self.d.submit(email(f"m{i}"))
        self.d.submit(email("reset", Priority.CRITICAL))
        self.d.submit(email("receipt", Priority.TRANSACTIONAL))
        self.d.start()
        self.assertTrue(self.d.wait_until_idle())
        order = [n.recipient for _, n in self.d.providers[Channel.EMAIL].deliveries]
        self.assertEqual(order[:2], ["reset", "receipt"])
        self.assertEqual(order[2:], [f"m{i}" for i in range(10)])

    def test_transient_failures_retry_then_succeed(self):
        self.d = make_dispatcher()
        self.d.providers[Channel.EMAIL].script(
            "flaky", TransientProviderError("503"), ThrottledError("429", 0.01))
        n, _ = self.d.submit(email("flaky"))
        self.d.start()
        self.assertTrue(self.d.wait_until_idle())
        final = self.d.store.get(n.id)
        self.assertEqual(final.status, Status.DISPATCHED)
        self.assertEqual(final.attempts, 3)

    def test_permanent_failure_is_not_retried(self):
        self.d = make_dispatcher()
        n, _ = self.d.submit(email("invalid@nowhere"))
        self.d.start()
        self.assertTrue(self.d.wait_until_idle())
        final = self.d.store.get(n.id)
        self.assertEqual((final.status, final.attempts), (Status.FAILED, 1))

    def test_gives_up_after_max_attempts(self):
        self.d = make_dispatcher()
        self.d.providers[Channel.EMAIL].script(
            "down", *[TransientProviderError("503")] * FAST_RETRY.max_attempts)
        n, _ = self.d.submit(email("down"))
        self.d.start()
        self.assertTrue(self.d.wait_until_idle())
        final = self.d.store.get(n.id)
        self.assertEqual(final.status, Status.FAILED)
        self.assertEqual(final.attempts, FAST_RETRY.max_attempts)
        self.assertIn("gave up", final.last_error)

    def test_no_double_send_under_concurrency(self):
        self.d = make_dispatcher(workers=8, failure_rate=0.2)
        ids = [self.d.submit(email(f"u{i}"))[0].id for i in range(200)]
        # Bypass the queue's own dedupe to force workers to race on the DB claim.
        for nid in ids:
            self.d.queue._members.discard(nid)
            self.d.queue.put(nid, Priority.MARKETING, Channel.EMAIL)
        self.d.start()
        self.assertTrue(self.d.wait_until_idle(timeout=30))
        delivered = [n.id for _, n in self.d.providers[Channel.EMAIL].deliveries]
        self.assertEqual(len(delivered), len(set(delivered)))  # nothing sent twice
        rows = [self.d.store.get(nid) for nid in ids]
        dispatched = {n.id for n in rows if n.status is Status.DISPATCHED}
        self.assertEqual(set(delivered), dispatched)  # DB agrees with the provider
        for n in rows:  # anything not sent exhausted its retries on 503s
            if n.status is Status.FAILED:
                self.assertEqual(n.attempts, FAST_RETRY.max_attempts)
        self.assertGreater(self.d.snapshot_metrics().get("claim_lost", 0), 0)

    def test_client_limiter_keeps_us_under_provider_limit(self):
        providers = {ch: MockProvider(ch, max_per_second=50, burst=1, latency=(0, 0))
                     for ch in Channel}
        self.d = make_dispatcher(workers=8, client_rate=40, providers=providers)
        for i in range(40):
            self.d.submit(Notification(Channel.SMS, f"+1555{i:04}", "code", Priority.CRITICAL))
        self.d.start()
        self.assertTrue(self.d.wait_until_idle(timeout=10))
        sms = providers[Channel.SMS]
        self.assertEqual(len(sms.deliveries), 40)
        self.assertEqual(sms.throttled, 0)
        times = [t for t, _ in sms.deliveries]
        self.assertGreaterEqual(times[-1] - times[0], 39 / 40 * 0.9)

    def test_pending_work_survives_restart(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = os.path.join(tmp.name, "n.db")
        first = make_dispatcher(db=db)
        ids = [first.submit(email(f"u{i}"))[0].id for i in range(5)]
        crashed = first.store.claim(ids[0], lease_seconds=-1)  # died mid-send
        self.assertIsNotNone(crashed)
        first.store.close()

        self.d = make_dispatcher(db=db)  # "new process"
        self.d.start()
        self.assertTrue(self.d.wait_until_idle())
        self.assertEqual(self.d.store.stats()["by_status"]["DISPATCHED"], 5)


if __name__ == "__main__":
    unittest.main()
