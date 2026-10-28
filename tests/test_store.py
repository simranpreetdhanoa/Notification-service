import os
import tempfile
import threading
import unittest

from notifier import Channel, Notification, NotificationStore, Priority, Status


def sms(**kw):
    return Notification(Channel.SMS, kw.pop("recipient", "+15550100"), "hi", **kw)


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.store = NotificationStore(os.path.join(self.dir.name, "t.db"))

    def tearDown(self):
        self.store.close()
        self.dir.cleanup()

    def test_idempotency_key_dedupes(self):
        a, created_a = self.store.insert(sms(idempotency_key="k1"))
        b, created_b = self.store.insert(sms(idempotency_key="k1"))
        self.assertTrue(created_a)
        self.assertFalse(created_b)
        self.assertEqual(a.id, b.id)
        self.assertEqual(self.store.stats()["by_status"]["PENDING"], 1)

    def test_concurrent_claims_have_exactly_one_winner(self):
        n, _ = self.store.insert(sms())
        barrier = threading.Barrier(16)
        winners = []

        def race():
            barrier.wait()
            if self.store.claim(n.id, lease_seconds=30):
                winners.append(threading.get_ident())

        threads = [threading.Thread(target=race) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.store.get(n.id).attempts, 1)

    def test_terminal_states_are_final(self):
        n, _ = self.store.insert(sms())
        self.store.claim(n.id, 30)
        self.assertTrue(self.store.mark_dispatched(n.id, "msg-1"))
        self.assertFalse(self.store.mark_failed(n.id, "late error"))
        self.assertIsNone(self.store.claim(n.id, 30))
        self.assertEqual(self.store.get(n.id).status, Status.DISPATCHED)

    def test_transitions_are_audited(self):
        n, _ = self.store.insert(sms())
        self.store.claim(n.id, 30)
        self.store.mark_retry(n.id, 0, "503")
        self.store.claim(n.id, 30)
        self.store.mark_dispatched(n.id, "msg-1")
        steps = [(h["from_status"], h["to_status"]) for h in self.store.history(n.id)]
        self.assertEqual(steps, [
            (None, "PENDING"), ("PENDING", "SENDING"), ("SENDING", "PENDING"),
            ("PENDING", "SENDING"), ("SENDING", "DISPATCHED"),
        ])

    def test_expired_leases_are_recovered(self):
        n, _ = self.store.insert(sms())
        self.store.claim(n.id, lease_seconds=-1)  # simulate a worker that died
        recovered = self.store.recover_expired_leases()
        self.assertEqual([r.id for r in recovered], [n.id])
        self.assertEqual(self.store.get(n.id).status, Status.PENDING)

    def test_schema_rejects_bad_priority(self):
        with self.assertRaises(Exception):
            self.store.insert(sms(priority=7))


if __name__ == "__main__":
    unittest.main()
