import threading
import time
import unittest

from notifier import Channel, DispatchQueue, Priority, TokenBucket


class PriorityQueueTest(unittest.TestCase):
    def test_critical_submitted_later_pops_first(self):
        q = DispatchQueue()
        for i in range(5):
            q.put(f"mkt{i}", Priority.MARKETING, Channel.EMAIL)
        q.put("reset", Priority.CRITICAL, Channel.EMAIL)
        q.put("order", Priority.TRANSACTIONAL, Channel.SMS)
        got = [q.get(timeout=0).notification_id for _ in range(7)]
        self.assertEqual(got[:2], ["reset", "order"])
        self.assertEqual(got[2:], [f"mkt{i}" for i in range(5)])  # FIFO within a level

    def test_delayed_item_does_not_block_ready_ones(self):
        q = DispatchQueue()
        q.put("retrying-critical", Priority.CRITICAL, Channel.SMS, delay=0.2)
        q.put("mkt", Priority.MARKETING, Channel.SMS)
        self.assertEqual(q.get(timeout=0).notification_id, "mkt")
        self.assertIsNone(q.get(timeout=0.01))
        self.assertEqual(q.get(timeout=1).notification_id, "retrying-critical")

    def test_requeue_with_original_seq_keeps_place(self):
        q = DispatchQueue()
        first = q.put("a", Priority.MARKETING, Channel.PUSH)
        q.put("b", Priority.MARKETING, Channel.PUSH)
        q.get(timeout=0)
        q.put("a", Priority.MARKETING, Channel.PUSH, seq=first.seq)
        self.assertEqual(q.get(timeout=0).notification_id, "a")

    def test_duplicate_ids_are_ignored(self):
        q = DispatchQueue()
        self.assertIsNotNone(q.put("x", Priority.CRITICAL, Channel.SMS))
        self.assertIsNone(q.put("x", Priority.CRITICAL, Channel.SMS))
        self.assertEqual(len(q), 1)

    def test_exhausted_channel_does_not_block_other_channels(self):
        sms = TokenBucket(rate=1, capacity=1)
        sms.try_acquire()  # SMS budget now empty for ~1s
        q = DispatchQueue({Channel.SMS: sms})
        q.put("sms-critical", Priority.CRITICAL, Channel.SMS)
        q.put("push-marketing", Priority.MARKETING, Channel.PUSH)
        self.assertEqual(q.get(timeout=0).notification_id, "push-marketing")
        self.assertIsNone(q.get(timeout=0.05))

    def test_close_wakes_blocked_getters(self):
        q = DispatchQueue()
        results = []
        t = threading.Thread(target=lambda: results.append(q.get()))
        t.start()
        time.sleep(0.05)
        q.close()
        t.join(1)
        self.assertEqual(results, [None])


if __name__ == "__main__":
    unittest.main()
