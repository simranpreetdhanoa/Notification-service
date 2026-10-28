import unittest

from notifier import RetryPolicy, TokenBucket


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class TokenBucketTest(unittest.TestCase):
    def test_burst_then_refill(self):
        clock = FakeClock()
        b = TokenBucket(rate=10, capacity=3, clock=clock)
        self.assertEqual([b.try_acquire() for _ in range(3)], [0.0, 0.0, 0.0])
        self.assertAlmostEqual(b.try_acquire(), 0.1)  # need 1 token at 10/s
        clock.t = 0.1
        self.assertEqual(b.try_acquire(), 0.0)

    def test_never_exceeds_capacity_after_idle(self):
        clock = FakeClock()
        b = TokenBucket(rate=10, capacity=2, clock=clock)
        clock.t = 100
        granted = sum(b.try_acquire() == 0.0 for _ in range(10))
        self.assertEqual(granted, 2)

    def test_rejects_bad_config(self):
        with self.assertRaises(ValueError):
            TokenBucket(rate=0)


class RetryPolicyTest(unittest.TestCase):
    def test_exponential_growth_is_capped(self):
        p = RetryPolicy(base_delay=0.5, multiplier=2, max_delay=3, jitter=False)
        self.assertEqual([p.backoff(n) for n in range(1, 6)], [0.5, 1.0, 2.0, 3, 3])

    def test_full_jitter_stays_within_ceiling(self):
        p = RetryPolicy(base_delay=1, max_delay=8)
        for attempt in range(1, 8):
            ceiling = min(8, 2 ** (attempt - 1))
            for _ in range(50):
                self.assertTrue(0 <= p.backoff(attempt) <= ceiling)

    def test_exhausted(self):
        p = RetryPolicy(max_attempts=3)
        self.assertFalse(p.exhausted(2))
        self.assertTrue(p.exhausted(3))


if __name__ == "__main__":
    unittest.main()
