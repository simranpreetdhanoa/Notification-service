"""Exponential backoff policy (capped, with "full jitter").

Full jitter -- sleep = uniform(0, min(cap, base * multiplier**n)) -- spreads
retries out so a provider outage doesn't produce synchronized retry storms.
"""
from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: bool = True

    def backoff(self, attempt: int, rng: random.Random | None = None) -> float:
        """Delay before the next try, given ``attempt`` tries already made (1-based)."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        ceiling = min(self.max_delay, self.base_delay * self.multiplier ** (attempt - 1))
        if not self.jitter:
            return ceiling
        return (rng or random).uniform(0, ceiling)

    def exhausted(self, attempt: int) -> bool:
        return attempt >= self.max_attempts
