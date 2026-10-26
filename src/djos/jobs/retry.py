"""When to retry a failed job: exponential backoff with full jitter.

Exponential: each failure doubles the ceiling (base, 2·base, 4·base, … up to `cap`), so a
struggling dependency gets more and more room to recover.

Jitter: the actual delay is random in [0, ceiling]. Without it, every job that failed
because of the same outage (say 500 jobs when a database blipped) would retry at exactly
the same instants and hit the recovering dependency as one synchronized wave, again and
again. "Full jitter" spreads the retries uniformly across the window, which AWS found
gives the least total work and the fastest overall completion under contention
(AWS Architecture Blog, "Exponential Backoff and Jitter").
"""

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    base_seconds: float = 1.0
    cap_seconds: float = 300.0

    def ceiling(self, attempt: int) -> float:
        """Upper bound of the delay after the `attempt`-th failure (1-based)."""
        # Cap the exponent so a huge attempt count can't overflow the float.
        return min(self.cap_seconds, self.base_seconds * 2 ** min(attempt - 1, 62))

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        """Seconds to wait before retrying after the `attempt`-th failure."""
        return (rng or random).uniform(0, self.ceiling(attempt))
