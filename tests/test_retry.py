import random

import pytest

from djos.jobs.retry import RetryPolicy


def test_ceiling_doubles_per_attempt_until_the_cap():
    policy = RetryPolicy(base_seconds=1, cap_seconds=10)
    assert [policy.ceiling(a) for a in range(1, 7)] == [1, 2, 4, 8, 10, 10]


def test_huge_attempt_numbers_stay_at_the_cap():
    assert RetryPolicy(base_seconds=1, cap_seconds=60).ceiling(10_000) == 60


@pytest.mark.parametrize("attempt", [1, 3, 8])
def test_delay_is_within_zero_and_the_ceiling(attempt):
    policy = RetryPolicy(base_seconds=2, cap_seconds=100)
    rng = random.Random(42)
    delays = [policy.delay(attempt, rng) for _ in range(1000)]
    assert all(0 <= d <= policy.ceiling(attempt) for d in delays)


def test_full_jitter_spreads_retries_across_the_window():
    # The point of jitter: jobs that failed together don't retry together.
    policy = RetryPolicy(base_seconds=1, cap_seconds=300)
    rng = random.Random(7)
    delays = [policy.delay(5, rng) for _ in range(1000)]  # window is [0, 16]
    buckets = {int(d // 4) for d in delays}  # four 4-second buckets
    assert buckets == {0, 1, 2, 3}
    assert 6 < sum(delays) / len(delays) < 10  # mean ≈ 8, half the window
