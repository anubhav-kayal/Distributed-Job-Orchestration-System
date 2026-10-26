import uuid
from collections import Counter

import pytest

from djos.models import Priority
from djos.queue import WeightedRotation

H, N, L = Priority.HIGH, Priority.NORMAL, Priority.LOW


def test_each_cycle_gives_every_tier_exactly_its_weight():
    rotation = WeightedRotation({H: 6, N: 3, L: 1})
    picks = [rotation.next() for _ in range(100)]
    assert Counter(picks) == {H: 60, N: 30, L: 10}
    for start in range(0, 100, 10):  # and it holds within every 10-pick cycle
        assert Counter(picks[start : start + 10]) == {H: 6, N: 3, L: 1}


def test_turns_are_interleaved_not_bursty():
    rotation = WeightedRotation({H: 6, N: 3, L: 1})
    cycle = [rotation.next() for _ in range(10)]
    assert cycle == [H, N, H, H, N, H, L, H, N, H]  # nginx smooth weighted round-robin
    longest_run = max(len(run) for run in "".join(p.value[0] for p in cycle).split("n"))
    assert longest_run < 6


@pytest.mark.parametrize("weights", [{H: 1, N: 1}, {H: 1, N: 1, L: 0}])
def test_invalid_weights_are_rejected(weights):
    with pytest.raises(ValueError):
        WeightedRotation(weights)


@pytest.mark.integration
async def test_preferred_tier_is_popped_first_and_falls_back_when_empty(queue):
    high, low = uuid.uuid4(), uuid.uuid4()
    await queue.enqueue(high, H)
    await queue.enqueue(low, L)

    assert await queue.dequeue(block_seconds=0.1, prefer=L) == low
    assert await queue.dequeue(block_seconds=0.1, prefer=L) == high  # low empty: fall back


@pytest.mark.integration
async def test_low_priority_is_not_starved_by_a_high_priority_flood(queue):
    for _ in range(50):
        await queue.enqueue(uuid.uuid4(), H)
    lows = {uuid.uuid4() for _ in range(5)}
    for job_id in lows:
        await queue.enqueue(job_id, L)

    rotation = WeightedRotation()  # 6/3/1; normal is empty, so its turns fall back to high
    popped = [await queue.dequeue(block_seconds=0.1, prefer=rotation.next()) for _ in range(20)]

    # Strict priority would pop 20 highs and no lows; the rotation guarantees 1 in 10.
    assert len(lows & set(popped)) == 2
