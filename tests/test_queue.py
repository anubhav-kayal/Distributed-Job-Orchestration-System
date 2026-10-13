import asyncio
import time
import uuid

import pytest

from djos.models import Priority

pytestmark = pytest.mark.integration


async def test_dequeue_times_out_when_empty(queue):
    start = time.monotonic()
    assert await queue.dequeue(block_seconds=0.2) is None
    assert time.monotonic() - start >= 0.15


async def test_fifo_within_a_priority(queue):
    ids = [uuid.uuid4() for _ in range(3)]
    for job_id in ids:
        await queue.enqueue(job_id, Priority.NORMAL)
    assert [await queue.dequeue(block_seconds=0.1) for _ in ids] == ids


async def test_higher_priority_is_dequeued_first(queue):
    low, normal, high = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await queue.enqueue(low, Priority.LOW)
    await queue.enqueue(normal, Priority.NORMAL)
    await queue.enqueue(high, Priority.HIGH)
    assert [await queue.dequeue(block_seconds=0.1) for _ in range(3)] == [high, normal, low]


async def test_depth_per_priority(queue):
    await queue.enqueue(uuid.uuid4(), Priority.HIGH)
    await queue.enqueue(uuid.uuid4(), Priority.LOW)
    await queue.enqueue(uuid.uuid4(), Priority.LOW)
    assert await queue.depth() == {Priority.HIGH: 1, Priority.NORMAL: 0, Priority.LOW: 2}


async def test_each_id_is_delivered_to_exactly_one_consumer(queue):
    # BLMPOP is atomic: concurrent consumers never receive the same element.
    ids = {uuid.uuid4() for _ in range(50)}
    for job_id in ids:
        await queue.enqueue(job_id, Priority.NORMAL)

    async def consume() -> list[uuid.UUID]:
        got = []
        while (job_id := await queue.dequeue(block_seconds=0.1)) is not None:
            got.append(job_id)
        return got

    results = await asyncio.gather(*(consume() for _ in range(5)))
    delivered = [job_id for batch in results for job_id in batch]
    assert len(delivered) == len(ids)
    assert set(delivered) == ids
