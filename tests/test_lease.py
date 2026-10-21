import asyncio
import uuid

import pytest

from djos.coordination import Leases, lease_key

pytestmark = pytest.mark.integration


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


async def test_only_one_owner_can_acquire(leases):
    job_id = uuid.uuid4()
    assert await leases.acquire(job_id, "a") is True
    assert await leases.acquire(job_id, "b") is False
    assert await leases.holder(job_id) == "a"


async def test_concurrent_acquire_has_exactly_one_winner(leases):
    job_id = uuid.uuid4()
    results = await asyncio.gather(*(leases.acquire(job_id, f"w{i}") for i in range(20)))
    assert results.count(True) == 1


async def test_lease_expires_on_its_own(redis):
    leases = Leases(redis, ttl_seconds=0.1)
    job_id = uuid.uuid4()
    await leases.acquire(job_id, "a")
    await asyncio.sleep(0.2)
    assert await leases.holder(job_id) is None
    assert await leases.acquire(job_id, "b") is True


async def test_owner_can_renew(redis, leases):
    job_id = uuid.uuid4()
    await leases.acquire(job_id, "a")
    await redis.pexpire(lease_key(job_id), 100)  # nearly expired
    assert await leases.renew(job_id, "a") is True
    assert await redis.pttl(lease_key(job_id)) > 4000


async def test_non_owner_cannot_renew_or_release(leases):
    job_id = uuid.uuid4()
    await leases.acquire(job_id, "a")
    assert await leases.renew(job_id, "b") is False
    assert await leases.release(job_id, "b") is False
    assert await leases.holder(job_id) == "a"


async def test_renew_fails_after_lease_was_lost(leases):
    job_id = uuid.uuid4()
    assert await leases.renew(job_id, "a") is False


async def test_release_does_not_delete_someone_elses_lease(redis):
    # The race a compare-and-delete prevents: A's lease expires, B acquires, then A
    # releases. A plain DEL would remove B's lease.
    leases = Leases(redis, ttl_seconds=0.1)
    job_id = uuid.uuid4()
    await leases.acquire(job_id, "a")
    await asyncio.sleep(0.2)
    await leases.acquire(job_id, "b")
    assert await leases.release(job_id, "a") is False
    assert await leases.holder(job_id) == "b"


async def test_alive(leases):
    held, free = uuid.uuid4(), uuid.uuid4()
    await leases.acquire(held, "a")
    assert await leases.alive([held, free]) == [True, False]
    assert await leases.alive([]) == []
