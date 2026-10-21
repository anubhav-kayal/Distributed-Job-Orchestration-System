import asyncio
import time
import uuid

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from djos.coordination import Leases, lease_key
from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.models import Job, JobStatus
from djos.worker import Worker
from djos.worker.worker import LeaseLost, Outcome


async def submit(sessionmaker, queue, seconds: float) -> uuid.UUID:
    async with sessionmaker() as session:
        job, _ = await service.submit_job(
            session, queue, JobCreate(type="sleep", payload={"seconds": seconds})
        )
        return job.id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


def test_heartbeat_must_be_at_most_half_the_ttl():
    class FakeLeases:
        ttl_ms = 1000

    with pytest.raises(ValueError, match="half the lease TTL"):
        Worker(None, None, FakeLeases(), heartbeat_seconds=0.6)


@pytest.mark.integration
async def test_heartbeat_keeps_a_long_job_alive_past_the_ttl(sessionmaker, queue, redis):
    leases = Leases(redis, ttl_seconds=0.4)
    worker = Worker(sessionmaker, queue, leases, block_seconds=0.1, heartbeat_seconds=0.1)
    job_id = await submit(sessionmaker, queue, seconds=1.0)  # 2.5x the TTL

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.attempts == 1


@pytest.mark.integration
async def test_worker_abandons_job_when_its_lease_is_lost(sessionmaker, queue, redis):
    leases = Leases(redis, ttl_seconds=2)
    worker = Worker(sessionmaker, queue, leases, block_seconds=0.1, heartbeat_seconds=0.1)
    job_id = await submit(sessionmaker, queue, seconds=30)

    task = asyncio.create_task(worker.process_next())
    await asyncio.sleep(0.3)
    await redis.delete(lease_key(job_id))  # e.g. it expired while we were stalled

    start = time.monotonic()
    await asyncio.wait_for(task, timeout=2)
    assert time.monotonic() - start < 1  # stopped promptly, didn't sleep 30s

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.RUNNING  # left for the monitor to reclaim
    assert job.result is None


async def test_heartbeat_gives_up_after_redis_is_unreachable_for_a_ttl():
    class UnreachableLeases:
        ttl_ms = 300

        async def renew(self, job_id, owner):
            raise RedisConnectionError("connection refused")

    worker = Worker(None, None, UnreachableLeases(), heartbeat_seconds=0.05)
    start = time.monotonic()
    with pytest.raises(LeaseLost, match="could not renew"):
        await worker._heartbeat(uuid.uuid4(), "me")
    assert 0.3 <= time.monotonic() - start < 0.6


@pytest.mark.integration
async def test_result_is_recorded_when_lease_loss_and_completion_coincide(
    sessionmaker, queue, redis, monkeypatch
):
    # Handler and heartbeat both finish in the same event-loop tick, the heartbeat by
    # raising LeaseLost. The finished result must still go through the fenced write
    # (which accepts it here: nobody else claimed the job) instead of being dropped.
    leases = Leases(redis, ttl_seconds=2)
    worker = Worker(sessionmaker, queue, leases, block_seconds=0.1, heartbeat_seconds=0.1)
    job_id = await submit(sessionmaker, queue, seconds=0)

    async def instant_outcome(job):
        return Outcome(JobStatus.SUCCEEDED, result={"done": True})

    async def lease_lost_now(job_id, owner):
        raise LeaseLost("lease expired or taken over")

    monkeypatch.setattr(worker, "_invoke", instant_outcome)
    monkeypatch.setattr(worker, "_heartbeat", lease_lost_now)

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.result == {"done": True}
