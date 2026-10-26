"""Fencing tokens: a worker that stalls past its lease must not be able to record a result
once its job has been reclaimed."""

import uuid

import pytest
from sqlalchemy import func, select

from djos.coordination import Leases, lease_key
from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.models import Effect, Job, JobStatus
from djos.monitor import Monitor
from djos.worker import Worker
from djos.worker.worker import Outcome

pytestmark = pytest.mark.integration


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


def make_worker(sessionmaker, queue, leases, name: str) -> Worker:
    return Worker(
        sessionmaker, queue, leases, worker_id=name, block_seconds=0.1, heartbeat_seconds=1
    )


async def submit(sessionmaker, queue) -> uuid.UUID:
    async with sessionmaker() as session:
        job, _ = await service.submit_job(
            session, queue, JobCreate(type="sleep", payload={"seconds": 0})
        )
        return job.id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


async def effects(sessionmaker, job_id) -> list[Effect]:
    async with sessionmaker() as session:
        return list(await session.scalars(select(Effect).where(Effect.job_id == job_id)))


async def test_success_records_one_effect_with_its_token(sessionmaker, queue, leases):
    job_id = await submit(sessionmaker, queue)
    await make_worker(sessionmaker, queue, leases, "a").process_next()

    [effect] = await effects(sessionmaker, job_id)
    assert effect.lease_token == 1
    assert effect.worker_id == "a"
    assert effect.data == {"slept": 0.0}


async def test_failure_records_no_effect(sessionmaker, queue, leases):
    async with sessionmaker() as session:
        job, _ = await service.submit_job(
            session, queue, JobCreate(type="flaky", payload={"fail_times": 1})
        )
    await make_worker(sessionmaker, queue, leases, "a").process_next()

    assert await effects(sessionmaker, job.id) == []


async def test_stale_worker_cannot_record_a_result_after_reclaim(
    sessionmaker, queue, leases, redis
):
    job_id = await submit(sessionmaker, queue)
    stale = make_worker(sessionmaker, queue, leases, "stale")
    fresh = make_worker(sessionmaker, queue, leases, "fresh")

    # Worker A pops and claims the job (token 1)...
    await queue.dequeue(block_seconds=0.1)
    claimed = await stale._claim(job_id)
    assert claimed.lease_token == 1

    # ...then freezes (GC pause, frozen VM). It can't heartbeat, so its lease expires,
    # the monitor reclaims the job, and worker B runs it to completion with token 2.
    await redis.delete(lease_key(job_id))
    assert (await Monitor(sessionmaker, queue, leases).sweep()).requeued == 1
    assert await fresh.process_next() is True

    job = await load(sessionmaker, job_id)
    assert (job.status, job.lease_token, job.locked_by) == (JobStatus.SUCCEEDED, 2, "fresh")

    # A wakes up, unaware of any of this, and tries to record its own result.
    accepted = await stale._finish(
        job_id, claimed.lease_token, Outcome(True, result={"from": "stale"})
    )

    assert accepted is False
    job = await load(sessionmaker, job_id)
    assert job.result == {"slept": 0.0}  # B's result, untouched
    [effect] = await effects(sessionmaker, job_id)  # exactly one effect...
    assert (effect.worker_id, effect.lease_token) == ("fresh", 2)  # ...and it's B's


async def test_stale_worker_is_fenced_even_while_the_new_run_is_in_progress(
    sessionmaker, queue, leases, redis
):
    # The dangerous case: the job is `running` again (B has it), so a status check alone
    # would let A's write through. Only the token tells A and B apart.
    job_id = await submit(sessionmaker, queue)
    stale = make_worker(sessionmaker, queue, leases, "stale")
    await queue.dequeue(block_seconds=0.1)
    claimed = await stale._claim(job_id)

    await redis.delete(lease_key(job_id))
    await Monitor(sessionmaker, queue, leases).sweep()
    await queue.dequeue(block_seconds=0.1)
    await make_worker(sessionmaker, queue, leases, "fresh")._claim(job_id)  # B: token 2

    accepted = await stale._finish(
        job_id, claimed.lease_token, Outcome(False, error="stale failure")
    )

    assert accepted is False
    job = await load(sessionmaker, job_id)
    assert (job.status, job.lease_token, job.last_error) == (
        JobStatus.RUNNING,
        2,
        "lease expired on attempt 1 (worker stale presumed dead); requeued",
    )


async def test_tokens_grow_with_every_claim(sessionmaker, queue, leases, redis):
    job_id = await submit(sessionmaker, queue)
    monitor = Monitor(sessionmaker, queue, leases)
    for expected in (1, 2, 3):
        await queue.dequeue(block_seconds=0.1)
        claimed = await make_worker(sessionmaker, queue, leases, f"w{expected}")._claim(job_id)
        assert claimed.lease_token == expected
        await redis.delete(lease_key(job_id))
        await monitor.sweep()

    async with sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(Effect)) == 0
