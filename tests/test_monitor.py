import uuid

import pytest
from sqlalchemy import update

from djos.coordination import Leases
from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.models import Job, JobStatus, Priority
from djos.monitor import Monitor
from djos.worker import Worker

pytestmark = pytest.mark.integration


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


@pytest.fixture
def monitor(sessionmaker, queue, leases) -> Monitor:
    return Monitor(sessionmaker, queue, leases, stranded_after_seconds=0)


def make_worker(sessionmaker, queue, leases, name: str) -> Worker:
    return Worker(
        sessionmaker, queue, leases, worker_id=name, block_seconds=0.1, heartbeat_seconds=1
    )


async def submit(sessionmaker, queue, **body) -> uuid.UUID:
    body.setdefault("type", "sleep")
    body.setdefault("payload", {"seconds": 0})
    async with sessionmaker() as session:
        job, _ = await service.submit_job(session, queue, JobCreate(**body))
        return job.id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


async def start_and_die(sessionmaker, queue, leases, name="dead-worker") -> uuid.UUID:
    """A worker pops a job and claims it, then dies without a trace: no lease survives."""
    job_id = await submit(sessionmaker, queue)
    worker = make_worker(sessionmaker, queue, leases, name)
    assert await queue.dequeue(block_seconds=0.1) == job_id
    assert await worker._claim(job_id) is not None
    return job_id


async def test_orphaned_running_job_is_requeued(monitor, sessionmaker, queue, leases):
    job_id = await start_and_die(sessionmaker, queue, leases)

    result = await monitor.sweep()

    assert result.requeued == 1
    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.QUEUED
    assert job.attempts == 1
    assert "dead-worker presumed dead" in job.last_error
    assert await queue.contains(job_id, Priority.NORMAL)


async def test_running_job_with_a_live_lease_is_left_alone(monitor, sessionmaker, queue, leases):
    job_id = await start_and_die(sessionmaker, queue, leases)
    await leases.acquire(job_id, "alive-worker")

    result = await monitor.sweep()

    assert result.requeued == 0
    assert (await load(sessionmaker, job_id)).status is JobStatus.RUNNING


async def test_orphan_with_no_attempts_left_fails(monitor, sessionmaker, queue, leases):
    job_id = await submit(sessionmaker, queue, max_attempts=1)
    await queue.dequeue(block_seconds=0.1)
    await make_worker(sessionmaker, queue, leases, "w")._claim(job_id)

    result = await monitor.sweep()

    assert result.failed == 1
    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.FAILED
    assert job.last_error.endswith("no attempts left")
    assert not await queue.contains(job_id, Priority.NORMAL)


async def test_crashed_job_is_finished_by_another_worker(monitor, sessionmaker, queue, leases):
    job_id = await start_and_die(sessionmaker, queue, leases)

    await monitor.sweep()
    survivor = make_worker(sessionmaker, queue, leases, "survivor")
    assert await survivor.process_next() is True

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.attempts == 2
    assert job.locked_by == "survivor"


async def test_stranded_queued_job_is_reenqueued(monitor, sessionmaker, queue, redis):
    job_id = await submit(sessionmaker, queue)
    await redis.delete("q:normal")  # the push was lost

    result = await monitor.sweep()

    assert result.reenqueued == 1
    assert await queue.contains(job_id, Priority.NORMAL)


async def test_queued_job_already_in_redis_is_not_duplicated(monitor, sessionmaker, queue, redis):
    await submit(sessionmaker, queue)

    result = await monitor.sweep()

    assert result.reenqueued == 0
    assert await redis.llen("q:normal") == 1


async def test_recently_queued_jobs_are_not_checked(sessionmaker, queue, leases, redis):
    monitor = Monitor(sessionmaker, queue, leases, stranded_after_seconds=60)
    await submit(sessionmaker, queue)
    await redis.delete("q:normal")

    assert (await monitor.sweep()).reenqueued == 0


async def test_stranded_check_pages_through_a_backlog_across_sweeps(
    sessionmaker, queue, leases, redis
):
    # Each sweep checks one batch and continues where it left off, so jobs deep in a
    # backlog are reached instead of the same oldest jobs being checked every sweep.
    monitor = Monitor(sessionmaker, queue, leases, stranded_after_seconds=0, batch_size=2)
    for _ in range(5):
        await submit(sessionmaker, queue)
    await redis.delete("q:normal")

    per_sweep = [(await monitor.sweep()).reenqueued for _ in range(4)]

    assert per_sweep == [2, 2, 1, 0]  # 4th sweep wrapped around: all present now
    assert await redis.llen("q:normal") == 5


async def test_reclaim_covers_more_running_jobs_than_one_batch(sessionmaker, queue, leases):
    monitor = Monitor(sessionmaker, queue, leases, stranded_after_seconds=60, batch_size=2)
    for i in range(5):
        await start_and_die(sessionmaker, queue, leases, name=f"dead-{i}")

    assert (await monitor.sweep()).requeued == 5


async def test_one_failing_job_does_not_stop_the_sweep(
    monitor, sessionmaker, queue, redis, monkeypatch
):
    bad, good = await submit(sessionmaker, queue), await submit(sessionmaker, queue)
    await redis.delete("q:normal")
    real_contains = queue.contains

    async def flaky_contains(job_id, priority):
        if job_id == bad:
            raise RuntimeError("boom")
        return await real_contains(job_id, priority)

    monkeypatch.setattr(queue, "contains", flaky_contains)
    result = await monitor.sweep()

    assert (result.errors, result.reenqueued) == (1, 1)
    assert await queue.contains(good, Priority.NORMAL)


async def test_cancelled_and_finished_jobs_are_ignored(monitor, sessionmaker, queue, redis):
    job_id = await submit(sessionmaker, queue)
    async with sessionmaker() as session:
        await session.execute(
            update(Job).where(Job.id == job_id).values(status=JobStatus.SUCCEEDED)
        )
        await session.commit()
    await redis.delete("q:normal")

    result = await monitor.sweep()

    assert (result.requeued, result.failed, result.reenqueued) == (0, 0, 0)
