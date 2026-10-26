import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from djos.coordination import Leases
from djos.jobs import service
from djos.jobs.retry import RetryPolicy
from djos.jobs.schemas import JobCreate
from djos.models import Job, JobStatus, Priority
from djos.scheduler import Scheduler
from djos.worker import Worker

pytestmark = pytest.mark.integration


async def submit(sessionmaker, queue, **body) -> uuid.UUID:
    body.setdefault("type", "sleep")
    body.setdefault("payload", {"seconds": 0})
    async with sessionmaker() as session:
        job, _ = await service.submit_job(session, queue, JobCreate(**body))
        return job.id


async def make_scheduled(sessionmaker, queue, redis, run_at: datetime) -> uuid.UUID:
    job_id = await submit(sessionmaker, queue)
    async with sessionmaker() as session:
        await session.execute(
            update(Job).where(Job.id == job_id).values(status=JobStatus.SCHEDULED, run_at=run_at)
        )
        await session.commit()
    await redis.delete("q:normal")
    return job_id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


async def test_due_jobs_are_queued_and_future_jobs_wait(sessionmaker, queue, redis):
    now = datetime.now(UTC)
    due = await make_scheduled(sessionmaker, queue, redis, now - timedelta(seconds=1))
    later = await make_scheduled(sessionmaker, queue, redis, now + timedelta(hours=1))

    assert await Scheduler(sessionmaker, queue).promote_due() == 1

    assert (await load(sessionmaker, due)).status is JobStatus.QUEUED
    assert await queue.contains(due, Priority.NORMAL)
    assert (await load(sessionmaker, later)).status is JobStatus.SCHEDULED
    assert not await queue.contains(later, Priority.NORMAL)


async def test_concurrent_schedulers_promote_each_job_exactly_once(sessionmaker, queue, redis):
    past = datetime.now(UTC) - timedelta(seconds=1)
    job_ids = {await make_scheduled(sessionmaker, queue, redis, past) for _ in range(40)}
    schedulers = [Scheduler(sessionmaker, queue, batch_size=5) for _ in range(4)]

    async def drain(s: Scheduler) -> int:
        total = 0
        while n := await s.promote_due():
            total += n
        return total

    counts = await asyncio.gather(*(drain(s) for s in schedulers))

    assert sum(counts) == 40
    queued = await redis.lrange("q:normal", 0, -1)
    assert len(queued) == 40
    assert {uuid.UUID(j) for j in queued} == job_ids


async def test_flaky_job_succeeds_on_third_attempt_after_backoff(sessionmaker, queue, redis):
    worker = Worker(
        sessionmaker,
        queue,
        Leases(redis, ttl_seconds=5),
        block_seconds=0.05,
        heartbeat_seconds=1,
        retry_policy=RetryPolicy(base_seconds=0.2, cap_seconds=1),
    )
    scheduler = Scheduler(sessionmaker, queue)
    job_id = await submit(sessionmaker, queue, type="flaky", payload={"fail_times": 2})

    history = []
    for _ in range(200):
        await worker.process_next()
        job = await load(sessionmaker, job_id)
        if not history or history[-1][0] != (job.status, job.attempts):
            history.append(((job.status, job.attempts), job.last_error))
        if job.status is JobStatus.SUCCEEDED:
            break
        await scheduler.promote_due()
        await asyncio.sleep(0.02)

    assert [state for state, _ in history] == [
        (JobStatus.SCHEDULED, 1),
        (JobStatus.SCHEDULED, 2),
        (JobStatus.SUCCEEDED, 3),
    ]
    assert job.result == {"succeeded_on_attempt": 3}
    assert history[1][1] == "RuntimeError: flaky failure on attempt 2 of 2"


async def test_cancelling_a_scheduled_job_stops_it(sessionmaker, queue, redis):
    job_id = await make_scheduled(
        sessionmaker, queue, redis, datetime.now(UTC) + timedelta(seconds=1)
    )
    async with sessionmaker() as session:
        await service.cancel_job(session, job_id)

    await Scheduler(sessionmaker, queue).promote_due(datetime.now(UTC) + timedelta(hours=1))

    assert (await load(sessionmaker, job_id)).status is JobStatus.CANCELLED
