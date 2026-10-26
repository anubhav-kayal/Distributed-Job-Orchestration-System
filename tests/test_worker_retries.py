import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from djos.coordination import Leases
from djos.jobs import service
from djos.jobs.retry import RetryPolicy
from djos.jobs.schemas import JobCreate
from djos.models import Effect, Job, JobStatus
from djos.worker import Worker

pytestmark = pytest.mark.integration


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


@pytest.fixture
def worker(sessionmaker, queue, leases) -> Worker:
    return Worker(
        sessionmaker,
        queue,
        leases,
        worker_id="w",
        block_seconds=0.1,
        heartbeat_seconds=1,
        retry_policy=RetryPolicy(base_seconds=10, cap_seconds=60),
    )


async def submit(sessionmaker, queue, **body) -> uuid.UUID:
    async with sessionmaker() as session:
        job, _ = await service.submit_job(session, queue, JobCreate(**body))
        return job.id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


async def test_retryable_failure_with_attempts_left_is_scheduled_for_later(
    worker, sessionmaker, queue, leases
):
    job_id = await submit(sessionmaker, queue, type="flaky", payload={"fail_times": 1})
    before = datetime.now(UTC)

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.SCHEDULED
    assert job.attempts == 1
    assert job.last_error == "RuntimeError: flaky failure on attempt 1 of 1"
    # First retry: delay in [0, base=10s].
    assert before <= job.run_at <= datetime.now(UTC) + timedelta(seconds=10)
    assert job.finished_at is None
    assert await leases.holder(job_id) is None
    assert not await queue.contains(job_id, job.priority)  # the scheduler re-queues it


async def test_out_of_attempts_fails_for_good(worker, sessionmaker, queue):
    job_id = await submit(sessionmaker, queue, type="fail", max_attempts=2)
    async with sessionmaker() as session:  # pretend attempt 1 already happened
        await session.execute(update(Job).where(Job.id == job_id).values(attempts=1))
        await session.commit()

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert (job.status, job.attempts) == (JobStatus.FAILED, 2)
    assert job.finished_at is not None


async def test_permanent_error_is_not_retried(worker, sessionmaker, queue):
    job_id = await submit(
        sessionmaker, queue, type="fail", payload={"permanent": True, "message": "bad input"}
    )

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert (job.status, job.attempts) == (JobStatus.FAILED, 1)  # 2 attempts left, unused
    assert job.last_error == "PermanentError: bad input"


async def test_unknown_job_type_is_not_retried(worker, sessionmaker, queue):
    job_id = await submit(sessionmaker, queue, type="sleep", payload={"seconds": 0})
    async with sessionmaker() as session:  # e.g. the handler was removed in a deploy
        await session.execute(update(Job).where(Job.id == job_id).values(type="removed"))
        await session.commit()

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert (job.status, job.attempts) == (JobStatus.FAILED, 1)
    assert job.last_error.startswith("UnknownJobType")


async def test_failed_attempts_record_no_effect(worker, sessionmaker, queue):
    job_id = await submit(sessionmaker, queue, type="flaky", payload={"fail_times": 1})
    await worker.process_next()
    async with sessionmaker() as session:
        assert list(await session.scalars(select(Effect).where(Effect.job_id == job_id))) == []
