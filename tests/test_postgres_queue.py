"""The Postgres queue backend: the jobs table is the queue (SELECT … FOR UPDATE SKIP LOCKED)."""

import asyncio
import uuid

import pytest
from sqlalchemy import select

from djos.coordination import Leases
from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.models import Effect, Job, JobStatus, Priority, Tenant
from djos.queue import PostgresQueue
from djos.worker import Worker

pytestmark = pytest.mark.integration


@pytest.fixture
def pg_queue(sessionmaker) -> PostgresQueue:
    return PostgresQueue(sessionmaker, poll_seconds=0.01)


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


def make_worker(sessionmaker, pg_queue, leases, name="w") -> Worker:
    return Worker(
        sessionmaker, pg_queue, leases, worker_id=name, block_seconds=0.1, heartbeat_seconds=1
    )


async def submit(sessionmaker, pg_queue, **body) -> uuid.UUID:
    body.setdefault("type", "sleep")
    body.setdefault("payload", {"seconds": 0})
    async with sessionmaker() as session:
        job, _ = await service.submit_job(session, pg_queue, JobCreate(**body))
        return job.id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


async def test_enqueue_is_a_no_op_and_depth_comes_from_the_table(sessionmaker, pg_queue, redis):
    await submit(sessionmaker, pg_queue, priority="high")
    await submit(sessionmaker, pg_queue)
    assert await redis.keys("q:*") == []  # nothing pushed anywhere
    assert await pg_queue.depth() == {Priority.HIGH: 1, Priority.NORMAL: 1, Priority.LOW: 0}


async def test_worker_runs_jobs_from_the_table(sessionmaker, pg_queue, leases):
    job_id = await submit(sessionmaker, pg_queue)

    assert await make_worker(sessionmaker, pg_queue, leases).process_next() is True

    job = await load(sessionmaker, job_id)
    assert (job.status, job.attempts, job.locked_by) == (JobStatus.SUCCEEDED, 1, "w")
    assert await leases.holder(job_id) is None


async def test_idle_worker_polls_then_gives_up(sessionmaker, pg_queue, leases):
    assert await make_worker(sessionmaker, pg_queue, leases).process_next() is False


async def test_oldest_job_in_the_preferred_tier_first(sessionmaker, pg_queue, leases):
    low = await submit(sessionmaker, pg_queue, priority="low")
    first_normal = await submit(sessionmaker, pg_queue)
    await submit(sessionmaker, pg_queue)
    high = await submit(sessionmaker, pg_queue, priority="high")
    worker = make_worker(sessionmaker, pg_queue, leases)

    claimed = [
        await worker._claim_from_postgres(prefer=p) for p in (None, Priority.NORMAL, Priority.LOW)
    ]

    assert [job.id for job, _ in claimed] == [high, first_normal, low]


async def test_concurrent_workers_never_take_the_same_job(sessionmaker, pg_queue, leases):
    job_ids = {await submit(sessionmaker, pg_queue) for _ in range(60)}
    workers = [make_worker(sessionmaker, pg_queue, leases, f"w{i}") for i in range(6)]

    async def drain(w):
        while await w.process_next():
            pass

    await asyncio.gather(*(drain(w) for w in workers))

    jobs = [await load(sessionmaker, j) for j in job_ids]
    assert all(j.status is JobStatus.SUCCEEDED and j.attempts == 1 for j in jobs)
    async with sessionmaker() as session:
        effects = list(await session.scalars(select(Effect.job_id)))
    assert sorted(effects) == sorted(job_ids)  # exactly one effect each
    assert len({j.locked_by for j in jobs}) > 1


async def test_tenant_cap_applies_on_this_path_too(sessionmaker, pg_queue, leases):
    async with sessionmaker() as session:
        tenant = Tenant(name="capped", max_concurrency=1)
        session.add(tenant)
        await session.commit()
    async with sessionmaker() as session:
        for _ in range(2):
            await service.submit_job(
                session, pg_queue, JobCreate(type="sleep"), tenant_id=tenant.id
            )
    worker = make_worker(sessionmaker, pg_queue, leases)

    first = await worker._claim_from_postgres(prefer=Priority.NORMAL)
    second = await worker._claim_from_postgres(prefer=Priority.NORMAL)

    assert first is not None and first[0].status is JobStatus.RUNNING
    assert second is not None and not isinstance(second, tuple)  # deferred, not claimed
