import asyncio
import uuid

import pytest
from sqlalchemy import select

from djos.coordination import Leases
from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.models import Job, JobStatus, Tenant
from djos.scheduler import Scheduler
from djos.tenancy.ratelimit import RateLimiter
from djos.worker import Worker


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


def make_worker(sessionmaker, queue, leases, name="w") -> Worker:
    return Worker(
        sessionmaker,
        queue,
        leases,
        worker_id=name,
        block_seconds=0.05,
        heartbeat_seconds=1,
        tenant_defer_seconds=0.2,
    )


async def add_tenant(sessionmaker, name, **limits) -> uuid.UUID:
    async with sessionmaker() as session:
        tenant = Tenant(name=name, **limits)
        session.add(tenant)
        await session.commit()
        return tenant.id


async def submit(sessionmaker, queue, tenant_id, seconds=0.0) -> uuid.UUID:
    async with sessionmaker() as session:
        job, _ = await service.submit_job(
            session,
            queue,
            JobCreate(type="sleep", payload={"seconds": seconds}),
            tenant_id=tenant_id,
        )
        return job.id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


@pytest.mark.integration
async def test_job_over_the_concurrency_limit_is_deferred_not_run(sessionmaker, queue, leases):
    t = await add_tenant(sessionmaker, "capped", max_concurrency=1)
    first, second = await submit(sessionmaker, queue, t), await submit(sessionmaker, queue, t)
    running = await make_worker(sessionmaker, queue, leases)._claim(first)

    assert await make_worker(sessionmaker, queue, leases, "w2")._claim(second) is None

    job = await load(sessionmaker, second)
    assert (job.status, job.attempts, job.lease_token) == (JobStatus.SCHEDULED, 0, 0)
    assert job.run_at is not None
    assert running.status is JobStatus.RUNNING


@pytest.mark.integration
async def test_racing_claims_cannot_exceed_the_limit(sessionmaker, queue, leases):
    t = await add_tenant(sessionmaker, "capped", max_concurrency=1)
    jobs = [await submit(sessionmaker, queue, t) for _ in range(6)]
    workers = [make_worker(sessionmaker, queue, leases, f"w{i}") for i in range(6)]

    claimed = await asyncio.gather(*(w._claim(j) for w, j in zip(workers, jobs, strict=True)))

    assert sum(c is not None for c in claimed) == 1


@pytest.mark.integration
async def test_unlimited_tenants_are_unaffected(sessionmaker, queue, leases):
    capped = await add_tenant(sessionmaker, "capped", max_concurrency=1)
    other = await add_tenant(sessionmaker, "other")
    await make_worker(sessionmaker, queue, leases)._claim(await submit(sessionmaker, queue, capped))
    assert await make_worker(sessionmaker, queue, leases)._claim(
        await submit(sessionmaker, queue, other)
    )


@pytest.mark.integration
async def test_a_noisy_tenant_cannot_starve_a_quiet_one(sessionmaker, queue, leases):
    noisy = await add_tenant(sessionmaker, "noisy", max_concurrency=1)
    quiet = await add_tenant(sessionmaker, "quiet")
    flood = [await submit(sessionmaker, queue, noisy, seconds=0.1) for _ in range(20)]
    polite = [await submit(sessionmaker, queue, quiet, seconds=0.1) for _ in range(5)]
    workers = [make_worker(sessionmaker, queue, leases, f"w{i}") for i in range(3)]
    scheduler = Scheduler(sessionmaker, queue)

    async def run_until(done) -> None:
        for _ in range(400):
            await asyncio.gather(*(w.process_next() for w in workers), scheduler.promote_due())
            if await done():
                return

    async def quiet_done() -> bool:
        statuses = [(await load(sessionmaker, j)).status for j in polite]
        return all(s is JobStatus.SUCCEEDED for s in statuses)

    await run_until(quiet_done)

    assert await quiet_done()
    async with sessionmaker() as session:
        noisy_left = await session.scalar(
            select(Job.id).where(Job.id.in_(flood), Job.status != JobStatus.SUCCEEDED).limit(1)
        )
    # The quiet tenant finished all its jobs while the noisy one was still working
    # through its backlog one at a time, even though the flood was submitted first.
    assert noisy_left is not None


@pytest.mark.integration
async def test_token_bucket_refills_over_time(redis):
    limiter, tenant = RateLimiter(redis), uuid.uuid4()
    assert (await limiter.take(tenant, rate=10, burst=1)).allowed
    denied = await limiter.take(tenant, rate=10, burst=1)
    assert not denied.allowed
    assert 0 < denied.retry_after <= 0.1
    await asyncio.sleep(0.12)
    assert (await limiter.take(tenant, rate=10, burst=1)).allowed


@pytest.mark.integration
async def test_concurrent_takes_never_overspend(redis):
    limiter, tenant = RateLimiter(redis), uuid.uuid4()
    results = await asyncio.gather(*(limiter.take(tenant, rate=0.01, burst=5) for _ in range(50)))
    assert sum(r.allowed for r in results) == 5


async def test_limiter_fails_open_when_redis_is_down():
    from redis.asyncio import Redis

    limiter = RateLimiter(Redis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.2))
    assert (await limiter.take(uuid.uuid4(), rate=1, burst=1)).allowed
