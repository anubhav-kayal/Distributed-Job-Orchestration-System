import asyncio
import time

import pytest

from djos.coordination import Leases
from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.models import Job, JobStatus
from djos.worker import Worker

pytestmark = pytest.mark.integration


async def test_stop_during_a_job_lets_it_finish_and_takes_no_new_work(sessionmaker, queue, redis):
    worker = Worker(
        sessionmaker, queue, Leases(redis, ttl_seconds=5), block_seconds=0.1, heartbeat_seconds=1
    )
    async with sessionmaker() as session:
        first, _ = await service.submit_job(
            session, queue, JobCreate(type="sleep", payload={"seconds": 0.6})
        )
        second, _ = await service.submit_job(
            session, queue, JobCreate(type="sleep", payload={"seconds": 0})
        )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    await asyncio.sleep(0.2)  # first job is now running

    stop.set()  # what SIGTERM does (docker stop, a scale-down)
    started = time.monotonic()
    await asyncio.wait_for(task, timeout=5)

    assert time.monotonic() - started < 2  # exits once the current job is done
    async with sessionmaker() as session:
        assert (await session.get(Job, first.id)).status is JobStatus.SUCCEEDED  # not cut off
        assert (await session.get(Job, second.id)).status is JobStatus.QUEUED  # left for others
