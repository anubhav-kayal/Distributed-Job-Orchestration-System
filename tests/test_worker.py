import asyncio
import uuid

import pytest

from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.models import Job, JobStatus, Priority
from djos.worker import Worker

pytestmark = pytest.mark.integration


@pytest.fixture
def worker(sessionmaker, queue) -> Worker:
    return Worker(sessionmaker, queue, worker_id="test-worker", block_seconds=0.1)


async def submit(sessionmaker, queue, **body) -> uuid.UUID:
    body.setdefault("type", "sleep")
    body.setdefault("payload", {"seconds": 0})
    async with sessionmaker() as session:
        job, _ = await service.submit_job(session, queue, JobCreate(**body))
        return job.id


async def load(sessionmaker, job_id) -> Job:
    async with sessionmaker() as session:
        return await session.get(Job, job_id)


async def test_runs_job_to_success(worker, sessionmaker, queue):
    job_id = await submit(sessionmaker, queue, type="hash", payload={"data": "x", "iterations": 1})

    assert await worker.process_next() is True

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.attempts == 1
    assert len(job.result["sha256"]) == 64
    assert job.last_error is None
    assert job.started_at <= job.finished_at


async def test_handler_exception_marks_job_failed(worker, sessionmaker, queue):
    job_id = await submit(sessionmaker, queue, type="flaky", payload={"fail_times": 1})

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.FAILED
    assert job.last_error == "RuntimeError: flaky failure on attempt 1 of 1"
    assert job.result is None
    assert job.finished_at is not None


async def test_empty_queue_returns_false(worker):
    assert await worker.process_next() is False


async def test_duplicate_push_runs_job_once(worker, sessionmaker, queue):
    job_id = await submit(sessionmaker, queue)
    await queue.enqueue(job_id, Priority.NORMAL)  # simulate a duplicate push

    assert await worker.process_next() is True
    assert await worker.process_next() is True  # popped the duplicate and skipped it
    assert await worker.process_next() is False

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.attempts == 1


async def test_job_cancelled_while_queued_is_skipped(worker, sessionmaker, queue):
    job_id = await submit(sessionmaker, queue)
    async with sessionmaker() as session:
        await service.cancel_job(session, job_id)

    await worker.process_next()

    job = await load(sessionmaker, job_id)
    assert job.status is JobStatus.CANCELLED
    assert job.attempts == 0


async def test_unknown_job_id_is_skipped(worker, queue):
    await queue.enqueue(uuid.uuid4(), Priority.NORMAL)
    assert await worker.process_next() is True


async def test_high_priority_runs_first(worker, sessionmaker, queue):
    low = await submit(sessionmaker, queue, priority="low")
    high = await submit(sessionmaker, queue, priority="high")

    await worker.process_next()

    assert (await load(sessionmaker, high)).status is JobStatus.SUCCEEDED
    assert (await load(sessionmaker, low)).status is JobStatus.QUEUED


async def test_run_loop_drains_queue_and_stops_on_signal(worker, sessionmaker, queue):
    job_ids = [await submit(sessionmaker, queue) for _ in range(5)]
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    async def all_done() -> bool:
        jobs = [await load(sessionmaker, j) for j in job_ids]
        return all(j.status is JobStatus.SUCCEEDED for j in jobs)

    for _ in range(50):
        if await all_done():
            break
        await asyncio.sleep(0.1)
    assert await all_done()

    stop.set()
    await asyncio.wait_for(task, timeout=2)
