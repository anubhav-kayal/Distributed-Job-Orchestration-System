"""The worker loop: pop a job ID from Redis, claim it in Postgres, run it, record the outcome.

Known gaps, closed on later days:
- A crash after popping but before claiming loses the ID from Redis. The job is still
  `queued` in Postgres, and the monitor re-enqueues it (Day 5).
- A crash while running leaves the job `running` forever. Leases and heartbeats detect
  this, and the monitor reclaims the job (Days 4–5).
- A stalled worker could finish a job someone else has re-run. Fencing tokens reject
  its writes (Day 6).
"""

import asyncio
import logging
import os
import socket
import uuid
from typing import Any

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from djos.handlers import JobContext, get_handler
from djos.jobs.states import transition
from djos.models import Job, JobStatus
from djos.queue import RedisQueue

log = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 4000


def default_worker_id() -> str:
    # In a container the hostname is the container ID, so this is unique per replica.
    return f"{socket.gethostname()}-{os.getpid()}"


class Worker:
    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        queue: RedisQueue,
        *,
        worker_id: str | None = None,
        block_seconds: float = 1.0,
    ):
        self.sessionmaker = sessionmaker
        self.queue = queue
        self.worker_id = worker_id or default_worker_id()
        self.block_seconds = block_seconds

    async def run(self, stop: asyncio.Event) -> None:
        """Process jobs until `stop` is set. A job in progress is always finished first;
        an idle worker notices `stop` within `block_seconds`."""
        log.info("worker %s started", self.worker_id)
        while not stop.is_set():
            try:
                await self.process_next()
            except RedisError as exc:
                # Redis restarting or unreachable: back off and keep going.
                log.warning("redis unavailable (%s); retrying in 1s", exc)
                await asyncio.sleep(1)
            except Exception:
                # Never let one bad job or a transient DB error kill the worker.
                log.exception("unexpected error in worker loop")
                await asyncio.sleep(1)
        log.info("worker %s stopped", self.worker_id)

    async def process_next(self) -> bool:
        """Pop and process one job. Returns False if the queue stayed empty."""
        job_id = await self.queue.dequeue(block_seconds=self.block_seconds)
        if job_id is None:
            return False
        job = await self._claim(job_id)
        if job is not None:
            await self._execute(job)
        return True

    async def _claim(self, job_id: uuid.UUID) -> Job | None:
        """Move the job queued → running in Postgres. Redis only told us the ID; Postgres
        decides whether it is still ours to run."""
        async with self.sessionmaker() as session:
            job = (
                await session.scalars(select(Job).where(Job.id == job_id).with_for_update())
            ).one_or_none()
            if job is None:
                log.warning("popped unknown job %s; skipping", job_id)
                return None
            if job.status is not JobStatus.QUEUED:
                # Duplicate push, or cancelled while waiting in the queue.
                log.info("job %s is %s, not queued; skipping", job_id, job.status)
                return None
            transition(job, JobStatus.RUNNING)
            await session.commit()
            return job

    async def _execute(self, job: Job) -> None:
        ctx = JobContext(job_id=job.id, attempt=job.attempts, worker_id=self.worker_id)
        log.info("running job %s type=%s attempt=%d", job.id, job.type, job.attempts)
        try:
            handler = get_handler(job.type)
            payload = handler.payload_model.model_validate(job.payload)
            result = await handler.fn(payload, ctx)
        except Exception as exc:
            log.exception("job %s failed", job.id)
            error = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
            await self._finish(job.id, JobStatus.FAILED, error=error)
        else:
            log.info("job %s succeeded", job.id)
            await self._finish(job.id, JobStatus.SUCCEEDED, result=result)

    async def _finish(
        self,
        job_id: uuid.UUID,
        status: JobStatus,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        async with self.sessionmaker() as session:
            job = (
                await session.scalars(select(Job).where(Job.id == job_id).with_for_update())
            ).one()
            if job.status is not JobStatus.RUNNING:
                log.warning(
                    "job %s changed to %s while running; dropping result", job_id, job.status
                )
                return
            job.result = result
            job.last_error = error
            transition(job, status)
            await session.commit()
