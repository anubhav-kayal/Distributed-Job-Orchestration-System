"""The worker loop: pop a job ID from Redis, claim it in Postgres, run it, record the outcome.

Before claiming, the worker takes a lease on the job in Redis (see coordination/lease.py)
and holds it until the outcome is recorded, so "running in Postgres" always comes with a
live lease unless the worker died. While the handler runs, a heartbeat task renews the
lease every `heartbeat_seconds`. If the lease is lost anyway (Redis unreachable for a
whole TTL, or the process stalled), the worker abandons the job: the monitor will
already be handing it to someone else.

Abandoning on lease loss is only best effort: a worker that stalls *without noticing*
(a long GC pause, a frozen VM, `docker pause`) can wake up after its job was reclaimed
and re-run, and carry on as if nothing happened. That is what fencing tokens are for.
Every claim gets a new, larger `lease_token`, and the final write only applies while the
job still carries the token this worker claimed with. A stale worker's write matches
nothing and is rejected, so each job's effect is recorded exactly once.
"""

import asyncio
import contextlib
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from djos.coordination import Leases
from djos.handlers import JobContext, PermanentError, UnknownJobType, get_handler
from djos.jobs.retry import RetryPolicy
from djos.jobs.states import transition
from djos.models import Effect, Job, JobStatus
from djos.queue import RedisQueue

log = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 4000


def _describe(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]


def default_worker_id() -> str:
    # In a container the hostname is the container ID, so this is unique per replica.
    return f"{socket.gethostname()}-{os.getpid()}"


@dataclass(frozen=True)
class Outcome:
    status: JobStatus  # SUCCEEDED or FAILED; _finish turns a retryable FAILED into a retry
    result: dict[str, Any] | None = None
    error: str | None = None
    retryable: bool = True


class LeaseLost(Exception):
    pass


class Worker:
    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        queue: RedisQueue,
        leases: Leases,
        *,
        worker_id: str | None = None,
        block_seconds: float = 1.0,
        heartbeat_seconds: float = 10.0,
        retry_policy: RetryPolicy | None = None,
    ):
        if heartbeat_seconds * 2 > leases.ttl_ms / 1000:
            raise ValueError("heartbeat interval must be at most half the lease TTL")
        self.sessionmaker = sessionmaker
        self.queue = queue
        self.leases = leases
        self.worker_id = worker_id or default_worker_id()
        self.block_seconds = block_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.retry_policy = retry_policy or RetryPolicy()

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

        # Unique per claim, so a lease from this worker's earlier run of the same job
        # can never be mistaken for the current one.
        owner = f"{self.worker_id}:{uuid.uuid4().hex[:12]}"
        if not await self.leases.acquire(job_id, owner):
            log.info("job %s is leased by %s; skipping", job_id, await self.leases.holder(job_id))
            return True
        try:
            job = await self._claim(job_id)
            if job is not None:
                await self._run_and_record(job, owner)
        finally:
            try:
                await self.leases.release(job_id, owner)
            except RedisError:
                log.warning("could not release lease on %s; it will expire", job_id)
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
            job.locked_by = self.worker_id
            await session.commit()
            return job

    async def _run_and_record(self, job: Job, owner: str) -> None:
        log.info("running job %s type=%s attempt=%d", job.id, job.type, job.attempts)
        work = asyncio.create_task(self._invoke(job))
        heartbeat = asyncio.create_task(self._heartbeat(job.id, owner))
        await asyncio.wait({work, heartbeat}, return_when=asyncio.FIRST_COMPLETED)

        if not work.done():
            # The heartbeat only returns by raising LeaseLost (or an unexpected error).
            work.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await work
            exc = heartbeat.exception()
            log.warning("abandoning job %s: %s", job.id, exc)
            return

        # The handler finished. The heartbeat may have raised LeaseLost in the same tick;
        # either way, try to record the outcome: the fenced write decides whether it counts.
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await heartbeat
        await self._finish(job.id, job.lease_token, work.result())

    async def _invoke(self, job: Job) -> Outcome:
        ctx = JobContext(
            job_id=job.id,
            attempt=job.attempts,
            worker_id=self.worker_id,
            lease_token=job.lease_token,
        )
        try:
            # An unknown type or a payload that no longer validates won't fix itself.
            handler = get_handler(job.type)
            payload = handler.payload_model.model_validate(job.payload)
        except (UnknownJobType, ValidationError) as exc:
            log.error("job %s can never run: %s", job.id, exc)
            return Outcome(JobStatus.FAILED, error=_describe(exc), retryable=False)
        try:
            result = await handler.fn(payload, ctx)
        except PermanentError as exc:
            log.warning("job %s failed permanently: %s", job.id, exc)
            return Outcome(JobStatus.FAILED, error=_describe(exc), retryable=False)
        except Exception as exc:
            log.exception("job %s failed", job.id)
            return Outcome(JobStatus.FAILED, error=_describe(exc))
        log.info("job %s succeeded", job.id)
        return Outcome(JobStatus.SUCCEEDED, result=result)

    async def _heartbeat(self, job_id: uuid.UUID, owner: str) -> None:
        """Renew the lease until cancelled. Raises LeaseLost once we can no longer be
        sure we hold it: Redis says it's gone, or we couldn't renew for a whole TTL."""
        ttl = self.leases.ttl_ms / 1000
        last_renewed = time.monotonic()
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            try:
                renewed = await self.leases.renew(job_id, owner)
            except RedisError as exc:
                if time.monotonic() - last_renewed >= ttl:
                    raise LeaseLost(f"could not renew for {ttl:.0f}s ({exc})") from None
                log.warning("heartbeat for %s failed (%s); will retry", job_id, exc)
                continue
            if not renewed:
                raise LeaseLost("lease expired or taken over")
            last_renewed = time.monotonic()

    async def _finish(self, job_id: uuid.UUID, token: int, outcome: Outcome) -> bool:
        """Record the outcome, fenced by `token`. Returns False if the write was rejected
        because the job has been reclaimed (and maybe re-run) since we claimed it."""
        async with self.sessionmaker() as session:
            job = (
                await session.scalars(select(Job).where(Job.id == job_id).with_for_update())
            ).one()
            if job.status is not JobStatus.RUNNING or job.lease_token != token:
                log.warning(
                    "fenced: our token %d for job %s is stale (job is %s with token %d); "
                    "discarding our result",
                    token,
                    job_id,
                    job.status,
                    job.lease_token,
                )
                return False
            job.result = outcome.result
            job.last_error = outcome.error
            if (
                outcome.status is JobStatus.FAILED
                and outcome.retryable
                and job.attempts < job.max_attempts
            ):
                delay = self.retry_policy.delay(job.attempts)
                job.run_at = datetime.now(UTC) + timedelta(seconds=delay)
                transition(job, JobStatus.SCHEDULED)
                log.info(
                    "job %s will retry in %.2fs (attempt %d of %d failed)",
                    job_id,
                    delay,
                    job.attempts,
                    job.max_attempts,
                )
            else:
                transition(job, outcome.status)
            if outcome.status is JobStatus.SUCCEEDED:
                # Same transaction as the status change: the effect exists if and only if
                # this claim's success was recorded.
                session.add(
                    Effect(
                        job_id=job_id,
                        lease_token=token,
                        worker_id=self.worker_id,
                        data=outcome.result,
                    )
                )
            await session.commit()
            return True
