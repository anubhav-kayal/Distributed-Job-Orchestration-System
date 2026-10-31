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
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from djos.coordination import Leases
from djos.core import metrics
from djos.core.logging import bind
from djos.handlers import JobContext, PermanentError, UnknownJobType, get_handler
from djos.jobs.retry import RetryPolicy
from djos.jobs.states import TERMINAL_STATUSES, transition
from djos.models import Effect, Job, JobStatus, Priority, Tenant
from djos.queue import PostgresQueue, Queue, WeightedRotation
from djos.workflows.engine import on_job_finished

log = logging.getLogger(__name__)

_DEFERRED = object()  # sentinel: a job was claimed-then-deferred (tenant at its cap)

MAX_ERROR_LENGTH = 4000


def _describe(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]


def default_worker_id() -> str:
    # In a container the hostname is the container ID, so this is unique per replica.
    return f"{socket.gethostname()}-{os.getpid()}"


@dataclass(frozen=True)
class Outcome:
    """What one run of the handler produced. _finish decides the job's next status:
    succeeded, scheduled (retry later) or dead."""

    succeeded: bool
    result: dict[str, Any] | None = None
    error: str | None = None
    retryable: bool = True


class LeaseLost(Exception):
    pass


class Worker:
    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        queue: Queue,
        leases: Leases,
        *,
        worker_id: str | None = None,
        block_seconds: float = 1.0,
        heartbeat_seconds: float = 10.0,
        retry_policy: RetryPolicy | None = None,
        priority_weights: dict[Priority, int] | None = None,
        tenant_defer_seconds: float = 1.0,
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
        self.rotation = WeightedRotation(priority_weights)
        self.tenant_defer_seconds = tenant_defer_seconds

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
        """Take and process one job. Returns False if none arrived within block_seconds."""
        if self.queue.claims_in_postgres:
            return await self._process_next_from_postgres()

        job_id = await self.queue.dequeue(
            block_seconds=self.block_seconds, prefer=self.rotation.next()
        )
        if job_id is None:
            return False

        owner = self._new_owner()
        if not await self.leases.acquire(job_id, owner):
            log.info("job %s is leased by %s; skipping", job_id, await self.leases.holder(job_id))
            return True
        try:
            with bind(job_id=job_id, worker_id=self.worker_id):
                job = await self._claim(job_id)
                if job is not None:
                    await self._run_claimed(job, owner)
        finally:
            await self._release(job_id, owner)
        return True

    async def _process_next_from_postgres(self) -> bool:
        """Postgres backend: pick-and-claim in one transaction; poll with backoff when idle
        (there's no blocking pop as with Redis)."""
        deadline = time.monotonic() + self.block_seconds
        delay = self.queue.poll_seconds
        while True:
            claimed = await self._claim_from_postgres(self.rotation.next())
            if claimed is _DEFERRED:
                return True
            if claimed is not None:
                job, owner = claimed
                try:
                    with bind(job_id=job.id, worker_id=self.worker_id):
                        await self._run_claimed(job, owner)
                finally:
                    await self._release(job.id, owner)
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, 0.5)

    async def _claim_from_postgres(self, prefer: Priority):
        """Lock the next queued job (SKIP LOCKED), lease it and mark it running, all in one
        transaction. Returns (job, owner), _DEFERRED, or None if nothing is queued."""
        async with self.sessionmaker() as session:
            for tier in PostgresQueue.order(prefer):
                skip: set[uuid.UUID] = set()
                while job := (
                    await session.scalars(PostgresQueue.next_in_tier(tier, skip))
                ).first():
                    owner = self._new_owner()
                    if not await self.leases.acquire(job.id, owner):
                        skip.add(job.id)  # a lease is still held (e.g. a just-reclaimed job)
                        continue
                    try:
                        if await self._tenant_at_capacity(session, job):
                            self._defer(job)
                            await session.commit()
                            return _DEFERRED
                        transition(job, JobStatus.RUNNING)
                        job.locked_by = self.worker_id
                        await session.commit()
                    except BaseException:
                        await self._release(job.id, owner)
                        raise
                    return job, owner
        return None

    def _new_owner(self) -> str:
        # Unique per claim, so a lease from this worker's earlier run of the same job
        # can never be mistaken for the current one.
        return f"{self.worker_id}:{uuid.uuid4().hex[:12]}"

    async def _release(self, job_id: uuid.UUID, owner: str) -> None:
        try:
            await self.leases.release(job_id, owner)
        except RedisError:
            log.warning("could not release lease on %s; it will expire", job_id)

    async def _run_claimed(self, job: Job, owner: str) -> None:
        runnable_since = job.run_at or job.created_at
        metrics.QUEUE_WAIT.labels(job.priority.value).observe(
            max(0.0, (job.started_at - runnable_since).total_seconds())
        )
        metrics.WORKER_BUSY.set(1)
        try:
            with bind(tenant_id=job.tenant_id, lease_token=job.lease_token):
                await self._run_and_record(job, owner)
        finally:
            metrics.WORKER_BUSY.set(0)

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
            if await self._tenant_at_capacity(session, job):
                self._defer(job)
                await session.commit()
                return None
            transition(job, JobStatus.RUNNING)
            job.locked_by = self.worker_id
            await session.commit()
            return job

    def _defer(self, job: Job) -> None:
        """Don't run it now, and don't hot-loop on it either: hand it back to the scheduler
        for a moment, so other tenants' jobs get the worker meanwhile."""
        job.run_at = datetime.now(UTC) + timedelta(seconds=self.tenant_defer_seconds)
        transition(job, JobStatus.SCHEDULED)
        metrics.JOBS_DEFERRED.inc()
        log.info("job %s deferred: tenant %s is at its concurrency limit", job.id, job.tenant_id)

    async def _tenant_at_capacity(self, session, job: Job) -> bool:
        """Whether the job's tenant already has max_concurrency jobs running.

        The tenant row is locked (FOR UPDATE) while counting, so two workers claiming
        jobs of the same tenant at once take turns: neither can count, see room for one
        more, and both start. Lock order is job → tenant, the same on every path.
        """
        limit = await session.scalar(
            select(Tenant.max_concurrency).where(Tenant.id == job.tenant_id)
        )
        if limit is None:
            return False
        await session.execute(select(Tenant.id).where(Tenant.id == job.tenant_id).with_for_update())
        running = await session.scalar(
            select(func.count()).where(
                Job.tenant_id == job.tenant_id, Job.status == JobStatus.RUNNING
            )
        )
        return running >= limit

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
            metrics.LEASES_LOST.inc()
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
            return Outcome(False, error=_describe(exc), retryable=False)
        started = time.monotonic()
        try:
            result = await handler.fn(payload, ctx)
        except PermanentError as exc:
            log.warning("job %s failed permanently: %s", job.id, exc)
            return Outcome(False, error=_describe(exc), retryable=False)
        except Exception as exc:
            log.exception("job %s failed", job.id)
            return Outcome(False, error=_describe(exc))
        finally:
            metrics.JOB_DURATION.labels(job.type).observe(time.monotonic() - started)
        log.info("job %s succeeded", job.id)
        return Outcome(True, result=result)

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
                metrics.FENCED_WRITES.inc()
                return False
            job.result = outcome.result
            job.last_error = outcome.error
            if outcome.succeeded:
                transition(job, JobStatus.SUCCEEDED)
                result_label = "succeeded"
            elif outcome.retryable and job.attempts < job.max_attempts:
                delay = self.retry_policy.delay(job.attempts)
                job.run_at = datetime.now(UTC) + timedelta(seconds=delay)
                transition(job, JobStatus.SCHEDULED)
                result_label = "retry"
                log.info(
                    "job %s will retry in %.2fs (attempt %d of %d failed)",
                    job_id,
                    delay,
                    job.attempts,
                    job.max_attempts,
                )
            else:
                transition(job, JobStatus.DEAD)
                result_label = "dead"
                log.warning(
                    "job %s is dead after %d attempt(s): %s", job_id, job.attempts, outcome.error
                )
            if outcome.succeeded:
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
            # Still inside the fenced transaction: unblocking children (or failing them)
            # commits together with this outcome, so it happens exactly once.
            ready = await on_job_finished(session, job) if job.status in TERMINAL_STATUSES else []
            await session.commit()
        metrics.JOBS_FINISHED.labels(job.type, result_label).inc()
        for child in ready:
            try:
                await self.queue.enqueue(child.id, child.priority)
            except RedisError:
                log.warning("enqueue of unblocked job %s failed; monitor will re-push", child.id)
        return True
