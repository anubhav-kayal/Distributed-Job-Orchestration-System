"""The monitor: finds jobs that fell through the cracks and puts them back in play.

Two sweeps, both driven from Postgres (the source of truth):

1. Orphan reclaim. A job that is `running` in Postgres but has no lease in Redis lost
   its worker (crash, kill -9, OOM, lost network). It goes back to `queued` and is
   re-enqueued, or to `dead` once it has used up `max_attempts`, so a job that keeps
   killing workers can't loop forever.

2. Stranded re-enqueue. A job that has been `queued` for a while but whose ID is not in
   its Redis queue will never be popped. That happens when the push after the commit
   failed, or a worker crashed between popping the ID and claiming the job. The ID is
   pushed again.

Each sweep is bounded: orphan reclaim pages through every running job (there are about
as many as there are workers), while the stranded check looks at `batch_size` jobs per
sweep and keeps a cursor, so a large backlog is covered over several sweeps instead of
the same oldest jobs being checked every time. A failure on one job is logged and the
sweep carries on.

Both sweeps can race with workers. That's fine: each change is made under a row lock
and re-checked, and an extra ID in a queue is harmless because claiming goes through
Postgres. Running several monitors is safe for the same reasons.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from redis.exceptions import RedisError
from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import async_sessionmaker

from djos.coordination import Leases
from djos.jobs.states import transition
from djos.models import Job, JobStatus
from djos.queue import RedisQueue

log = logging.getLogger(__name__)


@dataclass
class SweepResult:
    requeued: int = 0  # orphans put back in the queue
    dead: int = 0  # orphans that had no attempts left
    reenqueued: int = 0  # stranded queued jobs pushed to Redis again
    errors: int = 0  # jobs skipped this sweep because something failed


class Monitor:
    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        queue: RedisQueue,
        leases: Leases,
        *,
        interval_seconds: float = 5.0,
        stranded_after_seconds: float = 30.0,
        batch_size: int = 100,
    ):
        self.sessionmaker = sessionmaker
        self.queue = queue
        self.leases = leases
        self.interval_seconds = interval_seconds
        self.stranded_after = timedelta(seconds=stranded_after_seconds)
        self.batch_size = batch_size
        # (updated_at, id) of the last stranded candidate checked; None = start over.
        self._stranded_cursor: tuple[datetime, uuid.UUID] | None = None

    async def run(self, stop: asyncio.Event) -> None:
        log.info("monitor started")
        while not stop.is_set():
            try:
                result = await self.sweep()
                if result.requeued or result.dead or result.reenqueued or result.errors:
                    log.info("sweep: %s", result)
            except RedisError as exc:
                log.warning("redis unavailable (%s); will retry", exc)
            except Exception:
                log.exception("sweep failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval_seconds)
            except TimeoutError:
                pass
        log.info("monitor stopped")

    async def sweep(self) -> SweepResult:
        result = SweepResult()
        await self.reclaim_orphans(result)
        await self.reenqueue_stranded(result)
        return result

    async def reclaim_orphans(self, result: SweepResult) -> None:
        after: uuid.UUID | None = None
        while True:
            query = select(Job.id).where(Job.status == JobStatus.RUNNING)
            if after is not None:
                query = query.where(Job.id > after)
            async with self.sessionmaker() as session:
                running = list(await session.scalars(query.order_by(Job.id).limit(self.batch_size)))
            if not running:
                return
            alive = await self.leases.alive(running)
            for job_id, has_lease in zip(running, alive, strict=True):
                if not has_lease:
                    await self._guarded(self._reclaim(job_id, result), job_id, result)
            after = running[-1]

    async def _guarded(self, step, job_id: uuid.UUID, result: SweepResult) -> None:
        """Run one per-job step; on failure log it and let the sweep move on."""
        try:
            await step
        except Exception:
            result.errors += 1
            log.exception("monitor step failed for job %s; will retry next sweep", job_id)

    async def _reclaim(self, job_id, result: SweepResult) -> None:
        async with self.sessionmaker() as session:
            # SKIP LOCKED: if a worker is finishing this job right now, leave it alone
            # rather than wait; the next sweep will see the outcome.
            job = (
                await session.scalars(
                    select(Job)
                    .where(Job.id == job_id, Job.status == JobStatus.RUNNING)
                    .with_for_update(skip_locked=True)
                )
            ).one_or_none()
            # Re-check under the row lock: the job may have finished since we looked.
            if job is None or await self.leases.holder(job_id) is not None:
                return

            reason = (
                f"lease expired on attempt {job.attempts} (worker {job.locked_by} presumed dead)"
            )
            if job.attempts >= job.max_attempts:
                job.last_error = f"{reason}; no attempts left"
                transition(job, JobStatus.DEAD)
                result.dead += 1
            else:
                job.last_error = f"{reason}; requeued"
                transition(job, JobStatus.QUEUED)
                result.requeued += 1
            await session.commit()
            log.warning("job %s: %s", job_id, job.last_error)

        if job.status is JobStatus.QUEUED:
            await self.queue.enqueue(job.id, job.priority)

    async def reenqueue_stranded(self, result: SweepResult) -> None:
        cutoff = datetime.now(UTC) - self.stranded_after
        query = select(Job.id, Job.priority, Job.updated_at).where(
            Job.status == JobStatus.QUEUED, Job.updated_at < cutoff
        )
        if self._stranded_cursor is not None:
            query = query.where(tuple_(Job.updated_at, Job.id) > tuple_(*self._stranded_cursor))
        async with self.sessionmaker() as session:
            candidates = (
                await session.execute(query.order_by(Job.updated_at, Job.id).limit(self.batch_size))
            ).all()

        # Continue after this batch next time; wrap around once we've reached the end.
        if len(candidates) < self.batch_size:
            self._stranded_cursor = None
        else:
            self._stranded_cursor = (candidates[-1].updated_at, candidates[-1].id)

        for job_id, priority, _ in candidates:
            await self._guarded(
                self._reenqueue_if_missing(job_id, priority, result), job_id, result
            )

    async def _reenqueue_if_missing(self, job_id, priority, result: SweepResult) -> None:
        if not await self.queue.contains(job_id, priority):
            await self.queue.enqueue(job_id, priority)
            result.reenqueued += 1
            log.warning("job %s was queued but missing from Redis; re-enqueued", job_id)
