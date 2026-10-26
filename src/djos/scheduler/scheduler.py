"""The scheduler: promotes `scheduled` jobs to `queued` once their run_at has passed.

It works straight off Postgres rather than a Redis sorted set of due times. A sorted set
(the classic design) would be a second copy of "when does this job run" that has to be
kept in sync with Postgres: another dual write, with its own lost-update and recovery
cases. Querying Postgres keeps one source of truth, and the (status, run_at) index makes
the query cheap.

    SELECT … WHERE status = 'scheduled' AND run_at <= now()
    ORDER BY run_at LIMIT n FOR UPDATE SKIP LOCKED

SKIP LOCKED makes running several schedulers safe: each one locks the rows it picked,
and the others skip those rows instead of waiting for them or promoting them twice.

Promotion commits first and pushes to Redis second, the same order as submission. If
the push fails, the job is `queued` in Postgres but missing from Redis, which is exactly
what the monitor's stranded sweep repairs.
"""

import asyncio
import logging
from datetime import UTC, datetime

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from djos.jobs.states import transition
from djos.models import Job, JobStatus
from djos.queue import RedisQueue

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        queue: RedisQueue,
        *,
        interval_seconds: float = 0.5,
        batch_size: int = 100,
    ):
        self.sessionmaker = sessionmaker
        self.queue = queue
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size

    async def run(self, stop: asyncio.Event) -> None:
        log.info("scheduler started")
        while not stop.is_set():
            try:
                promoted = await self.tick()
            except Exception:
                log.exception("scheduler tick failed")
                promoted = 0
            # A full batch means there may be more due right now: go again immediately.
            if promoted < self.batch_size:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval_seconds)
                except TimeoutError:
                    pass
        log.info("scheduler stopped")

    async def tick(self, now: datetime | None = None) -> int:
        return await self.promote_due(now)

    async def promote_due(self, now: datetime | None = None) -> int:
        """Move due scheduled jobs to queued and enqueue them. Returns how many."""
        now = now or datetime.now(UTC)
        async with self.sessionmaker() as session:
            due = list(
                await session.scalars(
                    select(Job)
                    .where(Job.status == JobStatus.SCHEDULED, Job.run_at <= now)
                    .order_by(Job.run_at)
                    .limit(self.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for job in due:
                transition(job, JobStatus.QUEUED)
            await session.commit()

        for job in due:
            try:
                await self.queue.enqueue(job.id, job.priority)
            except RedisError as exc:
                log.warning("enqueue of due job %s failed (%s); monitor will re-push", job.id, exc)
        if due:
            log.info("promoted %d due job(s)", len(due))
        return len(due)
