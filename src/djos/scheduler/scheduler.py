"""The scheduler: fires cron schedules, and promotes `scheduled` jobs to `queued` once
their run_at has passed.

It works straight off Postgres rather than a Redis sorted set of due times. A sorted set
(the classic design) would be a second copy of "when does this job run" that has to be
kept in sync with Postgres: another dual write, with its own lost-update and recovery
cases. Querying Postgres keeps one source of truth, and the (status, run_at) index makes
the query cheap.

    SELECT … WHERE status = 'scheduled' AND run_at <= now()
    ORDER BY run_at LIMIT n FOR UPDATE SKIP LOCKED

SKIP LOCKED makes running several schedulers safe: each one locks the rows it picked,
and the others skip those rows instead of waiting for them or promoting them twice.

Cron schedules work the same way: due schedules are locked with SKIP LOCKED, and the
fired job is inserted in the same transaction that advances the schedule's next_fire_at,
so a fire and its job always exist together (or neither does). Each fired job also gets
the idempotency key `cron:{schedule_id}:{fire_time}` as a second line of defence: even
if a schedule were somehow processed twice for the same fire time, the unique key would
reject the second job. If the scheduler was down and several fire times were missed,
the schedule fires once and skips ahead rather than replaying every missed run.

"Now" always comes from Postgres, not the local clock: scheduler replicas on different
machines would otherwise disagree about what is due.

Promotion commits first and pushes to Redis second, the same order as submission. If
the push fails, the job is `queued` in Postgres but missing from Redis, which is exactly
what the monitor's stranded sweep repairs.
"""

import asyncio
import logging
from datetime import UTC, datetime

from pydantic import ValidationError
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from djos.core.db import db_now
from djos.cron.expr import next_fire_after
from djos.jobs.schemas import JobCreate
from djos.jobs.service import insert_job
from djos.jobs.states import transition
from djos.models import CronSchedule, Job, JobStatus
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
        """One pass: fire due cron schedules, then promote due jobs. Returns how many jobs
        were promoted (the run loop goes again at once after a full batch)."""
        await self.fire_due_schedules(now)
        return await self.promote_due(now)

    async def fire_due_schedules(self, now: datetime | None = None) -> int:
        """Create a job for every enabled schedule whose fire time has passed."""
        async with self.sessionmaker() as session:
            now = now or await db_now(session)
            schedules = list(
                await session.scalars(
                    select(CronSchedule)
                    .where(CronSchedule.enabled, CronSchedule.next_fire_at <= now)
                    .order_by(CronSchedule.next_fire_at)
                    .limit(self.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            fired: list[Job] = []
            for schedule in schedules:
                fire_time = schedule.next_fire_at
                job = await self._fire(session, schedule, fire_time, now)
                if job is not None:
                    fired.append(job)
                schedule.last_fired_at = fire_time
                # Next fire strictly after now: missed fires are skipped, not replayed.
                schedule.next_fire_at = next_fire_after(schedule.cron_expr, now)
                log.info(
                    "schedule %s fired for %s; next at %s",
                    schedule.name,
                    fire_time,
                    schedule.next_fire_at,
                )
            await session.commit()

        await self._enqueue([j for j in fired if j.status is JobStatus.QUEUED])
        return len(fired)

    async def _fire(
        self, session, schedule: CronSchedule, fire_time: datetime, now: datetime
    ) -> Job | None:
        """Insert the job for one fire (no commit). None if nothing was created."""
        try:
            spec = JobCreate(**schedule.job)
        except ValidationError as exc:
            # e.g. its job type was removed in a deploy. Skip this fire rather than fail
            # the whole batch and block every other schedule.
            log.error("schedule %s has an invalid job template: %s", schedule.name, exc)
            return None
        key = f"cron:{schedule.id}:{fire_time.astimezone(UTC):%Y-%m-%dT%H:%M:%SZ}"
        job = await insert_job(session, spec, key, now=now)
        if job is None:
            log.warning("schedule %s already fired for %s; skipping", schedule.name, fire_time)
        return job

    async def promote_due(self, now: datetime | None = None) -> int:
        """Move due scheduled jobs to queued and enqueue them. Returns how many."""
        async with self.sessionmaker() as session:
            now = now or await db_now(session)
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

        await self._enqueue(due)
        if due:
            log.info("promoted %d due job(s)", len(due))
        return len(due)

    async def _enqueue(self, jobs: list[Job]) -> None:
        for job in jobs:
            try:
                await self.queue.enqueue(job.id, job.priority)
            except RedisError as exc:
                log.warning("enqueue of job %s failed (%s); monitor will re-push", job.id, exc)
