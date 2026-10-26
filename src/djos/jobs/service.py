import hashlib
import json
import logging
import uuid

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from djos.jobs.schemas import JobCreate
from djos.jobs.states import transition
from djos.models import Job, JobStatus
from djos.queue import RedisQueue

log = logging.getLogger(__name__)


class JobNotFound(Exception):
    pass


class IdempotencyKeyReused(Exception):
    """The same Idempotency-Key was sent with a different request body."""


def request_fingerprint(data: JobCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_job(
    session: AsyncSession, data: JobCreate, idempotency_key: str | None = None
) -> tuple[Job, bool]:
    """Create a job, or return the existing one for a repeated Idempotency-Key.

    Returns (job, created). Safe under concurrency: two simultaneous requests with the
    same key race on the unique index, exactly one INSERT wins, and the loser reads the
    winner's row. A check-then-insert ("SELECT, and INSERT if missing") would let both
    requests see "missing" and create two jobs.
    """
    fingerprint = request_fingerprint(data)
    stmt = (
        insert(Job)
        .values(
            type=data.type,
            payload=data.payload,
            priority=data.priority,
            max_attempts=data.max_attempts,
            idempotency_key=idempotency_key,
            request_hash=fingerprint if idempotency_key else None,
        )
        .on_conflict_do_nothing(index_elements=[Job.idempotency_key])
        .returning(Job)
    )
    job = (await session.scalars(stmt)).one_or_none()
    if job is not None:
        await session.commit()
        return job, True

    # Conflict: a job with this key already exists. NULL keys never conflict, so we
    # only get here when a key was supplied.
    existing = (
        await session.scalars(select(Job).where(Job.idempotency_key == idempotency_key))
    ).one()
    if existing.request_hash != fingerprint:
        raise IdempotencyKeyReused(idempotency_key)
    return existing, False


async def submit_job(
    session: AsyncSession, queue: RedisQueue, data: JobCreate, idempotency_key: str | None = None
) -> tuple[Job, bool]:
    """Create the job durably in Postgres, then push its ID onto the Redis queue.

    This is a dual write (two systems, no shared transaction), so the order matters:
    - Commit first, push second. Pushing first could hand a worker an ID whose row
      isn't committed yet, or never will be if the commit fails.
    - If the push fails, the job is still safely `queued` in Postgres, so we don't fail
      the request. A client retry with the same Idempotency-Key pushes it again, and
      the monitor (Day 5) re-enqueues stranded jobs.
    Pushing an ID twice is harmless: workers claim jobs through Postgres, and a job
    that is no longer `queued` is skipped.
    """
    job, created = await create_job(session, data, idempotency_key)
    if job.status is JobStatus.QUEUED:
        try:
            await queue.enqueue(job.id, job.priority)
        except RedisError:
            log.warning("enqueue failed; job %s is stored but not yet queued", job.id)
    return job, created


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise JobNotFound(job_id)
    return job


async def list_jobs(
    session: AsyncSession, *, status: JobStatus | None, limit: int, offset: int
) -> list[Job]:
    stmt = select(Job).order_by(Job.created_at.desc(), Job.id).limit(limit).offset(offset)
    if status is not None:
        stmt = stmt.where(Job.status == status)
    return list(await session.scalars(stmt))


async def cancel_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    """Cancel a job that hasn't started yet.

    SELECT ... FOR UPDATE locks the row until commit, so a worker can't claim the job
    between our status check and our write.
    """
    job = (
        await session.scalars(select(Job).where(Job.id == job_id).with_for_update())
    ).one_or_none()
    if job is None:
        raise JobNotFound(job_id)
    transition(job, JobStatus.CANCELLED)  # InvalidTransition unless queued or scheduled
    await session.commit()
    return job


async def list_dead_jobs(
    session: AsyncSession, *, job_type: str | None, limit: int, offset: int
) -> list[Job]:
    """The dead-letter queue: most recently died first."""
    stmt = (
        select(Job)
        .where(Job.status == JobStatus.DEAD)
        .order_by(Job.finished_at.desc(), Job.id)
        .limit(limit)
        .offset(offset)
    )
    if job_type is not None:
        stmt = stmt.where(Job.type == job_type)
    return list(await session.scalars(stmt))


async def retry_dead_job(session: AsyncSession, queue: RedisQueue, job_id: uuid.UUID) -> Job:
    """Give a dead job a fresh set of attempts and put it back in the queue.

    attempts resets to 0, but lease_token is left alone: it must keep growing, or a
    worker still holding a token from before the job died could become "current" again.
    last_error is kept so the reason it died stays visible until the next run.
    """
    job = (
        await session.scalars(select(Job).where(Job.id == job_id).with_for_update())
    ).one_or_none()
    if job is None:
        raise JobNotFound(job_id)
    transition(job, JobStatus.QUEUED)  # InvalidTransition unless dead
    job.attempts = 0
    job.result = None
    job.run_at = None
    await session.commit()
    try:
        await queue.enqueue(job.id, job.priority)
    except RedisError:
        log.warning("enqueue failed; retried job %s is queued but not yet in Redis", job.id)
    log.info("dead job %s retried from the DLQ", job.id)
    return job
