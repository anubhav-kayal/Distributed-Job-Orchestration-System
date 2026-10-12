import hashlib
import json
import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from djos.jobs.schemas import JobCreate
from djos.jobs.states import transition
from djos.models import Job, JobStatus


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
    transition(job, JobStatus.CANCELLED)  # raises InvalidTransition unless still queued
    await session.commit()
    return job
