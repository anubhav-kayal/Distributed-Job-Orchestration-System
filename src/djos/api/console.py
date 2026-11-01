"""Read-only endpoints for the web console: one snapshot for the overview, and the job
types the submit form can offer."""

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import func, select

from djos.api.deps import SessionDep, TenantDep
from djos.core.db import db_now
from djos.handlers import get_handler, registered_types
from djos.models import Job, JobStatus, Priority

router = APIRouter(tags=["console"])

BUCKET_SECONDS = 15
WINDOW_BUCKETS = 20  # 5 minutes


class RunningJob(BaseModel):
    id: uuid.UUID
    type: str
    priority: Priority
    worker: str | None
    attempt: int
    lease_token: int
    started_at: datetime | None
    workflow_id: uuid.UUID | None
    workflow_key: str | None


class Bucket(BaseModel):
    at: datetime
    succeeded: int
    dead: int


class Failure(BaseModel):
    id: uuid.UUID
    type: str
    last_error: str | None
    finished_at: datetime | None


class Stats(BaseModel):
    now: datetime
    counts: dict[JobStatus, int]
    queued_by_priority: dict[Priority, int]
    running: list[RunningJob]
    throughput: list[Bucket]
    recent_failures: list[Failure]


@router.get("/stats", response_model=Stats)
async def stats(session: SessionDep, tenant: TenantDep) -> Stats:
    """A snapshot of the tenant's jobs for the console's overview."""
    now = await db_now(session)
    mine = Job.tenant_id == tenant.id

    counts = dict(
        (
            await session.execute(select(Job.status, func.count()).where(mine).group_by(Job.status))
        ).all()
    )
    queued = dict(
        (
            await session.execute(
                select(Job.priority, func.count())
                .where(mine, Job.status == JobStatus.QUEUED)
                .group_by(Job.priority)
            )
        ).all()
    )
    running = list(
        await session.scalars(
            select(Job)
            .where(mine, Job.status == JobStatus.RUNNING)
            .order_by(Job.started_at)
            .limit(60)
        )
    )

    # Finished jobs per 15-second bucket over the last 5 minutes.
    bucket = func.to_timestamp(
        func.floor(func.extract("epoch", Job.finished_at) / BUCKET_SECONDS) * BUCKET_SECONDS
    )
    since = func.to_timestamp(
        func.floor(func.extract("epoch", now) / BUCKET_SECONDS) * BUCKET_SECONDS
        - BUCKET_SECONDS * (WINDOW_BUCKETS - 1)
    )
    rows = (
        await session.execute(
            select(bucket.label("at"), Job.status, func.count())
            .where(
                mine,
                Job.finished_at >= since,
                Job.status.in_([JobStatus.SUCCEEDED, JobStatus.DEAD]),
            )
            .group_by("at", Job.status)
        )
    ).all()
    series: dict[float, dict[str, int]] = {}
    for at, status, n in rows:
        series.setdefault(at.timestamp(), {})[status.value] = n
    start = (now.timestamp() // BUCKET_SECONDS) * BUCKET_SECONDS - BUCKET_SECONDS * (
        WINDOW_BUCKETS - 1
    )
    throughput = [
        Bucket(
            at=datetime.fromtimestamp(start + i * BUCKET_SECONDS, now.tzinfo),
            succeeded=series.get(start + i * BUCKET_SECONDS, {}).get("succeeded", 0),
            dead=series.get(start + i * BUCKET_SECONDS, {}).get("dead", 0),
        )
        for i in range(WINDOW_BUCKETS)
    ]

    failures = list(
        await session.scalars(
            select(Job)
            .where(mine, Job.status == JobStatus.DEAD)
            .order_by(Job.finished_at.desc())
            .limit(5)
        )
    )
    return Stats(
        now=now,
        counts={s: counts.get(s, 0) for s in JobStatus},
        queued_by_priority={p: queued.get(p, 0) for p in Priority},
        running=[
            RunningJob(
                id=j.id,
                type=j.type,
                priority=j.priority,
                worker=j.locked_by,
                attempt=j.attempts,
                lease_token=j.lease_token,
                started_at=j.started_at,
                workflow_id=j.workflow_id,
                workflow_key=j.workflow_key,
            )
            for j in running
        ],
        throughput=throughput,
        recent_failures=[
            Failure(id=j.id, type=j.type, last_error=j.last_error, finished_at=j.finished_at)
            for j in failures
        ],
    )


class JobType(BaseModel):
    type: str
    description: str
    payload_schema: dict[str, Any]


@router.get("/job-types", response_model=list[JobType])
async def job_types() -> list[JobType]:
    """The job types workers can run, with each payload's JSON schema."""
    result = []
    for name in registered_types():
        handler = get_handler(name)
        doc = (handler.fn.__doc__ or "").strip().split("\n\n")[0]
        result.append(
            JobType(
                type=name,
                description=" ".join(doc.split()),
                payload_schema=handler.payload_model.model_json_schema(),
            )
        )
    return result
