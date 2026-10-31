import hashlib
import json
import logging
import uuid
from collections import Counter, defaultdict
from datetime import datetime

from redis.exceptions import RedisError
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from djos.jobs.states import transition
from djos.models import Job, JobDependency, JobStatus, Workflow
from djos.queue import RedisQueue
from djos.workflows.dag import topological_order
from djos.workflows.engine import lock_workflow
from djos.workflows.schemas import (
    WorkflowCreate,
    WorkflowNode,
    WorkflowOut,
    WorkflowStatus,
    WorkflowSummary,
)

log = logging.getLogger(__name__)

RUNNABLE = {JobStatus.BLOCKED, JobStatus.QUEUED, JobStatus.SCHEDULED, JobStatus.RUNNING}


class WorkflowNotFound(Exception):
    pass


class IdempotencyKeyReused(Exception):
    pass


def derive_status(counts: Counter, cancelled_at: datetime | None) -> WorkflowStatus:
    """A workflow's status, computed from its jobs rather than stored."""
    if cancelled_at is not None:
        return "cancelled"
    if any(counts[s] for s in RUNNABLE):
        return "running"
    if counts[JobStatus.SUCCEEDED] == sum(counts.values()):
        return "succeeded"
    return "failed"


def _fingerprint(data: WorkflowCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def submit_workflow(
    session: AsyncSession,
    queue: RedisQueue,
    data: WorkflowCreate,
    idempotency_key: str | None = None,
) -> tuple[Workflow, bool]:
    """Create the workflow and all its jobs in one transaction; enqueue the roots.

    Jobs without dependencies start `queued`; the rest start `blocked` and are unblocked
    by the engine as their dependencies succeed.
    """
    fingerprint = _fingerprint(data)
    workflow = (
        await session.scalars(
            insert(Workflow)
            .values(
                name=data.name,
                idempotency_key=idempotency_key,
                request_hash=fingerprint if idempotency_key else None,
            )
            .on_conflict_do_nothing(index_elements=[Workflow.idempotency_key])
            .returning(Workflow)
        )
    ).one_or_none()
    if workflow is None:
        existing = (
            await session.scalars(
                select(Workflow).where(Workflow.idempotency_key == idempotency_key)
            )
        ).one()
        if existing.request_hash != fingerprint:
            raise IdempotencyKeyReused(idempotency_key)
        return existing, False

    specs = {j.key: j for j in data.jobs}
    jobs: dict[str, Job] = {}
    for key in topological_order({j.key: j.depends_on for j in data.jobs}):
        spec = specs[key]
        deps = set(spec.depends_on)
        jobs[key] = Job(
            id=uuid.uuid4(),
            type=spec.type,
            payload=spec.payload,
            priority=spec.priority,
            max_attempts=spec.max_attempts,
            status=JobStatus.BLOCKED if deps else JobStatus.QUEUED,
            workflow_id=workflow.id,
            workflow_key=key,
            pending_deps=len(deps),
            attempts=0,
            lease_token=0,
        )
    session.add_all(jobs.values())
    await session.flush()
    session.add_all(
        JobDependency(job_id=jobs[key].id, depends_on_job_id=jobs[dep].id)
        for key, spec in specs.items()
        for dep in set(spec.depends_on)
    )
    await session.commit()

    for job in jobs.values():
        if job.status is JobStatus.QUEUED:
            try:
                await queue.enqueue(job.id, job.priority)
            except RedisError:
                log.warning("enqueue of workflow root %s failed; monitor will re-push", job.id)
    log.info("workflow %s submitted with %d jobs", workflow.id, len(jobs))
    return workflow, True


async def get_workflow(session: AsyncSession, workflow_id: uuid.UUID) -> WorkflowOut:
    workflow = await session.get(Workflow, workflow_id)
    if workflow is None:
        raise WorkflowNotFound(workflow_id)
    jobs = list(
        await session.scalars(
            select(Job).where(Job.workflow_id == workflow_id).order_by(Job.created_at, Job.id)
        )
    )
    key_of = {j.id: j.workflow_key for j in jobs}
    deps: dict[uuid.UUID, list[str]] = defaultdict(list)
    for child, parent in (
        await session.execute(
            select(JobDependency.job_id, JobDependency.depends_on_job_id).where(
                JobDependency.job_id.in_(key_of)
            )
        )
    ).all():
        deps[child].append(key_of[parent])
    counts = Counter(j.status for j in jobs)
    return WorkflowOut(
        id=workflow.id,
        name=workflow.name,
        status=derive_status(counts, workflow.cancelled_at),
        counts={s.value: n for s, n in counts.items()},
        created_at=workflow.created_at,
        cancelled_at=workflow.cancelled_at,
        jobs=[
            WorkflowNode(
                key=j.workflow_key,
                job_id=j.id,
                type=j.type,
                status=j.status,
                depends_on=sorted(deps[j.id]),
                pending_deps=j.pending_deps,
                attempts=j.attempts,
                result=j.result,
                last_error=j.last_error,
                started_at=j.started_at,
                finished_at=j.finished_at,
            )
            for j in jobs
        ],
    )


async def list_workflows(
    session: AsyncSession, *, limit: int, offset: int
) -> list[WorkflowSummary]:
    workflows = list(
        await session.scalars(
            select(Workflow)
            .order_by(Workflow.created_at.desc(), Workflow.id)
            .limit(limit)
            .offset(offset)
        )
    )
    counts: dict[uuid.UUID, Counter] = defaultdict(Counter)
    if workflows:
        rows = await session.execute(
            select(Job.workflow_id, Job.status, func.count())
            .where(Job.workflow_id.in_([w.id for w in workflows]))
            .group_by(Job.workflow_id, Job.status)
        )
        for workflow_id, status, n in rows.all():
            counts[workflow_id][status] = n
    return [
        WorkflowSummary(
            id=w.id,
            name=w.name,
            status=derive_status(counts[w.id], w.cancelled_at),
            counts={s.value: n for s, n in counts[w.id].items()},
            created_at=w.created_at,
            cancelled_at=w.cancelled_at,
        )
        for w in workflows
    ]


async def cancel_workflow(session: AsyncSession, workflow_id: uuid.UUID) -> None:
    """Cancel every job that hasn't started. Running jobs are left to finish; their
    children are already cancelled, so nothing new starts afterwards.

    SKIP LOCKED: a job a worker is claiming right now is skipped (it's about to be
    running, and will finish); waiting for it could deadlock with that worker, which
    locks the job before the workflow.
    """
    if await session.get(Workflow, workflow_id) is None:
        raise WorkflowNotFound(workflow_id)
    workflow = await lock_workflow(session, workflow_id)
    if workflow.cancelled_at is not None:
        return
    jobs = list(
        await session.scalars(
            select(Job)
            .where(
                Job.workflow_id == workflow_id,
                Job.status.in_([JobStatus.BLOCKED, JobStatus.QUEUED, JobStatus.SCHEDULED]),
            )
            .order_by(Job.id)
            .with_for_update(skip_locked=True)
        )
    )
    for job in jobs:
        transition(job, JobStatus.CANCELLED)
        job.last_error = "workflow cancelled"
    await session.execute(
        update(Workflow).where(Workflow.id == workflow_id).values(cancelled_at=func.now())
    )
    await session.commit()
    log.info("workflow %s cancelled (%d jobs)", workflow_id, len(jobs))
