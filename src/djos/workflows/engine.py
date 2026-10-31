"""What happens to the rest of a workflow when one of its jobs changes state.

Called inside the same transaction as the job's own status change, so the job's outcome
and its effect on the workflow commit together or not at all. For a worker's success
that transaction is the fenced one: a stale worker's write is rejected before it gets
here, so a job can't unblock its children twice.

Locking order, used by every path that touches more than one job of a workflow:
the job itself → its workflow row → other jobs of the workflow, in id order. The
workflow row acts as a mutex for the workflow's structure. Two parents finishing at
the same moment serialize on it (no deadlock, and the shared child is unblocked exactly
once), and a concurrent "cancel workflow" either sees the unblocked children or blocks
the unblocking until it has cancelled them.

Readiness is recounted rather than decremented. A child becomes `queued` when none of
its dependencies is still unfinished, counted from the dependencies' actual statuses.
A counter would drift if anything ever double-counted or missed an event; a recount
can't.
"""

import uuid

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from djos.jobs.states import transition
from djos.models import Job, JobDependency, JobStatus, Workflow

FAILED_UPSTREAM = (JobStatus.DEAD, JobStatus.CANCELLED, JobStatus.UPSTREAM_FAILED)

_DESCENDANTS = text(
    """
    WITH RECURSIVE descendants(id) AS (
        SELECT job_id FROM job_dependencies WHERE depends_on_job_id = :job_id
        UNION
        SELECT d.job_id
        FROM job_dependencies d
        JOIN descendants ON d.depends_on_job_id = descendants.id
    )
    SELECT id FROM descendants
    """
)


async def lock_workflow(session: AsyncSession, workflow_id: uuid.UUID) -> Workflow:
    return (
        await session.scalars(select(Workflow).where(Workflow.id == workflow_id).with_for_update())
    ).one()


async def on_job_finished(session: AsyncSession, job: Job) -> list[Job]:
    """Propagate a job's terminal status through its workflow. Returns the jobs that just
    became `queued`; the caller enqueues them after committing."""
    if job.workflow_id is None:
        return []
    await lock_workflow(session, job.workflow_id)
    if job.status is JobStatus.SUCCEEDED:
        return await _unblock_children(session, job)
    if job.status in FAILED_UPSTREAM:
        await _fail_descendants(session, job)
    return []


async def on_job_revived(session: AsyncSession, job: Job) -> None:
    """A dead job was retried from the DLQ: its descendants that were marked
    upstream_failed (and have no *other* failed dependency) go back to waiting."""
    if job.workflow_id is None:
        return
    await lock_workflow(session, job.workflow_id)
    descendants = await _lock_jobs(session, await _descendant_ids(session, job.id))
    parents = await _dependencies_of(session, [d.id for d in descendants])
    status = {d.id: d.status for d in descendants}
    status[job.id] = job.status
    parent_status = await _statuses(session, {p for ps in parents.values() for p in ps})
    parent_status.update(status)

    # Revive in waves until nothing changes: a node is revived once none of its
    # dependencies is failed, which may only become true after its parents are revived.
    changed = True
    while changed:
        changed = False
        for d in descendants:
            if d.status is not JobStatus.UPSTREAM_FAILED:
                continue
            if any(parent_status[p] in FAILED_UPSTREAM for p in parents[d.id]):
                continue
            transition(d, JobStatus.BLOCKED)
            d.last_error = None
            d.pending_deps = sum(parent_status[p] is not JobStatus.SUCCEEDED for p in parents[d.id])
            parent_status[d.id] = JobStatus.BLOCKED
            changed = True


async def _unblock_children(session: AsyncSession, parent: Job) -> list[Job]:
    child_ids = list(
        await session.scalars(
            select(JobDependency.job_id).where(JobDependency.depends_on_job_id == parent.id)
        )
    )
    children = await _lock_jobs(session, child_ids)
    unfinished = await _unfinished_dependency_counts(session, child_ids)
    ready = []
    for child in children:
        child.pending_deps = unfinished.get(child.id, 0)
        if child.pending_deps == 0 and child.status is JobStatus.BLOCKED:
            transition(child, JobStatus.QUEUED)
            ready.append(child)
    return ready


async def _fail_descendants(session: AsyncSession, job: Job) -> None:
    reason = f"upstream job {job.workflow_key!r} is {job.status}"
    for descendant in await _lock_jobs(session, await _descendant_ids(session, job.id)):
        if descendant.status is JobStatus.BLOCKED:
            descendant.last_error = reason
            transition(descendant, JobStatus.UPSTREAM_FAILED)


async def _descendant_ids(session: AsyncSession, job_id: uuid.UUID) -> list[uuid.UUID]:
    return list(await session.scalars(_DESCENDANTS, {"job_id": job_id}))


async def _lock_jobs(session: AsyncSession, ids: list[uuid.UUID]) -> list[Job]:
    if not ids:
        return []
    return list(
        await session.scalars(select(Job).where(Job.id.in_(ids)).order_by(Job.id).with_for_update())
    )


async def _unfinished_dependency_counts(
    session: AsyncSession, job_ids: list[uuid.UUID]
) -> dict[uuid.UUID, int]:
    if not job_ids:
        return {}
    rows = await session.execute(
        select(JobDependency.job_id, func.count())
        .join(Job, Job.id == JobDependency.depends_on_job_id)
        .where(JobDependency.job_id.in_(job_ids), Job.status != JobStatus.SUCCEEDED)
        .group_by(JobDependency.job_id)
    )
    return dict(rows.all())


async def _dependencies_of(
    session: AsyncSession, job_ids: list[uuid.UUID]
) -> dict[uuid.UUID, list[uuid.UUID]]:
    deps: dict[uuid.UUID, list[uuid.UUID]] = {j: [] for j in job_ids}
    if job_ids:
        rows = await session.execute(
            select(JobDependency.job_id, JobDependency.depends_on_job_id).where(
                JobDependency.job_id.in_(job_ids)
            )
        )
        for child, parent in rows.all():
            deps[child].append(parent)
    return deps


async def _statuses(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, JobStatus]:
    if not ids:
        return {}
    rows = await session.execute(select(Job.id, Job.status).where(Job.id.in_(ids)))
    return dict(rows.all())
