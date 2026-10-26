import uuid
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from djos.api.deps import QueueDep, SessionDep
from djos.jobs import service
from djos.jobs.schemas import JobList, JobOut
from djos.jobs.states import InvalidTransition

router = APIRouter(prefix="/dlq", tags=["dead-letter queue"])


@router.get("", response_model=JobList)
async def list_dead_jobs(
    session: SessionDep,
    job_type: Annotated[str | None, Query(alias="type")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobList:
    """Jobs that ran out of attempts or failed permanently, most recent first."""
    jobs = await service.list_dead_jobs(session, job_type=job_type, limit=limit, offset=offset)
    return JobList(items=[JobOut.model_validate(j) for j in jobs], limit=limit, offset=offset)


@router.post("/{job_id}/retry", response_model=JobOut)
async def retry_dead_job(job_id: uuid.UUID, session: SessionDep, queue: QueueDep) -> JobOut:
    """Re-queue a dead job with a fresh set of attempts."""
    try:
        return JobOut.model_validate(await service.retry_dead_job(session, queue, job_id))
    except service.JobNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found") from None
    except InvalidTransition as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"job is {exc.current}; only dead jobs can be retried"
        ) from None
