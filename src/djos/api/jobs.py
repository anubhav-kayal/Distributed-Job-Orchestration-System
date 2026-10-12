import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Query, Response, status

from djos.api.deps import SessionDep
from djos.jobs import service
from djos.jobs.schemas import JobCreate, JobList, JobOut
from djos.jobs.states import InvalidTransition
from djos.models import JobStatus

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobOut,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {"description": "Replay: a job with this Idempotency-Key already exists"},
        422: {"description": "Invalid body, or Idempotency-Key reused with a different body"},
    },
)
async def submit_job(
    body: JobCreate,
    session: SessionDep,
    response: Response,
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ] = None,
) -> JobOut:
    try:
        job, created = await service.create_job(session, body, idempotency_key)
    except service.IdempotencyKeyReused:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "Idempotency-Key was already used with a different request body",
        ) from None
    if not created:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
    return JobOut.model_validate(job)


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: uuid.UUID, session: SessionDep) -> JobOut:
    try:
        return JobOut.model_validate(await service.get_job(session, job_id))
    except service.JobNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found") from None


@router.get("", response_model=JobList)
async def list_jobs(
    session: SessionDep,
    job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobList:
    jobs = await service.list_jobs(session, status=job_status, limit=limit, offset=offset)
    return JobList(items=[JobOut.model_validate(j) for j in jobs], limit=limit, offset=offset)


@router.post("/{job_id}/cancel", response_model=JobOut)
async def cancel_job(job_id: uuid.UUID, session: SessionDep) -> JobOut:
    try:
        return JobOut.model_validate(await service.cancel_job(session, job_id))
    except service.JobNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found") from None
    except InvalidTransition as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"job is {exc.current}; only queued jobs can be cancelled"
        ) from None
