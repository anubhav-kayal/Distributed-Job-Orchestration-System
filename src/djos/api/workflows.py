import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Query, Response, status

from djos.api.deps import QueueDep, SessionDep
from djos.workflows import service
from djos.workflows.schemas import WorkflowCreate, WorkflowList, WorkflowOut

router = APIRouter(prefix="/workflows", tags=["workflows"])

NOT_FOUND = HTTPException(status.HTTP_404_NOT_FOUND, "workflow not found")


@router.post(
    "",
    response_model=WorkflowOut,
    status_code=status.HTTP_201_CREATED,
    responses={200: {"description": "Replay: a workflow with this Idempotency-Key exists"}},
)
async def submit_workflow(
    body: WorkflowCreate,
    session: SessionDep,
    queue: QueueDep,
    response: Response,
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ] = None,
) -> WorkflowOut:
    """Submit a DAG of jobs. A job runs once every job in its `depends_on` has succeeded;
    if one of them dies or is cancelled, it is marked `upstream_failed` instead."""
    try:
        workflow, created = await service.submit_workflow(session, queue, body, idempotency_key)
    except service.IdempotencyKeyReused:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "Idempotency-Key was already used with a different request body",
        ) from None
    if not created:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
    return await service.get_workflow(session, workflow.id)


@router.get("", response_model=WorkflowList)
async def list_workflows(
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> WorkflowList:
    items = await service.list_workflows(session, limit=limit, offset=offset)
    return WorkflowList(items=items, limit=limit, offset=offset)


@router.get("/{workflow_id}", response_model=WorkflowOut)
async def get_workflow(workflow_id: uuid.UUID, session: SessionDep) -> WorkflowOut:
    """The workflow's derived status and every job with its dependencies."""
    try:
        return await service.get_workflow(session, workflow_id)
    except service.WorkflowNotFound:
        raise NOT_FOUND from None


@router.post("/{workflow_id}/cancel", response_model=WorkflowOut)
async def cancel_workflow(workflow_id: uuid.UUID, session: SessionDep) -> WorkflowOut:
    """Cancel every job that hasn't started; running jobs finish, nothing new starts."""
    try:
        await service.cancel_workflow(session, workflow_id)
        return await service.get_workflow(session, workflow_id)
    except service.WorkflowNotFound:
        raise NOT_FOUND from None
