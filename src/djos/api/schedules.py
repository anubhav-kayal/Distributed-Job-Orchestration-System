import uuid

from fastapi import APIRouter, HTTPException, Response, status

from djos.api.deps import SessionDep
from djos.cron import service
from djos.cron.schemas import ScheduleCreate, ScheduleList, ScheduleOut, ScheduleUpdate

router = APIRouter(prefix="/schedules", tags=["cron schedules"])

NOT_FOUND = HTTPException(status.HTTP_404_NOT_FOUND, "schedule not found")


@router.post("", response_model=ScheduleOut, status_code=status.HTTP_201_CREATED)
async def create_schedule(body: ScheduleCreate, session: SessionDep) -> ScheduleOut:
    """Create a recurring job. `cron` is a standard 5-field expression, evaluated in UTC."""
    try:
        return ScheduleOut.model_validate(await service.create_schedule(session, body))
    except service.ScheduleNameTaken:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"a schedule named {body.name!r} already exists"
        ) from None


@router.get("", response_model=ScheduleList)
async def list_schedules(session: SessionDep) -> ScheduleList:
    schedules = await service.list_schedules(session)
    return ScheduleList(items=[ScheduleOut.model_validate(s) for s in schedules])


@router.get("/{schedule_id}", response_model=ScheduleOut)
async def get_schedule(schedule_id: uuid.UUID, session: SessionDep) -> ScheduleOut:
    try:
        return ScheduleOut.model_validate(await service.get_schedule(session, schedule_id))
    except service.ScheduleNotFound:
        raise NOT_FOUND from None


@router.patch("/{schedule_id}", response_model=ScheduleOut)
async def update_schedule(
    schedule_id: uuid.UUID, body: ScheduleUpdate, session: SessionDep
) -> ScheduleOut:
    """Change the cron expression or job, or pause/resume with `enabled`."""
    try:
        return ScheduleOut.model_validate(await service.update_schedule(session, schedule_id, body))
    except service.ScheduleNotFound:
        raise NOT_FOUND from None


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(schedule_id: uuid.UUID, session: SessionDep) -> Response:
    try:
        await service.delete_schedule(session, schedule_id)
    except service.ScheduleNotFound:
        raise NOT_FOUND from None
    return Response(status_code=status.HTTP_204_NO_CONTENT)
