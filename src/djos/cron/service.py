import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from djos.core.db import db_now
from djos.cron.expr import next_fire_after
from djos.cron.schemas import ScheduleCreate, ScheduleUpdate
from djos.models import DEFAULT_TENANT_ID, CronSchedule


class ScheduleNotFound(Exception):
    pass


class ScheduleNameTaken(Exception):
    pass


async def create_schedule(
    session: AsyncSession, data: ScheduleCreate, *, tenant_id: uuid.UUID = DEFAULT_TENANT_ID
) -> CronSchedule:
    now = await db_now(session)
    schedule = CronSchedule(
        name=data.name,
        tenant_id=tenant_id,
        cron_expr=data.cron,
        job=data.job.model_dump(mode="json"),
        enabled=data.enabled,
        next_fire_at=next_fire_after(data.cron, now),
    )
    session.add(schedule)
    try:
        await session.commit()
    except IntegrityError:
        raise ScheduleNameTaken(data.name) from None
    return schedule


def _owned(stmt, tenant_id: uuid.UUID | None):
    return stmt if tenant_id is None else stmt.where(CronSchedule.tenant_id == tenant_id)


async def list_schedules(
    session: AsyncSession, *, tenant_id: uuid.UUID | None = None
) -> list[CronSchedule]:
    return list(
        await session.scalars(_owned(select(CronSchedule), tenant_id).order_by(CronSchedule.name))
    )


async def get_schedule(
    session: AsyncSession, schedule_id: uuid.UUID, *, tenant_id: uuid.UUID | None = None
) -> CronSchedule:
    schedule = (
        await session.scalars(
            _owned(select(CronSchedule).where(CronSchedule.id == schedule_id), tenant_id)
        )
    ).first()
    if schedule is None:
        raise ScheduleNotFound(schedule_id)
    return schedule


async def update_schedule(
    session: AsyncSession,
    schedule_id: uuid.UUID,
    data: ScheduleUpdate,
    *,
    tenant_id: uuid.UUID | None = None,
) -> CronSchedule:
    schedule = (
        await session.scalars(
            _owned(
                select(CronSchedule).where(CronSchedule.id == schedule_id), tenant_id
            ).with_for_update()
        )
    ).one_or_none()
    if schedule is None:
        raise ScheduleNotFound(schedule_id)
    reschedule = False
    if data.cron is not None and data.cron != schedule.cron_expr:
        schedule.cron_expr = data.cron
        reschedule = True
    if data.enabled is not None and data.enabled != schedule.enabled:
        schedule.enabled = data.enabled
        # Resuming starts from now: fires missed while paused are skipped, not replayed.
        reschedule = reschedule or data.enabled
    if data.job is not None:
        schedule.job = data.job.model_dump(mode="json")
    if reschedule:
        schedule.next_fire_at = next_fire_after(schedule.cron_expr, await db_now(session))
    await session.commit()
    return schedule


async def delete_schedule(
    session: AsyncSession, schedule_id: uuid.UUID, *, tenant_id: uuid.UUID | None = None
) -> None:
    schedule = await get_schedule(session, schedule_id, tenant_id=tenant_id)
    await session.delete(schedule)
    await session.commit()
