import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from djos.core.db import db_now
from djos.cron.expr import next_fire_after
from djos.cron.schemas import ScheduleCreate, ScheduleUpdate
from djos.models import CronSchedule


class ScheduleNotFound(Exception):
    pass


class ScheduleNameTaken(Exception):
    pass


async def create_schedule(session: AsyncSession, data: ScheduleCreate) -> CronSchedule:
    now = await db_now(session)
    schedule = CronSchedule(
        name=data.name,
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


async def list_schedules(session: AsyncSession) -> list[CronSchedule]:
    return list(await session.scalars(select(CronSchedule).order_by(CronSchedule.name)))


async def get_schedule(session: AsyncSession, schedule_id: uuid.UUID) -> CronSchedule:
    schedule = await session.get(CronSchedule, schedule_id)
    if schedule is None:
        raise ScheduleNotFound(schedule_id)
    return schedule


async def update_schedule(
    session: AsyncSession, schedule_id: uuid.UUID, data: ScheduleUpdate
) -> CronSchedule:
    schedule = (
        await session.scalars(
            select(CronSchedule).where(CronSchedule.id == schedule_id).with_for_update()
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


async def delete_schedule(session: AsyncSession, schedule_id: uuid.UUID) -> None:
    schedule = await session.get(CronSchedule, schedule_id)
    if schedule is None:
        raise ScheduleNotFound(schedule_id)
    await session.delete(schedule)
    await session.commit()
