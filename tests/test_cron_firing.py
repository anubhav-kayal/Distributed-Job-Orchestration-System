import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update

from djos.models import CronSchedule, Job, JobStatus, Priority
from djos.scheduler import Scheduler

pytestmark = pytest.mark.integration

T0 = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)  # a minute boundary


async def add_schedule(sessionmaker, *, name="s", cron="* * * * *", next_fire_at=T0, **kw):
    async with sessionmaker() as session:
        schedule = CronSchedule(
            name=name,
            cron_expr=cron,
            job=kw.pop("job", {"type": "sleep", "payload": {"seconds": 0}}),
            next_fire_at=next_fire_at,
            **kw,
        )
        session.add(schedule)
        await session.commit()
        return schedule.id


async def jobs_for(sessionmaker, schedule_id) -> list[Job]:
    async with sessionmaker() as session:
        return list(
            await session.scalars(
                select(Job)
                .where(Job.idempotency_key.like(f"cron:{schedule_id}:%"))
                .order_by(Job.created_at)
            )
        )


async def load_schedule(sessionmaker, schedule_id) -> CronSchedule:
    async with sessionmaker() as session:
        return await session.get(CronSchedule, schedule_id)


async def test_due_schedule_fires_one_job_and_advances(sessionmaker, queue):
    sid = await add_schedule(sessionmaker)

    fired = await Scheduler(sessionmaker, queue).fire_due_schedules(T0 + timedelta(seconds=1))

    assert fired == 1
    [job] = await jobs_for(sessionmaker, sid)
    assert job.idempotency_key == f"cron:{sid}:2030-01-01T12:00:00Z"
    assert job.status is JobStatus.QUEUED
    assert await queue.contains(job.id, Priority.NORMAL)
    schedule = await load_schedule(sessionmaker, sid)
    assert schedule.last_fired_at == T0
    assert schedule.next_fire_at == T0 + timedelta(minutes=1)


async def test_not_yet_due_and_disabled_schedules_do_not_fire(sessionmaker, queue):
    await add_schedule(sessionmaker, name="later", next_fire_at=T0 + timedelta(minutes=5))
    await add_schedule(sessionmaker, name="paused", enabled=False)

    assert await Scheduler(sessionmaker, queue).fire_due_schedules(T0 + timedelta(seconds=1)) == 0


async def test_missed_fires_run_once_then_skip_ahead(sessionmaker, queue):
    # The scheduler was down for an hour: one catch-up job, not sixty.
    sid = await add_schedule(sessionmaker)
    now = T0 + timedelta(hours=1, seconds=30)

    assert await Scheduler(sessionmaker, queue).fire_due_schedules(now) == 1

    assert len(await jobs_for(sessionmaker, sid)) == 1
    assert (await load_schedule(sessionmaker, sid)).next_fire_at == T0 + timedelta(
        hours=1, minutes=1
    )


async def test_concurrent_schedulers_fire_each_due_schedule_once(sessionmaker, queue):
    ids = [await add_schedule(sessionmaker, name=f"s{i}") for i in range(10)]
    schedulers = [Scheduler(sessionmaker, queue, batch_size=3) for _ in range(3)]
    now = T0 + timedelta(seconds=1)

    async def drain(s: Scheduler) -> None:
        while await s.fire_due_schedules(now):
            pass

    await asyncio.gather(*(drain(s) for s in schedulers))

    assert [len(await jobs_for(sessionmaker, sid)) for sid in ids] == [1] * 10


async def test_every_minute_cron_with_two_schedulers_makes_exactly_one_job_per_minute(
    sessionmaker, queue
):
    # Day 10's acceptance test, with simulated time: two schedulers tick every 10s for
    # five minutes, sometimes at the same instant. Exactly five jobs, one per minute.
    sid = await add_schedule(sessionmaker)
    a, b = Scheduler(sessionmaker, queue), Scheduler(sessionmaker, queue)
    for tick in range(30):
        now = T0 + timedelta(seconds=10 * tick + 1)
        await asyncio.gather(a.fire_due_schedules(now), b.fire_due_schedules(now))

    jobs = await jobs_for(sessionmaker, sid)
    assert [j.idempotency_key.rsplit(":", 1)[0][-5:] for j in jobs] == [
        "12:00",
        "12:01",
        "12:02",
        "12:03",
        "12:04",
    ]


async def test_idempotency_key_stops_a_second_job_for_the_same_fire_time(sessionmaker, queue):
    # Belt and braces: even if a schedule's next_fire_at were rewound to a fire time that
    # already produced a job, the unique key refuses a second one.
    sid = await add_schedule(sessionmaker)
    scheduler = Scheduler(sessionmaker, queue)
    await scheduler.fire_due_schedules(T0 + timedelta(seconds=1))
    async with sessionmaker() as session:
        await session.execute(
            update(CronSchedule).where(CronSchedule.id == sid).values(next_fire_at=T0)
        )
        await session.commit()

    assert await scheduler.fire_due_schedules(T0 + timedelta(seconds=2)) == 0
    assert len(await jobs_for(sessionmaker, sid)) == 1


async def test_a_broken_template_does_not_block_other_schedules(sessionmaker, queue):
    broken = await add_schedule(sessionmaker, name="broken", job={"type": "removed"})
    healthy = await add_schedule(sessionmaker, name="healthy")

    fired = await Scheduler(sessionmaker, queue).fire_due_schedules(T0 + timedelta(seconds=1))

    assert fired == 1
    assert len(await jobs_for(sessionmaker, healthy)) == 1
    assert (await load_schedule(sessionmaker, broken)).next_fire_at == T0 + timedelta(minutes=1)


async def test_fired_jobs_inherit_the_template(sessionmaker, queue):
    sid = await add_schedule(
        sessionmaker,
        job={
            "type": "fake_email",
            "payload": {"to": "a@b.co", "subject": "digest"},
            "priority": "high",
            "max_attempts": 5,
        },
    )
    await Scheduler(sessionmaker, queue).fire_due_schedules(T0 + timedelta(seconds=1))

    [job] = await jobs_for(sessionmaker, sid)
    assert (job.type, job.priority, job.max_attempts) == ("fake_email", Priority.HIGH, 5)
    assert job.payload == {"to": "a@b.co", "subject": "digest", "body": ""}
    async with sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
